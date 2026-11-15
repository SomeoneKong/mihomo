package probe

import (
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// Conditioned is a group that tells the conditions it reads evidence under. A test under a
// condition that differs from one of them only in its timeout tells of it as well, as far as
// its result decides how a test under that timeout would have ended.
type Conditioned interface {
	Conditions() []Condition
}

// testTimeout is the time a test under cond runs at most.
func testTimeout(cond Condition) time.Duration {
	if cond.Timeout <= 0 {
		return DefaultTestTimeout
	}
	return cond.Timeout
}

// resultUnder is how a test that ended with result under a timeout of from would have ended
// under a timeout of to, if the result decides it: a success within to, or a failure that
// came before to or after a timeout no shorter than to. A success after to says nothing of
// the node a shorter test would have given up on, and a timeout shorter than to says nothing
// of a longer one.
func resultUnder(result C.ProbeResult, from, to time.Duration) (C.ProbeResult, bool) {
	if result.Canceled {
		return result, false
	}
	if result.Satisfied {
		return result, result.Elapsed <= to
	}
	if result.ErrorClass == C.ProbeErrorTimeout && from < to {
		return result, false
	}
	if result.Elapsed > to {
		// it would have run out of time first
		result.ErrorClass, result.Error, result.Status, result.Elapsed = C.ProbeErrorTimeout, "", 0, to
	}
	return result, true
}

// relatedKeysLocked are the keys of the target of key a group reads under a condition that
// differs from that of key only in its timeout.
func (c *Coordinator) relatedKeysLocked(key Key) []Key {
	var keys []Key
	seen := map[Condition]bool{key.Cond: true}
	for _, reg := range c.registrations {
		g, ok := reg.group.(Conditioned)
		if !ok {
			continue
		}
		for _, cond := range g.Conditions() {
			if seen[cond] || cond.URL != key.Cond.URL || cond.Expected != key.Cond.Expected {
				continue
			}
			seen[cond] = true
			if related := (Key{Target: key.Target, Cond: cond}); c.referencedLocked(related) {
				keys = append(keys, related)
			}
		}
	}
	return keys
}

// relatedResult is the result of a test as it decides the evidence of a related key.
type relatedResult struct {
	key           Key
	result        C.ProbeResult
	before, after Evidence
}

// adoptRelatedLocked takes the result of exec into the evidence of the related keys it decides,
// ordered with their other tests by when it started.
func (c *Coordinator) adoptRelatedLocked(exec *execution, result C.ProbeResult) []relatedResult {
	if exec.void || result.Canceled {
		return nil
	}
	var list []relatedResult
	for _, key := range c.relatedKeysLocked(exec.key) {
		r, ok := resultUnder(result, testTimeout(exec.key.Cond), testTimeout(key.Cond))
		if !ok {
			continue
		}
		related := *exec
		related.key = key
		if adopted, before, after := c.adoptLocked(&related, r); adopted {
			list = append(list, relatedResult{key: key, result: r, before: before, after: after})
		}
	}
	return list
}

// poolUnderLocked takes the result of exec into the pool of its target under the conditions it
// tells of, those that differ from its own in their timeout only, whether a group reads them
// for the target or not: a success also when the evidence under them is decided by a later
// result, a failure when it found the target newly dead.
func (c *Coordinator) poolUnderLocked(exec *execution, result C.ProbeResult, newlyDead bool) {
	pk, ok := nodePoolKey(exec.key)
	if !ok || !result.Satisfied && !newlyDead {
		return
	}
	// the conditions the groups read, and those the pool has failures or outages under
	conds := map[Condition]bool{}
	for _, reg := range c.registrations {
		if g, ok := reg.group.(Conditioned); ok {
			for _, cond := range g.Conditions() {
				conds[cond] = true
			}
		}
	}
	for other := range c.outages.recent {
		if other.pool == pk.pool {
			conds[other.cond] = true
		}
	}
	for other := range c.outages.active {
		if other.pool == pk.pool {
			conds[other.cond] = true
		}
	}
	for _, o := range c.outages.past {
		if o.key.pool == pk.pool && o.state == OutageEstablished && !o.committed {
			conds[o.key.cond] = true
		}
	}
	for cond := range conds {
		if cond == exec.key.Cond || !sameTest(cond, exec.key.Cond) {
			continue
		}
		r, ok := resultUnder(result, testTimeout(exec.key.Cond), testTimeout(cond))
		key := Key{Target: exec.key.Target, Cond: cond}
		switch {
		case !ok:
		case r.Satisfied:
			c.poolAliveLocked(key, exec.started, exec.id)
		default:
			// dead before under that condition, it is tested again: no new failure, unless the
			// failure counts anew under it
			if ev := c.evidence[key]; ev != nil && ev.State == Dead && ev.DeadSince.Before(exec.started) && !exec.reconfirms[key] {
				continue
			}
			c.poolFailureLocked(key, exec.started)
		}
	}
}

func relatedFields(list []relatedResult) []any {
	fields := make([]any, 0, len(list))
	for i := range list {
		r := &list[i]
		fields = append(fields, map[string]any{
			"timeout": testTimeout(r.key.Cond).Milliseconds(),
			"ok":      r.result.Satisfied,
			"before":  evidenceFields(&r.before),
			"after":   evidenceFields(&r.after),
		})
	}
	return fields
}

// finishRelatedLocked completes the related keys a result was adopted for, as finish does its
// own key: the memory of failures, the outages of pools, the needs waiting and the groups
// reading them. own is the completion of the key the test ran under.
func (c *Coordinator) finishRelatedLocked(exec *execution, list []relatedResult, own *Completion) {
	for i := range list {
		r := &list[i]
		related := *exec
		related.key = r.key
		related.needs, related.requests = nil, nil
		completion := Completion{
			Exec: exec.id, Key: r.key, Started: exec.started, Result: r.result,
			Adopted: true, Before: r.before, After: r.after, API: exec.manualAPI,
			Pools: own.Pools, UnknownPath: own.UnknownPath, Seq: own.Seq,
			Comparing: own.Comparing, Ended: own.Ended,
		}
		if r.after.State == Alive && r.before.State == Dead && !r.before.DeadSince.IsZero() {
			duration := exec.started.Sub(r.before.DeadSince) - c.network.excused(r.before.DeadSince, exec.started)
			completion.Episode = &Episode{Since: r.before.DeadSince, Until: exec.started, Duration: duration}
		}
		c.evidence[r.key].Seq = own.Seq
		completion.After.Seq = own.Seq
		c.adoptedLocked(&related, r.before, r.after, &completion)
		if r.after.State == Dead {
			c.deferWaitingLocked(r.key, r.after)
		}
		for _, reg := range c.registrations {
			if reg.group.References(r.key.Target, r.key.Cond) {
				reg.group.Deliver(completion)
			}
		}
	}
}
