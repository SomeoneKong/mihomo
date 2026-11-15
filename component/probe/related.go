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

// executionKeysLocked snapshots ordinary evidence a test can decide. An unread
// primary can update existing evidence until pruning or a change of ownership.
func (c *Coordinator) executionKeysLocked(key Key) map[Key]keyEligibility {
	keys := map[Key]keyEligibility{key: keyReferenced}
	for _, reg := range c.registrations {
		g, ok := reg.group.(Conditioned)
		if !ok {
			continue
		}
		for _, cond := range g.Conditions() {
			if sameTest(cond, key.Cond) {
				keys[Key{Target: key.Target, Cond: cond}] = keyReferenced
			}
		}
	}
	for k := range keys {
		ev := c.evidence[k]
		switch {
		case c.delegates[k] != nil:
			keys[k] = keyRevoked
		case c.referencedLocked(k):
			continue
		case k == key && ev != nil && !ev.Delegated:
			keys[k] = keyUnreadPrimary
		default:
			delete(keys, k)
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
	for key, eligibility := range exec.keys {
		if eligibility != keyReferenced || key == exec.key || !c.referencedLocked(key) {
			continue
		}
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

// canTellPoolLocked also admits pool witnesses under conditions read only for other nodes.
// A removed or newly referenced evidence key cannot contribute a witness from an older test.
func (c *Coordinator) canTellPoolLocked(exec *execution, key Key) bool {
	if exec.void {
		return false
	}
	if eligibility, known := exec.keys[key]; known && eligibility != keyUnreadPrimary {
		return eligibility == keyReferenced
	}
	return !c.referencedLocked(key)
}

// poolUnderLocked registers successes even when a newer test decided the evidence.
// Adopted failures register in adoptedLocked. Only pool-only conditions need a
// projected failure, and only when this execution found a first failure or reconfirmation.
func (c *Coordinator) poolUnderLocked(exec *execution, result C.ProbeResult) {
	pk, ok := nodePoolKey(exec.key)
	if !ok || !result.Satisfied && len(exec.chargedKeys) == 0 {
		return
	}
	// the conditions the groups read, and those the pool has failures or outages under; a
	// failure is taken only under those the nodes of the pool are read under, or the pool
	// has failures or outages under
	conds := map[Condition]bool{}
	if result.Satisfied {
		conds[exec.key.Cond] = false
	}
	for _, reg := range c.registrations {
		if g, ok := reg.group.(Conditioned); ok {
			for _, cond := range g.Conditions() {
				if _, ok := conds[cond]; !ok {
					conds[cond] = false
				}
			}
		}
	}
	for cond := range c.outages.used[pk.pool] {
		conds[cond] = true
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
	for cond, takesFailure := range conds {
		key := Key{Target: exec.key.Target, Cond: cond}
		if !sameTest(cond, exec.key.Cond) || !c.canTellPoolLocked(exec, key) {
			continue
		}
		if _, ok := resultUnder(result, testTimeout(exec.key.Cond), testTimeout(cond)); !ok && cond != exec.key.Cond {
			continue
		}
		if result.Satisfied {
			c.poolAliveLocked(key, exec.started, exec.id)
			continue
		}
		if !takesFailure || exec.chargedKeys[key] || c.referencedLocked(key) {
			continue
		}
		// An existing primary takes only its own adopted failure. An unread
		// related key already dead before this test has no new failure either.
		if ev := c.evidence[key]; ev != nil && (key == exec.key || ev.State == Dead && ev.DeadSince.Before(exec.started)) {
			continue
		}
		c.poolFailureLocked(key, exec.started)
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
