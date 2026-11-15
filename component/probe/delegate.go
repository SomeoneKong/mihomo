package probe

import (
	"time"

	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

// SourceDelegate is the source of the needs a group derives from what the groups listing it
// forwarded to it.
const SourceDelegate = "delegate"

// Availability is what a group that judges its own availability under its condition
// publishes: the groups listing it under the same condition read it instead of testing
// through it, and forward it what they need of it.
type Availability struct {
	// State is Alive when the group carries traffic, through a tested path or, when Terminal,
	// by a direct outbound it does not test; Dead when nothing carries it; Unknown when the
	// path it prefers is not known yet.
	State    State
	Terminal bool
	// Failed is set when it is Dead because tests found what carried it dead, rather than by
	// a pin, an exclusion or a change of its members.
	Failed bool
	// Proof is when the test it rests on started: the test of the preferred path, or the
	// latest failure that left nothing; zero when no test supports it. Exec is that test.
	Proof time.Time
	Exec  uint64
	Delay uint16 // of the preferred path, when known
	Seq   uint64 // the event the group recorded it by
	// Probe is the member a manual test of the group tests instead of the group: the path it
	// prefers, or while it is not available the first member to recover; nil when it ends at a
	// direct outbound it does not test, or has nothing to test.
	Probe C.Proxy
}

// Forward is a need of a group for a group that judges its own availability, forwarded to it.
type Forward struct {
	Group  string `json:"group"`
	Need   uint64 `json:"need"`
	Class  Class  `json:"class"`
	Source string `json:"source"`
	// NotBefore, when set, asks for a conclusion resting on a test that started after it.
	NotBefore time.Time `json:"notBefore"`
	// RepairUntil is set when only the repair window of the group asks for it to recover.
	RepairUntil time.Time `json:"repairUntil"`
}

// Forwarded tells a group that judges its own availability what the groups listing it need
// of it now, replacing what it was told before.
type Forwarded struct {
	Key   Key
	Needs []Forward
}

func (Forwarded) notice() {}

// delegate is a key whose evidence a group publishes.
type delegate struct {
	reg       *Registration
	forwarded []Forward // as last told
	probe     C.Proxy   // what a manual test of the group tests, as last published
}

// Delegate tells whether the group judges its availability under key itself. A change
// drops what was known of key from the other source: tests through the group, or what the
// group published.
func (r *Registration) Delegate(key Key, on bool) {
	c := r.coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.registrations[r.id] != r {
		return // dropped by a reload
	}
	d := c.delegates[key]
	if on == (d != nil && d.reg == r) {
		return
	}
	// No test of the previous path can write this key, even after the group
	// gives up ownership. Other timeouts keep their own eligibility.
	for exec := range c.inflight {
		if exec.key.Target == key.Target && sameTest(exec.key.Cond, key.Cond) {
			exec.keys[key] = keyRevoked
		}
	}
	if on {
		c.delegates[key] = &delegate{reg: r}
		if d == nil {
			c.delegateTargets[key.Target]++
		}
		delete(c.network.retained, key)
	} else {
		if d == nil {
			return
		}
		c.dropDelegateLocked(key)
		r.group.Deliver(Forwarded{Key: key})
	}
	ev := c.evidenceLocked(key)
	before := *ev
	c.nextVersion++
	*ev = Evidence{Delegated: on, Version: c.nextVersion}
	completion := Completion{Key: key, Adopted: true, Before: before, After: *ev}
	for _, reg := range c.registrations {
		if reg != r && reg.group.References(key.Target, key.Cond) {
			reg.group.Deliver(completion)
		}
	}
	now := c.now()
	for _, reg := range c.registrations {
		if entry, ok := reg.needs[key]; ok && entry.status != needRunning {
			entry.eligibleAt, entry.waiting = time.Time{}, ""
			c.placeLocked(entry, now)
		}
	}
	c.forwardAllLocked()
}

// Publish takes the availability the group judges under key as the evidence of key: the
// groups listing it read it, and the needs they have of it may be satisfied by it.
func (r *Registration) Publish(key Key, a Availability) {
	c := r.coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if d := c.delegates[key]; d == nil || d.reg != r {
		return
	}
	c.delegates[key].probe = a.Probe
	ev := c.evidenceLocked(key)
	before := *ev
	c.nextVersion++
	deadSince := time.Time{}
	if a.State == Dead {
		deadSince = before.DeadSince
		if before.State != Dead {
			deadSince = a.Proof
		}
	}
	*ev = Evidence{
		State: a.State, Started: a.Proof, Exec: a.Exec, DeadSince: deadSince, Seq: a.Seq,
		Version: c.nextVersion, Delegated: true, Terminal: a.Terminal,
	}
	completion := Completion{
		Exec: a.Exec, Seq: a.Seq, Key: key, Started: a.Proof, Adopted: true, Before: before, After: *ev,
	}
	completion.Result.Satisfied = a.State == Alive && !a.Terminal
	completion.Result.Delay = a.Delay
	completion.Policy = a.State == Dead && !a.Failed
	completion.Charged = a.Failed && before.State != Dead && !a.Proof.IsZero()
	if a.State == Alive && !a.Proof.IsZero() && before.State == Dead && !before.DeadSince.IsZero() {
		duration := a.Proof.Sub(before.DeadSince) - c.network.excused(before.DeadSince, a.Proof)
		completion.Episode = &Episode{Since: before.DeadSince, Until: a.Proof, Duration: duration}
	}
	// A failure found by tests counts for the memory of the groups listing it, not for a
	// traffic pool and not for the check of the local network: the tests of its paths do.
	if a.Failed && before.State != Dead && !a.Proof.IsZero() {
		c.addPendingLocked(&pendingFailure{
			key: key, exec: a.Exec, started: a.Proof, commitAt: a.Proof.Add(AttributionDelay), object: c.objectIDLocked(key.Target),
		})
	}
	now := c.now()
	for _, reg := range c.registrations {
		if entry, ok := reg.needs[key]; ok && entry.status == needPending && satisfiedBy(ev, &entry.need, now) {
			entry.status, entry.eligibleAt, entry.waiting = needDone, time.Time{}, ""
			c.recordNeedLocked(entry, "reused", map[string]any{"evidence": evidenceFields(ev)})
		}
	}
	for _, reg := range c.registrations {
		if reg != r && reg.group.References(key.Target, key.Cond) {
			reg.group.Deliver(completion)
		}
	}
	// a failure whose attribution delay passed while its tests ran is due already
	c.attributeDueLocked()
	c.forwardAllLocked()
}

// Delegated reports whether a group judges its availability under key itself.
func (c *Coordinator) Delegated(key Key) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.delegates[key] != nil
}

// manualTargetLocked is what a manual test of target under cond tests: the member a group
// judging its own availability under a condition with the same URL and status tests for it,
// down through the groups on the way; target itself otherwise. via lists the groups passed.
func (c *Coordinator) manualTargetLocked(target C.Proxy, cond Condition) (C.Proxy, []C.Proxy) {
	var via []C.Proxy
	for i := 0; i < 16; i++ {
		var next C.Proxy
		for key, d := range c.delegates {
			if key.Target == target && key.Cond.URL == cond.URL && key.Cond.Expected == cond.Expected {
				next = d.probe
				break
			}
		}
		if next == nil {
			break
		}
		via = append(via, target)
		target = next
	}
	return target, via
}

// delegatedTargetLocked reports whether a group judges its own availability under some
// condition: the failures and successes through it tell of its paths, which tell of them
// themselves.
func (c *Coordinator) delegatedTargetLocked(target C.Proxy) bool {
	return c.delegateTargets[target] > 0
}

// forwardAllLocked tells each group judging its own availability what the active groups
// listing it need of it, when that changed.
func (c *Coordinator) forwardAllLocked() {
	for key, d := range c.delegates {
		var list []Forward
		for _, reg := range c.registrations {
			if reg.state != stateActive || reg.persistent {
				continue
			}
			entry, ok := reg.needs[key]
			if !ok || entry.status != needPending {
				continue
			}
			list = append(list, Forward{
				Group: reg.group.Name(), Need: entry.id, Class: entry.need.Class, Source: entry.need.Source,
				NotBefore: entry.need.NotBefore, RepairUntil: entry.need.RepairUntil,
			})
		}
		slices.SortFunc(list, func(a, b Forward) int { return int(a.Need) - int(b.Need) })
		if slices.Equal(list, d.forwarded) {
			continue
		}
		d.forwarded = list
		d.reg.group.Deliver(Forwarded{Key: key, Needs: slices.Clone(list)})
	}
}

// dropDelegatesLocked forgets the keys a group that is dropped published.
func (c *Coordinator) dropDelegatesLocked(r *Registration) {
	for key, d := range c.delegates {
		if d.reg == r {
			c.dropDelegateLocked(key)
		}
	}
}

func (c *Coordinator) dropDelegateLocked(key Key) {
	delete(c.delegates, key)
	if c.delegateTargets[key.Target]--; c.delegateTargets[key.Target] <= 0 {
		delete(c.delegateTargets, key.Target)
	}
}
