package probe

import (
	"math"
	"sort"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// outage is a suspected outage of a traffic pool under a test condition: nodes of it failed
// one after another and none was found alive since. Only the recovery tests of its dead nodes
// are merged while it lasts; a node of it that did not fail and is found alive disproves it,
// and one of its dead nodes coming back establishes it.
type outage struct {
	key   poolKey
	state string
	first time.Time // the earliest failure of it started then
	since time.Time // suspected
	end   time.Time
	dead  map[Key]time.Time // the nodes found dead, with when their failure started
	// the recovery tests: one node at a time, in turn, as often as the outage backs off
	failures int
	nextAt   time.Time
	last     uint64 // object number of the node tested last
	// verdict is the conclusion of the result that started first so far, waiting for the
	// tests of the pool that started earlier and still run
	verdict *verdict
}

type verdict struct {
	state   string
	started time.Time
	exec    uint64 // the test, after any test that started at the same time for a connection
	by      map[string]any
}

// earlier orders the results that conclude an outage: by when they started, then by test.
func earlier(started time.Time, exec uint64, than *verdict) bool {
	return started.Before(than.started) || (started.Equal(than.started) && exec < than.exec)
}

func (o *outage) covers(t time.Time) bool {
	return !t.Before(o.first) && (o.end.IsZero() || t.Before(o.end))
}

type poolFailure struct {
	key     Key
	started time.Time
}

// outages follows the failures of the nodes of each pool and the outages they suggest.
type outages struct {
	recent  map[poolKey][]poolFailure
	active  map[poolKey]*outage
	past    []*outage // concluded, kept while a failure they cover may still be attributed
	success map[string]time.Time
}

func newOutages() *outages {
	return &outages{recent: map[poolKey][]poolFailure{}, active: map[poolKey]*outage{}, success: map[string]time.Time{}}
}

func (c *Coordinator) outageCovering(key poolKey, t time.Time) *outage {
	if o := c.outages.active[key]; o != nil && o.covers(t) {
		return o
	}
	for _, o := range c.outages.past {
		if o.key == key && o.covers(t) {
			return o
		}
	}
	return nil
}

// nodePoolKey is the pool of a node target under the condition of key, if it has one.
func nodePoolKey(key Key) (poolKey, bool) {
	if _, isGroup := key.Target.Adapter().(groupView); isGroup {
		return poolKey{}, false
	}
	pool := poolOf(key.Target)
	if pool == "" {
		return poolKey{}, false
	}
	return poolKey{pool: pool, cond: key.Cond}, true
}

// poolFailureLocked takes a node found dead: it may suggest an outage of its pool, or belong
// to one suspected.
func (c *Coordinator) poolFailureLocked(key Key, started time.Time) {
	pk, ok := nodePoolKey(key)
	if !ok {
		return
	}
	if o := c.outages.active[pk]; o != nil {
		if _, ok := o.dead[key]; !ok {
			o.dead[key] = started
		}
		return
	}
	kept := c.outages.recent[pk][:0]
	for _, f := range c.outages.recent[pk] {
		if started.Sub(f.started) < OutageWindow && f.key != key {
			kept = append(kept, f)
		}
	}
	clearAfter(c.outages.recent[pk], len(kept))
	kept = append(kept, poolFailure{key: key, started: started})
	c.outages.recent[pk] = kept
	if len(kept) < OutageNodes || c.network.state != NetworkNormal {
		return
	}
	first := started
	for _, f := range kept {
		if f.started.Before(first) {
			first = f.started
		}
	}
	// No node of the pool may have been found alive, or carried a new connection, since.
	for k, ev := range c.evidence {
		if other, ok := nodePoolKey(k); ok && other == pk && ev.State == Alive && !ev.Started.Before(first) {
			return
		}
	}
	if !c.outages.success[pk.pool].Before(first) {
		return
	}
	o := &outage{key: pk, state: OutageSuspected, first: first, since: c.now(), dead: map[Key]time.Time{}}
	for _, f := range kept {
		o.dead[f.key] = f.started
	}
	// Nodes found dead earlier that are still dead belong to it too.
	for k, ev := range c.evidence {
		if other, ok := nodePoolKey(k); ok && other == pk && ev.State == Dead {
			if _, in := o.dead[k]; !in {
				o.dead[k] = ev.DeadSince
			}
		}
	}
	o.nextAt = c.now()
	c.outages.active[pk] = o
	delete(c.outages.recent, pk)
	c.recordOutageLocked(o, "failures", nil)
	c.deliverOutageLocked(o)
}

// poolAliveLocked takes a node found alive by a test that started then.
func (c *Coordinator) poolAliveLocked(key Key, started time.Time, exec uint64) {
	pk, ok := nodePoolKey(key)
	if !ok {
		return
	}
	o := c.outages.active[pk]
	if o == nil || started.Before(o.first) {
		return
	}
	c.offerVerdictLocked(o, aliveVerdict(o, key, started), started, exec, map[string]any{"probe": exec, "node": c.nodeRef(key.Target)})
}

// aliveVerdict is what a node found alive by what started then tells of an outage: one of the
// dead nodes back establishes it, any node alive before it failed disproves it.
func aliveVerdict(o *outage, key Key, started time.Time) string {
	if failed, dead := o.dead[key]; dead && !started.Before(failed) {
		return OutageEstablished
	}
	return OutageDisproved
}

// offerVerdictLocked takes a result that concludes an outage. The one that started first
// decides, whatever order they end in: a test of the pool that started earlier and still runs
// is waited for.
func (c *Coordinator) offerVerdictLocked(o *outage, state string, started time.Time, exec uint64, by map[string]any) {
	if o.verdict == nil || earlier(started, exec, o.verdict) {
		o.verdict = &verdict{state: state, started: started, exec: exec, by: by}
	}
	c.settleVerdictLocked(o)
}

func (c *Coordinator) settleVerdictLocked(o *outage) {
	if o.verdict == nil || c.outages.active[o.key] != o {
		return
	}
	for exec := range c.inflight {
		if exec.void || !earlier(exec.started, exec.id, o.verdict) || exec.started.Before(o.first) {
			continue
		}
		if pk, ok := nodePoolKey(exec.key); ok && pk == o.key {
			return
		}
	}
	c.concludeOutageLocked(o, o.verdict.state, o.verdict.by)
}

// poolSuccessLocked takes a new connection through target that received data.
func (c *Coordinator) poolSuccessLocked(target C.Proxy, dialed time.Time, _ bool) {
	pool := poolOf(target)
	if pool == "" {
		return
	}
	if dialed.After(c.outages.success[pool]) {
		c.outages.success[pool] = dialed
	}
	for pk, o := range c.outages.active {
		if pk.pool != pool || dialed.Before(o.since) {
			continue
		}
		state := aliveVerdict(o, Key{Target: target, Cond: pk.cond}, dialed)
		c.offerVerdictLocked(o, state, dialed, math.MaxUint64, map[string]any{"connection": formatTime(dialed), "node": c.nodeRef(target)})
	}
}

func (c *Coordinator) nodeRef(p C.Proxy) map[string]any {
	return map[string]any{"name": p.Name(), "object": c.objects[p]}
}

// concludeOutageLocked ends a suspected outage. The evidence stays; the other dead nodes are
// tested again without waiting for a backoff.
func (c *Coordinator) concludeOutageLocked(o *outage, state string, by map[string]any) {
	now := c.now()
	if state == OutageEstablished && c.network.covers(o.first) {
		// it began while the local network seemed down: not the pool's
		state = OutageUndecided
	}
	o.state, o.end = state, now
	delete(c.outages.active, o.key)
	c.outages.past = append(c.outages.past, o)
	kept := c.outages.past[:0]
	for _, p := range c.outages.past {
		if now.Sub(p.end) < LongWindow {
			kept = append(kept, p)
		}
	}
	clearAfter(c.outages.past, len(kept))
	c.outages.past = kept
	for k := range o.dead {
		if ev := c.evidence[k]; ev != nil && ev.State == Dead {
			ev.Failures = 0
			ev.NextAllowed = now
		}
	}
	if state == OutageEstablished {
		events := pruneEvents(c.quality.pools[o.key], now)
		c.quality.pools[o.key] = append(events, QualityEvent{At: o.first, Weight: 1})
	}
	c.recordOutageLocked(o, "concluded", by)
	c.deliverOutageLocked(o)
	c.attributeDueLocked()
}

func (c *Coordinator) recordOutageLocked(o *outage, reason string, by map[string]any) {
	var nodes []any
	keys := make([]Key, 0, len(o.dead))
	for k := range o.dead {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return c.objects[keys[i].Target] < c.objects[keys[j].Target] })
	for _, k := range keys {
		nodes = append(nodes, map[string]any{"name": k.Target.Name(), "object": c.objects[k.Target], "failed": formatTime(o.dead[k])})
	}
	fields := map[string]any{
		"event": "outage", "pool": o.key.pool, "test": o.key.cond.URL, "state": o.state,
		"first": formatTime(o.first), "since": formatTime(o.since), "end": formatTime(o.end),
		"nodes": nodes, "reason": reason, "display": outageDisplay(o, c.outageGroupsLocked(o)),
	}
	if by != nil {
		fields["by"] = by
	}
	if o.state == OutageEstablished {
		fields["counts"] = CountEvents(c.quality.pools[o.key], c.now())
	}
	c.records.appendPerformance(fields)
}

// outageGroupsLocked names the groups referring to a node of an outage.
func (c *Coordinator) outageGroupsLocked(o *outage) []string {
	var names []string
	for _, reg := range c.registrations {
		if reg.persistent || reg.state != stateActive {
			continue
		}
		for k := range o.dead {
			if reg.group.References(k.Target, k.Cond) {
				names = append(names, reg.group.Name())
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

// pruneOutagesLocked forgets the nodes no group refers to any more: an outage left without
// nodes cannot be concluded and ends undecided.
func (c *Coordinator) pruneOutagesLocked(referenced map[Key]struct{}) {
	for pk, list := range c.outages.recent {
		kept := list[:0]
		for _, f := range list {
			if _, ok := referenced[f.key]; ok {
				kept = append(kept, f)
			}
		}
		clearAfter(list, len(kept))
		if len(kept) == 0 {
			delete(c.outages.recent, pk)
		} else {
			c.outages.recent[pk] = kept
		}
	}
	for _, o := range c.outages.active {
		for k := range o.dead {
			if _, ok := referenced[k]; !ok {
				delete(o.dead, k)
			}
		}
		if len(o.dead) == 0 {
			c.concludeOutageLocked(o, OutageUndecided, map[string]any{"reason": "removed"})
			continue
		}
		c.settleVerdictLocked(o)
	}
	c.attributeDueLocked()
}

func (c *Coordinator) deliverOutageLocked(o *outage) {
	notice := OutageChanged{Pool: o.key.pool, Cond: o.key.cond, State: o.state, First: o.first, Since: o.since, End: o.end}
	for _, reg := range c.registrations {
		reg.group.Deliver(notice)
	}
}

// recoveringLocked is an automatic recovery test of a dead node of an outage still running,
// whether it started before the outage was suspected or as its representative.
func (c *Coordinator) recoveringLocked(o *outage) *execution {
	for exec := range c.inflight {
		if exec.manualAPI != "" || exec.class != ClassMaintain {
			continue
		}
		if _, dead := o.dead[exec.key]; dead {
			return exec
		}
	}
	return nil
}

// outageBackoff is how long a suspected outage waits between the recovery tests of its nodes.
func outageBackoff(o *outage, now time.Time) time.Duration {
	limit := BackoffMax
	if now.Sub(o.first) > LongOutageAfter {
		limit = BackoffMaxLong
	}
	return backoffUpTo(o.failures, limit)
}

// mergedRecovery reports whether a need of key is a recovery test an outage merges, and if so
// whether it is the one to run now.
func (c *Coordinator) mergedRecoveryLocked(key Key, need *Need) (merged bool, o *outage) {
	if need.Class != ClassMaintain {
		return false, nil
	}
	pk, ok := nodePoolKey(key)
	if !ok {
		return false, nil
	}
	o = c.outages.active[pk]
	if o == nil {
		return false, nil
	}
	if _, dead := o.dead[key]; !dead {
		return false, nil
	}
	if ev := c.evidence[key]; ev == nil || ev.State != Dead {
		return false, nil
	}
	return true, o
}

// representativeLocked picks, among the keys of an outage with a pending recovery need, the
// one after the node tested last, in a fixed order.
func (c *Coordinator) representativeLocked(o *outage, waiting []Key) Key {
	sort.Slice(waiting, func(i, j int) bool { return c.objectIDLocked(waiting[i].Target) < c.objectIDLocked(waiting[j].Target) })
	for _, k := range waiting {
		if c.objectIDLocked(k.Target) > o.last {
			return k
		}
	}
	return waiting[0]
}

// Outages lists the outages of pools suspected now.
func (c *Coordinator) Outages() []OutageChanged {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	list := make([]OutageChanged, 0, len(c.outages.active))
	for _, o := range c.outages.active {
		list = append(list, OutageChanged{Pool: o.key.pool, Cond: o.key.cond, State: o.state, First: o.first, Since: o.since})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Pool < list[j].Pool })
	return list
}

// OutageOf returns the suspected outage of a pool under a condition, if any.
func (c *Coordinator) OutageOf(pool string, cond Condition) (OutageChanged, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if o := c.outages.active[poolKey{pool, cond}]; o != nil {
		return OutageChanged{Pool: pool, Cond: cond, State: o.state, First: o.first, Since: o.since}, true
	}
	return OutageChanged{}, false
}
