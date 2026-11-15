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
// and one of the nodes that witnessed it coming back establishes it.
//
// An established outage counts against the pool only once its witnesses are attributed: none
// of them may have failed while the local network did not seem normal.
type outage struct {
	key   poolKey
	state string
	first time.Time // the earliest failure of it started then
	since time.Time // suspected
	end   time.Time
	// dead are the nodes still dead, with when their failure started: their recovery tests are
	// merged
	dead map[Key]time.Time
	// witness are the nodes whose failures witness it: those it was suspected from, and those
	// that failed in it since its first. A node that failed before it, former, takes part in the
	// recovery tests only: that it works, when back or later, tells nothing of this outage, also
	// once it failed again in it.
	witness map[Key]*witness
	former  map[Key]bool
	// covered are the witnesses that failed while the local network did not seem normal, noted
	// as soon as that is known, as the coverage and the nodes may be gone before it counts
	covered map[Key]coveredWitness
	// commitAt is when an established outage is attributed; committed once it is, counted
	// when it counted against the pool
	commitAt  time.Time
	committed bool
	counted   bool
	expired   bool // counted, it began longer ago than the memory of the pool reaches
	// the recovery tests: one node at a time, in turn, as often as the outage backs off
	failures int
	nextAt   time.Time
	last     uint64 // object number of the node tested last
	// verdicts are the successes that may conclude it, waiting for the tests of the pool that
	// started earlier and still run; what each tells is judged once they are in, as their
	// failures may change who witnessed it
	verdicts []*verdict
	// orphaned is set once no witness or no dead node of it is left: it ends undecided unless
	// a success waiting concludes it
	orphaned bool
}

type verdict struct {
	key     Key // of the node found alive
	started time.Time
	exec    uint64 // the test, after any test that started at the same time for a connection
	by      map[string]any
}

// earlier orders the results that conclude an outage: by when they started, then by test.
func earlier(started time.Time, exec uint64, than *verdict) bool {
	return started.Before(than.started) || (started.Equal(than.started) && exec < than.exec)
}

// involved are the nodes an outage took in: those still dead, its witnesses, and those that
// failed before it.
func (o *outage) involved() []Key {
	seen := map[Key]bool{}
	var keys []Key
	add := func(k Key) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range o.dead {
		add(k)
	}
	for k := range o.witness {
		add(k)
	}
	for k := range o.former {
		add(k)
	}
	return keys
}

func (o *outage) covers(t time.Time) bool {
	return !t.Before(o.first) && (o.end.IsZero() || t.Before(o.end))
}

// witness is a node whose failures witness an outage: when it first failed in it, and every
// failure it had in it, with what it was, which the records keep once it is gone.
type witness struct {
	name     string
	object   uint64
	first    time.Time
	failures []time.Time
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
	// alive is when the latest test that found a node of the pool alive under the condition
	// started, however the evidence of the node changed since
	alive map[poolKey]time.Time
}

func newOutages() *outages {
	return &outages{recent: map[poolKey][]poolFailure{}, active: map[poolKey]*outage{}, success: map[string]time.Time{}, alive: map[poolKey]time.Time{}}
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

// Covered reports whether a test that started then along some pools may have failed for no
// fault of its own: the local network did not seem normal, or an outage of one of the pools
// under cond was suspected or not disproved.
func (c *Coordinator) Covered(started time.Time, pools []string, cond Condition) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.expireNetworkLocked()
	if c.network.covers(started) {
		return true
	}
	for _, pool := range pools {
		if o := c.outageCovering(poolKey{pool, cond}, started); o != nil && o.state != OutageDisproved {
			return true
		}
	}
	return false
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
	// A failure that started within an outage concluded, coming late, belongs to it: one
	// established but not attributed yet takes it as a witness. It suggests no other outage.
	concluded := false
	for _, o := range c.outages.past {
		if o.key != pk || !o.covers(started) {
			continue
		}
		concluded = true
		if o.state == OutageEstablished && !o.committed {
			c.addWitnessLocked(o, key, started)
			if due := started.Add(AttributionDelay); due.After(o.commitAt) {
				o.commitAt = due
			}
		}
		break
	}
	if o := c.outages.active[pk]; o != nil {
		if prev, ok := o.dead[key]; !ok || started.After(prev) {
			o.dead[key] = started // its latest failure
		}
		if !started.Before(o.first) {
			c.addWitnessLocked(o, key, started)
		} else if o.witness[key] == nil {
			o.former[key] = true
		}
		return
	}
	// a failure the local network explains suggests no outage of the pool
	if concluded || c.network.covers(started) {
		return
	}
	// The recent failures span OutageWindow up to the latest of them: one that started earlier,
	// coming late, joins none of them.
	list := c.outages.recent[pk]
	latest := started
	for _, f := range list {
		if f.started.After(latest) {
			latest = f.started
		}
	}
	if latest.Sub(started) >= OutageWindow {
		return
	}
	kept := list[:0]
	for _, f := range list {
		if f.key != key && latest.Sub(f.started) < OutageWindow {
			kept = append(kept, f)
		}
	}
	clearAfter(list, len(kept))
	kept = append(kept, poolFailure{key: key, started: started})
	c.outages.recent[pk] = kept
	if c.network.state != NetworkNormal {
		return
	}
	// Only the failures that started after a node of the pool was last found alive, or last
	// carried a new connection, suggest it is out; a node dead since earlier takes part as one
	// that failed before.
	since := c.outages.success[pk.pool]
	if alive := c.outages.alive[pk]; alive.After(since) {
		since = alive
	}
	for k, ev := range c.evidence {
		if other, ok := nodePoolKey(k); ok && other == pk && ev.State == Alive && ev.Started.After(since) {
			since = ev.Started
		}
	}
	var failed []poolFailure
	first := time.Time{}
	for _, f := range kept {
		if f.started.After(since) {
			failed = append(failed, f)
			if first.IsZero() || f.started.Before(first) {
				first = f.started
			}
		}
	}
	if len(failed) < OutageNodes {
		return
	}
	o := &outage{key: pk, state: OutageSuspected, first: first, since: c.now(), dead: map[Key]time.Time{}, witness: map[Key]*witness{}, former: map[Key]bool{}}
	for _, f := range failed {
		o.dead[f.key] = f.started
		c.addWitnessLocked(o, f.key, f.started)
	}
	// Nodes found dead earlier that are still dead belong to it too.
	for k, ev := range c.evidence {
		if other, ok := nodePoolKey(k); ok && other == pk && ev.State == Dead {
			if _, in := o.dead[k]; !in {
				o.dead[k] = ev.DeadSince
				o.former[k] = true
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
	if started.After(c.outages.alive[pk]) {
		c.outages.alive[pk] = started
	}
	c.forgetRecentLocked(pk, key.Target, started)
	o := c.outages.active[pk]
	if o == nil || started.Before(o.first) {
		return
	}
	if aliveVerdict(o, key, started) == "" {
		// a node that failed before the outage and is back leaves its recovery tests
		if ev := c.evidence[key]; ev != nil && ev.State == Alive {
			delete(o.dead, key)
		}
	}
	// what it tells is judged once the tests that started before it are in
	c.offerVerdictLocked(o, key, started, exec, map[string]any{"probe": exec, "node": c.nodeRef(key.Target)})
}

// forgetRecentLocked drops the recent failures of a node of a pool that a success that started
// later ended: they suggest no outage with the failures to come.
func (c *Coordinator) forgetRecentLocked(pk poolKey, target C.Proxy, started time.Time) {
	list := c.outages.recent[pk]
	kept := list[:0]
	for _, f := range list {
		if f.key.Target != target || !f.started.Before(started) {
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

// addWitnessLocked takes a failure of a node in an outage, noting it when the local network
// explains it.
func (c *Coordinator) addWitnessLocked(o *outage, key Key, started time.Time) {
	w := o.witness[key]
	if w == nil {
		w = &witness{name: key.Target.Name(), object: c.objectIDLocked(key.Target), first: started}
		o.witness[key] = w
	} else if started.Before(w.first) {
		w.first = started
	}
	w.failures = append(w.failures, started)
	c.markCoveredLocked(o)
}

type coveredWitness struct {
	name   string
	object uint64
	failed time.Time
}

// markCoveredLocked notes the witnesses of an outage the local network explains.
func (c *Coordinator) markCoveredLocked(o *outage) {
	for k, w := range o.witness {
		if _, ok := o.covered[k]; ok {
			continue
		}
		for _, at := range w.failures {
			if c.network.covers(at) {
				if o.covered == nil {
					o.covered = map[Key]coveredWitness{}
				}
				o.covered[k] = coveredWitness{name: w.name, object: w.object, failed: at}
				break
			}
		}
	}
}

// aliveVerdict is what a node found alive by what started then tells of an outage: one of its
// witnesses back establishes it, any node alive before it failed disproves it, and a node that
// failed before it tells nothing, when back or later.
func aliveVerdict(o *outage, key Key, started time.Time) string {
	if w := o.witness[key]; w != nil && !started.Before(w.first) {
		return OutageEstablished
	}
	if o.former[key] {
		return ""
	}
	return OutageDisproved
}

// sameTest reports whether a test under one condition tells of another: they differ in their
// timeouts at most.
func sameTest(a, b Condition) bool {
	return a.URL == b.URL && a.Expected == b.Expected
}

// poolTestRunningLocked reports whether a test of a node of the pool of an outage, under its
// condition or one that tells of it, that started as told still runs.
func (c *Coordinator) poolTestRunningLocked(o *outage, started func(*execution) bool) bool {
	for exec := range c.inflight {
		if exec.void || !started(exec) {
			continue
		}
		if pk, ok := nodePoolKey(exec.key); ok && pk.pool == o.key.pool && sameTest(pk.cond, o.key.cond) {
			return true
		}
	}
	return false
}

// offerVerdictLocked takes a success of a node that may conclude an outage. The one that
// started first and tells something decides, whatever order they end in: a test of the pool
// that started earlier and still runs is waited for.
func (c *Coordinator) offerVerdictLocked(o *outage, key Key, started time.Time, exec uint64, by map[string]any) {
	o.verdicts = append(o.verdicts, &verdict{key: key, started: started, exec: exec, by: by})
	c.settleVerdictLocked(o)
}

func (c *Coordinator) settleVerdictLocked(o *outage) {
	if len(o.verdicts) == 0 || c.outages.active[o.key] != o {
		return
	}
	sort.Slice(o.verdicts, func(i, j int) bool { return earlier(o.verdicts[i].started, o.verdicts[i].exec, o.verdicts[j]) })
	for len(o.verdicts) > 0 {
		v := o.verdicts[0]
		// any test of the pool that started before may still change who witnessed it
		if c.poolTestRunningLocked(o, func(exec *execution) bool { return earlier(exec.started, exec.id, v) }) {
			return
		}
		// judged on what the tests in so far tell of who witnessed it
		if state := aliveVerdict(o, v.key, v.started); state != "" {
			c.concludeOutageLocked(o, state, v.by)
			return
		}
		// it tells nothing of the outage; that the node works since it last failed takes it out
		// of the recovery tests: a connection dialed since, or a test its evidence took
		if failed, dead := o.dead[v.key]; dead && v.started.After(failed) {
			if ev := c.evidence[v.key]; v.exec == math.MaxUint64 || ev != nil && ev.State == Alive {
				delete(o.dead, v.key)
			}
		}
		o.verdicts[0] = nil
		o.verdicts = o.verdicts[1:]
	}
	if o.orphaned {
		c.concludeOutageLocked(o, OutageUndecided, map[string]any{"reason": "removed"})
	}
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
	for pk := range c.outages.recent {
		if pk.pool == pool {
			c.forgetRecentLocked(pk, target, dialed)
		}
	}
	for pk, o := range c.outages.active {
		if pk.pool != pool || dialed.Before(o.since) {
			continue
		}
		key := Key{Target: target, Cond: pk.cond}
		if aliveVerdict(o, key, dialed) == "" {
			// it works since it last failed: it leaves the recovery tests
			if failed, dead := o.dead[key]; dead && dialed.After(failed) {
				delete(o.dead, key)
			}
		}
		c.offerVerdictLocked(o, key, dialed, math.MaxUint64, map[string]any{"connection": formatTime(dialed), "node": c.nodeRef(target)})
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
	// the nodes it took in, also those that left the recovery tests on a connection working
	for _, k := range o.involved() {
		if ev := c.evidence[k]; ev != nil && ev.State == Dead {
			ev.Failures = 0
			ev.NextAllowed = now
		}
	}
	if state == OutageEstablished {
		// counted once its witnesses are attributed, as late as a check may still explain them
		o.commitAt = now
		for _, w := range o.witness {
			for _, at := range w.failures {
				if due := at.Add(AttributionDelay); due.After(o.commitAt) {
					o.commitAt = due
				}
			}
		}
	}
	c.recordOutageLocked(o, "concluded", by)
	c.deliverOutageLocked(o)
	c.attributeDueLocked()
}

// commitOutagesLocked attributes the established outages that are due: one counts against its
// pool unless a witness of it failed while the local network did not seem normal. A test of the
// pool that started within it and still runs may find another witness: it is waited for.
func (c *Coordinator) commitOutagesLocked() {
	now := c.now()
	for _, o := range c.outages.past {
		if o.state != OutageEstablished || o.committed || now.Before(o.commitAt) {
			continue
		}
		if c.poolTestRunningLocked(o, func(exec *execution) bool { return exec.started.Before(o.end) }) {
			continue
		}
		o.committed = true
		c.markCoveredLocked(o)
		list := make([]coveredWitness, 0, len(o.covered))
		for _, w := range o.covered {
			list = append(list, w)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].object < list[j].object })
		var covered []any
		for _, w := range list {
			covered = append(covered, map[string]any{"name": w.name, "object": w.object, "failed": formatTime(w.failed)})
		}
		by := map[string]any{"covered": covered}
		if len(covered) == 0 {
			// it explains the failures in it; one that began longer ago than the memory reaches
			// adds nothing to it
			o.counted = true
			events := pruneEvents(c.quality.pools[o.key], now)
			c.quality.pools[o.key] = append(events, QualityEvent{At: o.first, Weight: 1})
			if now.Sub(o.first) >= LongWindow {
				o.expired = true
				by["expired"] = true
			}
		}
		c.recordOutageLocked(o, "committed", by)
		c.deliverOutageLocked(o)
	}
}

func (c *Coordinator) recordOutageLocked(o *outage, reason string, by map[string]any) {
	var nodes []any
	keys := o.involved()
	sort.Slice(keys, func(i, j int) bool { return c.objects[keys[i].Target] < c.objects[keys[j].Target] })
	for _, k := range keys {
		node := map[string]any{"name": k.Target.Name(), "object": c.objects[k.Target]}
		if failed, dead := o.dead[k]; dead {
			node["failed"] = formatTime(failed)
		}
		if w := o.witness[k]; w != nil {
			node["name"], node["object"] = w.name, w.object // as it was when it failed
			node["witness"] = true
			if _, dead := o.dead[k]; !dead {
				node["failed"] = formatTime(w.failures[len(w.failures)-1])
			}
		}
		nodes = append(nodes, node)
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
		if o.committed {
			fields["counted"] = o.counted
			fields["counts"] = CountEvents(c.quality.pools[o.key], c.now())
		} else {
			fields["commitAt"] = formatTime(o.commitAt)
		}
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
		for _, k := range o.involved() {
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
	// a time a pool worked tells of no failure that may still suggest an outage once older than
	// OutageWindow and than the recent failures
	now := c.now()
	for pk, alive := range c.outages.alive {
		if now.Sub(alive) < OutageWindow {
			continue
		}
		before := false
		for _, f := range c.outages.recent[pk] {
			if f.started.Before(alive) {
				before = true
			}
		}
		if !before {
			delete(c.outages.alive, pk)
		}
	}
	for _, o := range c.outages.active {
		for k := range o.dead {
			if _, ok := referenced[k]; !ok {
				delete(o.dead, k)
			}
		}
		// only a witness coming back establishes it: with none left, or no node left to test,
		// and no conclusion waiting for earlier tests, it cannot be concluded
		witnesses := 0
		for k := range o.witness {
			if _, ok := referenced[k]; ok {
				witnesses++
			}
		}
		// once the successes waiting are judged, if none concludes it
		o.orphaned = len(o.dead) == 0 || witnesses == 0
		c.settleVerdictLocked(o)
		if c.outages.active[o.key] == o && o.orphaned && len(o.verdicts) == 0 {
			c.concludeOutageLocked(o, OutageUndecided, map[string]any{"reason": "removed"})
		}
	}
	c.attributeDueLocked()
}

func (c *Coordinator) deliverOutageLocked(o *outage) {
	notice := OutageChanged{Pool: o.key.pool, Cond: o.key.cond, State: o.state, First: o.first, Since: o.since, End: o.end, Committed: o.committed, Counted: o.counted}
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
		if _, dead := o.dead[exec.key]; dead && exec.recovery {
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
