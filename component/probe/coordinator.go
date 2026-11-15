package probe

import (
	"context"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type registrationState uint8

const (
	// stateInactive is a group published by a config not yet activated: it receives notices
	// and keeps its state, but its needs are ignored.
	stateInactive registrationState = iota
	stateActive
	// stateRetiring is a group of the config being replaced: like an inactive one until the
	// new config is activated, when it is dropped.
	stateRetiring
)

type needStatus uint8

const (
	needPending needStatus = iota // waiting for its eligible time and a slot
	needRunning                   // attached to a running execution
	needDone                      // satisfied, by an execution or by existing evidence
)

type needEntry struct {
	id         uint64
	need       Need
	reg        *Registration
	status     needStatus
	eligibleAt time.Time
	waiting    string // what a pending need last waited for, recorded once
}

type keyEligibility uint8

const (
	keyRevoked keyEligibility = iota
	keyReferenced
	keyUnreadPrimary // existing ordinary evidence, until the next prune or reader
)

type execution struct {
	id      uint64
	key     Key
	class   Class
	started time.Time
	// needs are the needs still waiting for the result; requests are all the needs the
	// execution served, which the records tell even if a need was withdrawn meanwhile.
	needs    []*needEntry
	requests []Request
	// keys records ordinary write eligibility at the start. Revoked entries also
	// exclude the key from pool witnesses for the lifetime of this execution.
	keys map[Key]keyEligibility
	void bool // a prune left no key that can take this execution
	// recovery is set on an automatic test of a node dead when it started
	recovery  bool
	manualAPI string
	// via are the groups judging their own availability a manual test of them was taken to
	// the member it tests for them
	via []C.Proxy
	// What the target was when the test started, recorded with its result even if the
	// target is dropped meanwhile.
	identity map[string]any
	// The contexts in effect when the test started.
	config, exclusions uint64
	// chargedKeys are the keys with an adopted first failure or reconfirmation, decided
	// for all keys before the test can move local network coverage.
	chargedKeys map[Key]bool
	// comparing is set when the test started only for comparisons, of a target not dead
	// then: it runs marked so, no other need joins it, and a failure of it is not taken in
	comparing bool
}

// comparesOnly reports whether every need the execution serves is a comparison.
func (exec *execution) comparesOnly() bool {
	if exec.manualAPI != "" || exec.recovery || len(exec.needs) == 0 {
		return false
	}
	for _, entry := range exec.needs {
		if entry.need.Source != SourceCompare {
			return false
		}
	}
	return true
}

// Registration is a group registered with the coordinator.
type Registration struct {
	id          uint64
	group       Group
	coordinator *Coordinator
	state       registrationState
	persistent  bool // stays active across reloads, as the census does
	version     uint64
	needs       map[Key]*needEntry
}

// Coordinator keeps the evidence, runs the tests the groups need, tells the groups what they
// found, and records it all.
type Coordinator struct {
	mutex      sync.Mutex
	now        func() time.Time
	runner     func(ctx context.Context, key Key) C.ProbeResult
	records    *Records
	exclusions *Exclusions
	lookup     atomic.Pointer[func(name string) C.Proxy]
	// nodes finds the node a route went through by name, those of providers included
	nodes atomic.Pointer[func(name string) C.Proxy]

	evidence      map[Key]*Evidence
	objects       map[C.Proxy]uint64
	nextObject    uint64
	registrations map[uint64]*Registration
	nextGroup     uint64
	running       map[Key]*execution
	inflight      map[*execution]struct{} // automatic and manual
	slots         [classCount]int
	rotation      [classCount]uint64
	nextExec      uint64
	nextNeed      uint64
	nextVersion   uint64
	timer         *time.Timer
	timerAt       time.Time
	// publishing is set while a new config is published and not yet activated: no
	// automatic test starts then, those of the census included.
	publishing bool

	census *census
	// delegates are the keys whose evidence a group publishes, by Publish; delegateTargets
	// counts them by target.
	delegates       map[Key]*delegate
	delegateTargets map[C.Proxy]int

	quality *quality
	network *network
	outages *outages
	// lastAlive is when a test last found each proxy alive: the check of the local network
	// connects to proxies that worked lately, dead since or not.
	lastAlive map[C.Proxy]time.Time
	// resolveHop finds the address a check of the local network connects to for a node, and
	// dialHop connects to it once as the node would; spacing separates the connections to it.
	resolveHop func(ctx context.Context, p C.Proxy) (netip.AddrPort, error)
	dialHop    func(ctx context.Context, p C.Proxy, addr netip.AddrPort) (time.Duration, error)
	spacing    time.Duration
}

// Default is the coordinator of this process.
var Default = New(time.Now, nil)

// New returns a coordinator. runner runs one test; nil runs it on the proxy itself.
func New(now func() time.Time, runner func(ctx context.Context, key Key) C.ProbeResult) *Coordinator {
	instance, startedAt := statistic.DefaultUsage.Instance()
	c := &Coordinator{
		now:             now,
		runner:          runner,
		records:         newRecords(instance, startedAt, now, EventCapacity),
		evidence:        map[Key]*Evidence{},
		objects:         map[C.Proxy]uint64{},
		registrations:   map[uint64]*Registration{},
		delegates:       map[Key]*delegate{},
		delegateTargets: map[C.Proxy]int{},
		running:         map[Key]*execution{},
		inflight:        map[*execution]struct{}{},
		quality:         newQuality(),
		network:         &network{},
		outages:         newOutages(),
		lastAlive:       map[C.Proxy]time.Time{},
		resolveHop:      resolveHop,
		dialHop:         dialHop,
		spacing:         LocalSpacing,
	}
	if c.runner == nil {
		c.runner = runTest
	}
	c.exclusions = newExclusions(now, c.commitExclusions, c.exclusionsChanged)
	c.records.setParams(params())
	c.records.SetExclusions(1, map[string]any{"pools": []ExclusionEntry{}}, false)
	c.census = newCensus(c)
	return c
}

func params() map[string]any {
	return map[string]any{
		"kind":               ContextParams,
		"schema":             SchemaVersion,
		"version":            C.Version,
		"useWindow":          UseWindow.Milliseconds(),
		"silentAfter":        SilentAfter.Milliseconds(),
		"udpSilentCount":     UDPSilentCount,
		"udpSilentWindow":    UDPSilentWindow.Milliseconds(),
		"reuseWindow":        ReuseWindow.Milliseconds(),
		"backoffFirst":       BackoffFirst.Milliseconds(),
		"backoffMax":         BackoffMax.Milliseconds(),
		"poolSize":           PoolSize,
		"refillRange":        RefillRange,
		"batchSize":          BatchSize,
		"poolFresh":          PoolFresh.Milliseconds(),
		"poolFreshSwitched":  PoolFreshSwitched.Milliseconds(),
		"switchedWindow":     SwitchedWindow.Milliseconds(),
		"poolJitter":         PoolJitter,
		"cooldownFirst":      CooldownFirst.Milliseconds(),
		"cooldownMax":        CooldownMax.Milliseconds(),
		"cooldownReset":      CooldownReset.Milliseconds(),
		"repairWindow":       RepairWindow.Milliseconds(),
		"verifySlots":        VerifySlots,
		"searchSlots":        SearchSlots,
		"maintainSlots":      MaintainSlots,
		"eventCapacity":      EventCapacity,
		"signalsPerMinute":   SignalsPerMinute,
		"defaultTestTimeout": DefaultTestTimeout.Milliseconds(),
		"fastDial":           FastDial.Milliseconds(),
		"fastProbe":          map[string]any{"limit": FastProbe.Milliseconds(), "ofTimeout": 0.5},
		"slowerRatio":        SlowerRatio,
		"certainWindow":      CertainWindow.Milliseconds(),
		"hedgeAttempts":      HedgeAttempts,
		"hedgeInFlight":      HedgeInFlight,
		"dialBudget":         DialBudget,
		"pairWindow":         PairWindow.Milliseconds(),
		"attributionDelay":   AttributionDelay.Milliseconds(),
		"localCheck": map[string]any{
			"failures": LocalFailures, "window": LocalWindow.Milliseconds(), "targets": LocalTargets,
			"candidates": LocalCandidates, "resolve": LocalResolve.Milliseconds(), "samples": LocalSamples,
			"spacing": LocalSpacing.Milliseconds(), "timeout": LocalCheckTimeout.Milliseconds(), "transport": "tcp",
			"slowExtra": LocalSlowExtra.Milliseconds(), "badShare": LocalBadShare, "clean": LocalClean,
			"recovered": LocalRecovered, "confirm": LocalConfirm.Milliseconds(), "interval": CheckInterval.Milliseconds(),
			"downInterval": DownInterval.Milliseconds(), "slow": FastDial.Milliseconds(), "idle": UseWindow.Milliseconds(),
		},
		"outage":          map[string]any{"nodes": OutageNodes, "window": OutageWindow.Milliseconds()},
		"persistentAfter": PersistentAfter.Milliseconds(),
		"longOutageAfter": LongOutageAfter.Milliseconds(),
		"backoffMaxLong":  BackoffMaxLong.Milliseconds(),
		"quality": map[string]any{
			"recentWindow": RecentWindow.Milliseconds(), "longWindow": LongWindow.Milliseconds(),
			"yield":         map[string]any{"recent": YieldRecent, "long": YieldLong},
			"return":        map[string]any{"recent": ReturnRecent, "long": ReturnLong},
			"failureWeight": 1, "degradeWeight": DegradeWeight, "degradeMerge": DegradeMerge.Milliseconds(),
			"poolWeight": 1, "trial": TrialLength.Milliseconds(), "readmitFirst": ReadmitFirst.Milliseconds(),
			"readmitMax": ReadmitMax.Milliseconds(), "repeatWindow": RepeatWindow.Milliseconds(),
		},
		"standbys": map[string]any{
			"trusted": TrustedStandbys, "trustedRisky": TrustedStandbysAt, "min": PoolSize, "max": PoolMax,
			"refillHealthy": RefillRange, "refillMax": RefillMax,
		},
		"summaryDelay": SummaryDelay.Milliseconds(),
		"display":      DisplayVersion,
		"compare": map[string]any{
			"timely": CompareTimely.Milliseconds(), "every": CompareEvery.Milliseconds(), "jitter": CompareJitter,
			"challengers": CompareChallenge, "start": CompareStart.Milliseconds(), "window": PairWindow.Milliseconds(),
			"kept": CompareKept, "recent": CompareRecent, "minimum": CompareMinimum, "wins": CompareWins,
			"expiry": CompareExpiry.Milliseconds(), "stay": CompareStay.Milliseconds(),
			"priority": ComparePriority, "costWindow": CompareCostWindow,
		},
	}
}

// runTest runs a URL test on the target itself.
func runTest(ctx context.Context, key Key) C.ProbeResult {
	expected, _ := utils.NewUnsignedRanges[uint16](key.Cond.Expected)
	if tester, ok := key.Target.(C.ProbeTester); ok {
		return tester.ProbeURL(ctx, key.Cond.URL, expected)
	}
	started := time.Now()
	delay, err := key.Target.URLTest(ctx, key.Cond.URL, expected)
	result := C.ProbeResult{Started: started, Elapsed: time.Since(started)}
	switch {
	case err == nil && delay > 0:
		result.Satisfied, result.Delay = true, delay
	case err != nil:
		result.ErrorClass, result.Error = C.ProbeErrorOther, err.Error()
	default:
		result.ErrorClass = C.ProbeErrorStatus
	}
	return result
}

// dialedAdapter is the outbound the dialer route of a test starts from: the one it dialed, as
// a group passes the dial on to its member, or the target itself.
func dialedAdapter(target C.Proxy, result C.ProbeResult) C.ProxyAdapter {
	if result.Outbound != nil {
		return result.Outbound
	}
	return target.Adapter()
}

// Now is the time of the coordinator.
func (c *Coordinator) Now() time.Time {
	return c.now()
}

// outageLocked is how long a dead proxy has been dead, the time the local network seemed down
// or poor left out, that of the current such state too.
func (c *Coordinator) outageLocked(ev *Evidence, now time.Time) time.Duration {
	if ev.State != Dead || ev.DeadSince.IsZero() {
		return 0
	}
	return now.Sub(ev.DeadSince) - c.network.excused(ev.DeadSince, now)
}

// DeadFor is how long a proxy under a condition has been dead, the time the local network did
// not seem normal left out; zero when it is not dead.
func (c *Coordinator) DeadFor(key Key) time.Duration {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ev, ok := c.evidence[key]; ok {
		return c.outageLocked(ev, c.now())
	}
	return 0
}

// Persistent reports whether a proxy under a condition has been dead long enough to count as
// down rather than unstable.
func (c *Coordinator) Persistent(key Key) bool {
	return c.DeadFor(key) >= PersistentAfter
}

// Records returns the decision records.
func (c *Coordinator) Records() *Records {
	return c.records
}

// Exclusions returns the exclusion table.
func (c *Coordinator) Exclusions() *Exclusions {
	return c.exclusions
}

// SetLookup sets how proxies and groups are found by name, as dialer-proxy names them.
func (c *Coordinator) SetLookup(lookup func(name string) C.Proxy) {
	c.lookup.Store(&lookup)
}

// Lookup finds a proxy or group by name in the published config.
func (c *Coordinator) Lookup(name string) C.Proxy {
	if lookup := c.lookup.Load(); lookup != nil {
		return (*lookup)(name)
	}
	return nil
}

// SetNodeLookup sets how the nodes the routes of tests went through are found by name, among
// the nodes of the providers too; nil when the name tells no one node.
func (c *Coordinator) SetNodeLookup(lookup func(name string) C.Proxy) {
	c.nodes.Store(&lookup)
}

func (c *Coordinator) lookupNode(name string) C.Proxy {
	if lookup := c.nodes.Load(); lookup != nil {
		return (*lookup)(name)
	}
	return c.Lookup(name)
}

// NewEvaluator returns an exclusion evaluator of the current table.
func (c *Coordinator) NewEvaluator() *Evaluator {
	return c.exclusions.NewEvaluator(c.Lookup)
}

// ObjectID returns the number of a proxy object, unique within this process.
func (c *Coordinator) ObjectID(p C.Proxy) uint64 {
	if p == nil {
		return 0
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.objectIDLocked(p)
}

func (c *Coordinator) objectIDLocked(p C.Proxy) uint64 {
	if id, ok := c.objects[p]; ok {
		return id
	}
	c.nextObject++
	c.objects[p] = c.nextObject
	return c.nextObject
}

// KnownObjectID returns the number of a proxy object already numbered, 0 for one that is not:
// recording an object of an earlier config must not keep it from being released.
func (c *Coordinator) KnownObjectID(p C.Proxy) uint64 {
	if p == nil {
		return 0
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.objects[p]
}

// Register registers a group published by a config. It receives notices at once; its needs
// count once the config is activated.
func (c *Coordinator) Register(group Group) *Registration {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nextGroup++
	reg := &Registration{id: c.nextGroup, group: group, coordinator: c, needs: map[Key]*needEntry{}}
	c.registrations[reg.id] = reg
	return reg
}

func (c *Coordinator) registerPersistent(group Group) *Registration {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nextGroup++
	reg := &Registration{id: c.nextGroup, group: group, coordinator: c, needs: map[Key]*needEntry{}, state: stateActive, persistent: true}
	c.registrations[reg.id] = reg
	return reg
}

// BeginPublish is called before a new config publishes its proxies: the groups registered so
// far stop asking for tests, and keep receiving notices until Activate.
func (c *Coordinator) BeginPublish() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.publishing = true
	for _, reg := range c.registrations {
		if reg.state == stateActive && !reg.persistent {
			reg.state = stateRetiring
			c.cancelNeedsLocked(reg, "reload")
		}
	}
	c.forwardAllLocked()
}

// Retiring returns the retiring group of name, whose state a new group inherits.
func (c *Coordinator) Retiring(name string) Group {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, reg := range c.registrations {
		if reg.state == stateRetiring && reg.group.Name() == name {
			return reg.group
		}
	}
	return nil
}

// Activate makes the registered inactive groups active and drops the retiring ones. Evidence
// of proxies no active group refers to is dropped, and their running tests change nothing.
// keys lists what the active groups refer to; retain lists the proxies of the config, whose
// object numbers are kept.
func (c *Coordinator) Activate(keys []Key, retain []C.Proxy) {
	c.mutex.Lock()
	c.publishing = false
	for id, reg := range c.registrations {
		switch reg.state {
		case stateRetiring:
			delete(c.registrations, id)
			c.dropDelegatesLocked(reg)
		case stateInactive:
			reg.state = stateActive
		}
	}
	c.pruneLocked(keys, retain)
	c.forwardAllLocked()
	c.mutex.Unlock()
	c.kick()
}

// Prune drops the evidence of proxies no group refers to any more, as after a provider
// update; keys lists what the groups refer to, and retain the proxies of the config.
func (c *Coordinator) Prune(keys []Key, retain []C.Proxy) {
	c.mutex.Lock()
	c.pruneLocked(keys, retain)
	c.forwardAllLocked()
	c.mutex.Unlock()
	c.kick()
}

func (c *Coordinator) pruneLocked(keys []Key, retain []C.Proxy) {
	// Once a test has seen a new evidence key, losing that reader cannot turn
	// its old result into a pool-only witness for the same condition.
	byTarget := make(map[C.Proxy][]*execution)
	for exec := range c.inflight {
		if !exec.void {
			byTarget[exec.key.Target] = append(byTarget[exec.key.Target], exec)
		}
	}
	excludeNewKey := func(key Key) {
		for _, exec := range byTarget[key.Target] {
			if _, known := exec.keys[key]; !known && sameTest(exec.key.Cond, key.Cond) {
				exec.keys[key] = keyRevoked
			}
		}
	}
	referenced := make(map[Key]struct{}, len(keys))
	targets := make(map[C.Proxy]struct{}, len(keys)+len(retain))
	used := map[string]map[Condition]bool{}
	for _, key := range keys {
		referenced[key] = struct{}{}
		targets[key.Target] = struct{}{}
		excludeNewKey(key)
		if pk, ok := nodePoolKey(key); ok {
			if used[pk.pool] == nil {
				used[pk.pool] = map[Condition]bool{}
			}
			used[pk.pool][pk.cond] = true
		}
	}
	c.outages.used = used
	for _, p := range retain {
		targets[p] = struct{}{}
	}
	for key := range c.evidence {
		// a group judging its own availability publishes only what changes
		if _, ok := referenced[key]; !ok && c.delegates[key] == nil {
			excludeNewKey(key)
			delete(c.evidence, key)
		}
	}
	for exec := range c.inflight {
		exec.void = true
		for key, eligibility := range exec.keys {
			if _, ok := referenced[key]; !ok || eligibility == keyUnreadPrimary {
				exec.keys[key] = keyRevoked
			} else if eligibility == keyReferenced {
				exec.void = false
			}
		}
	}
	// a failure of a key no group reads any more is attributed to nothing, also if a group
	// comes to read the key again before its attribution
	kept := c.quality.pending[:0]
	for _, p := range c.quality.pending {
		if _, ok := referenced[p.key]; ok || c.delegates[p.key] != nil {
			kept = append(kept, p)
			continue
		}
		c.recordAttributionLocked(p, Attribution{Outcome: OutcomeUndecided}, "removed")
		c.deliverAttributedLocked(p, Attribution{Outcome: OutcomeUndecided})
	}
	clearAfter(c.quality.pending, len(kept))
	c.quality.pending = kept
	c.armQualityLocked()
	// Recorded before the objects are forgotten, so that recording them does not number them
	// again.
	for _, reg := range c.registrations {
		for key, entry := range reg.needs {
			_, ok := referenced[key]
			// a group asks only for its own members, which another group may still have
			if !ok || !reg.persistent && !reg.group.References(key.Target, key.Cond) {
				c.dropNeedLocked(reg, key, entry, "removed")
			}
		}
	}
	for p := range c.objects {
		if _, ok := targets[p]; !ok {
			delete(c.objects, p)
		}
	}
	for p := range c.lastAlive {
		if _, ok := targets[p]; !ok {
			delete(c.lastAlive, p)
		}
	}
	c.pruneQualityLocked(referenced)
	c.pruneOutagesLocked(referenced)
	for key := range c.network.retained {
		if _, ok := referenced[key]; !ok {
			delete(c.network.retained, key)
		}
	}
}

// dropNeedLocked removes a need the group no longer has. While the local network seems down,
// a repair that waited its turn and is no longer asked for keeps one chance to run.
func (c *Coordinator) dropNeedLocked(reg *Registration, key Key, entry *needEntry, reason string) {
	// nothing runs for a key a group judges itself: what its own needs ask for keeps it
	repair := !entry.need.RepairUntil.IsZero() && !c.now().Before(entry.need.RepairUntil) && c.delegates[key] == nil
	if reason == "unneeded" && c.network.state == NetworkDown && repair && entry.status == needPending {
		if c.network.retained == nil {
			c.network.retained = map[Key]*Registration{}
		}
		c.network.retained[key] = reg
	}
	switch entry.status {
	case needPending:
		c.recordNeedLocked(entry, "canceled", map[string]any{"reason": reason})
	case needRunning:
		c.detachLocked(entry)
	}
	c.records.Unpin(entry.need.Config, entry.need.Exclusions)
	delete(reg.needs, key)
}

// Unregister drops a group that is closed outside a reload.
func (r *Registration) Unregister() {
	c := r.coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cancelNeedsLocked(r, "removed")
	delete(c.registrations, r.id)
	c.dropDelegatesLocked(r)
	c.forwardAllLocked()
}

// Active reports whether the registration's needs count.
func (r *Registration) Active() bool {
	c := r.coordinator
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return r.state == stateActive
}

// Evidence returns the evidence of key, Unknown when none.
func (c *Coordinator) Evidence(key Key) Evidence {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ev, ok := c.evidence[key]; ok {
		return *ev
	}
	return Evidence{Delegated: c.delegates[key] != nil}
}

func (c *Coordinator) evidenceLocked(key Key) *Evidence {
	ev, ok := c.evidence[key]
	if !ok {
		ev = &Evidence{}
		c.evidence[key] = ev
	}
	return ev
}

func (c *Coordinator) referencedLocked(key Key) bool {
	for _, reg := range c.registrations {
		if reg.group.References(key.Target, key.Cond) {
			return true
		}
	}
	return false
}

// Submit replaces the needs the group submitted before. version orders the submissions of a
// group; an older one than already accepted is ignored.
func (r *Registration) Submit(version uint64, needs []Need) {
	c := r.coordinator
	c.mutex.Lock()
	if r.state != stateActive || version <= r.version {
		c.mutex.Unlock()
		return
	}
	r.version = version
	now := c.now()
	wanted := make(map[Key]*Need, len(needs))
	for i := range needs {
		need := &needs[i]
		// a group asks for a direct member only if it tests it
		if IsBuiltin(need.Target) && need.Target.Type() != C.Direct {
			continue
		}
		// A group submitting late may ask for a member it no longer has.
		if !r.persistent && !r.group.References(need.Target, need.Cond) {
			continue
		}
		key := Key{Target: need.Target, Cond: need.Cond}
		// the census tests the paths of a group judging its own availability where they are
		if r.persistent && c.delegates[key] != nil {
			continue
		}
		wanted[key] = need
	}
	for key, entry := range r.needs {
		if need, ok := wanted[key]; ok {
			c.updateNeedLocked(entry, need, now)
			continue
		}
		c.dropNeedLocked(r, key, entry, "unneeded")
	}
	for key, need := range wanted {
		if _, ok := r.needs[key]; ok {
			continue
		}
		c.nextNeed++
		entry := &needEntry{id: c.nextNeed, need: *need, reg: r}
		r.needs[key] = entry
		c.records.Pin(need.Config, need.Exclusions)
		fields := map[string]any{
			"source": need.Source, "class": need.Class, "rank": need.Rank,
			"notBefore": formatTime(need.NotBefore), "snapshot": need.Snapshot,
		}
		if need.Search != 0 {
			fields["search"] = need.Search
		}
		if need.Batch != 0 {
			fields["batch"] = need.Batch
		}
		fields["derivation"] = version
		c.recordNeedLocked(entry, "created", fields)
		c.placeLocked(entry, now)
	}
	c.forwardAllLocked()
	c.mutex.Unlock()
	c.kick()
}

// updateNeedLocked takes a need the group submits again, maybe with another priority or
// another time its test must start after.
func (c *Coordinator) updateNeedLocked(entry *needEntry, need *Need, now time.Time) {
	moved := !entry.need.NotBefore.Equal(need.NotBefore)
	notified := entry.need.Notify
	wasBound := entry.need.bound()
	// another source or notice may let it join a test it could not before
	changed := entry.need.Source != need.Source || need.Notify && !notified
	snapshot := entry.need.Snapshot
	if entry.need.Config != need.Config || entry.need.Exclusions != need.Exclusions {
		c.records.Pin(need.Config, need.Exclusions)
		c.records.Unpin(entry.need.Config, entry.need.Exclusions)
	}
	entry.need = *need
	if need.Snapshot == nil {
		entry.need.Snapshot = snapshot
	}
	switch entry.status {
	case needRunning:
		exec, ok := c.running[entry.need.key()]
		switch {
		case ok && (moved && exec.started.Before(need.NotBefore) || exec.comparing && need.Source != SourceCompare):
			c.detachLocked(entry)
			entry.status = needPending
			c.waitLocked(entry, "running", nil)
		case ok && need.Notify && !notified:
			c.notifyBeganLocked(exec, entry)
		}
	case needPending:
		if moved || changed {
			entry.eligibleAt = time.Time{}
			entry.waiting = ""
			c.placeLocked(entry, now)
		}
	case needDone:
		// What did for the need may not any more: a later test may have found the target
		// dead, or the success it reused may have grown old. A comparison has its result
		// once, whatever the evidence says.
		// one answered by evidence held asks for a test of its own once bound
		if moved || need.bound() && !wasBound || !need.bound() && !satisfiedBy(c.evidenceLocked(entry.need.key()), &entry.need, now) {
			entry.eligibleAt = time.Time{}
			entry.waiting = ""
			c.placeLocked(entry, now)
		}
	}
}

// placeLocked decides what a new need waits for: nothing when existing evidence does, a
// running test it can join, the backoff of a dead target, or a slot.
func (c *Coordinator) placeLocked(entry *needEntry, now time.Time) {
	key := entry.need.key()
	if c.delegates[key] != nil {
		// the group judging it publishes what does
		ev := c.evidenceLocked(key)
		if satisfiedBy(ev, &entry.need, now) {
			entry.status = needDone
			c.recordNeedLocked(entry, "reused", map[string]any{"evidence": evidenceFields(ev)})
			return
		}
		entry.status = needPending
		c.waitLocked(entry, "delegated", nil)
		return
	}
	if exec, ok := c.running[key]; ok {
		// a need other than a comparison waits for a test only comparisons started
		if !exec.started.Before(entry.need.NotBefore) && exec.keys[key] == keyReferenced && (!exec.comparing || entry.need.Source == SourceCompare) {
			entry.status = needRunning
			c.attachLocked(exec, entry)
			c.notifyBeganLocked(exec, entry)
			return
		}
		entry.status = needPending
		c.waitLocked(entry, "running", nil)
		return
	}
	ev := c.evidenceLocked(key)
	if satisfiedBy(ev, &entry.need, now) {
		entry.status = needDone
		c.recordNeedLocked(entry, "reused", map[string]any{"evidence": evidenceFields(ev)})
		return
	}
	entry.status = needPending
	if ev.State == Dead && now.Before(ev.NextAllowed) {
		entry.eligibleAt = ev.NextAllowed
		c.waitLocked(entry, "backoff", map[string]any{"until": formatTime(ev.NextAllowed)})
	}
}

// satisfiedBy reports whether existing evidence does for a need: a test that started after
// what the need asks for, or for maintenance needs, a recent success.
func satisfiedBy(ev *Evidence, need *Need, now time.Time) bool {
	if ev.State == Unknown || need.bound() {
		return false
	}
	if ev.Terminal {
		// it ends at a direct outbound it does not test: nothing more to find
		return true
	}
	if !need.NotBefore.IsZero() {
		return !ev.Started.Before(need.NotBefore)
	}
	return need.Class == ClassMaintain && ev.State == Alive && now.Sub(ev.Started) < ReuseWindow
}

func (c *Coordinator) waitLocked(entry *needEntry, reason string, fields map[string]any) {
	if entry.waiting == reason {
		return
	}
	entry.waiting = reason
	if fields == nil {
		fields = map[string]any{}
	}
	fields["reason"] = reason
	c.recordNeedLocked(entry, "deferred", fields)
}

func (c *Coordinator) attachLocked(exec *execution, entry *needEntry) {
	exec.needs = append(exec.needs, entry)
	for _, request := range exec.requests {
		if request.Need == entry.id {
			return // served once already; the first source stays
		}
	}
	exec.requests = append(exec.requests, Request{Group: entry.reg.group.Name(), Source: entry.need.Source, Need: entry.id})
}

// notifyBeganLocked tells the group of a need that asked to be told that exec serves it.
func (c *Coordinator) notifyBeganLocked(exec *execution, entry *needEntry) {
	if entry.need.Notify {
		entry.reg.group.Deliver(Began{Exec: exec.id, Key: exec.key, Started: exec.started})
	}
}

func (c *Coordinator) detachLocked(entry *needEntry) {
	for _, exec := range c.running {
		for i, attached := range exec.needs {
			if attached == entry {
				exec.needs = append(exec.needs[:i], exec.needs[i+1:]...)
				return
			}
		}
	}
}

func (c *Coordinator) cancelNeedsLocked(r *Registration, reason string) {
	for key, entry := range r.needs {
		c.dropNeedLocked(r, key, entry, reason)
	}
}

func (c *Coordinator) recordNeedLocked(entry *needEntry, state string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["need"] = entry.id
	fields["state"] = state
	fields["group"] = entry.reg.group.Name()
	fields["target"] = map[string]any{"name": entry.need.Target.Name(), "object": c.objects[entry.need.Target]}
	// the versions the group derived the need under
	fields["derivedConfig"] = entry.need.Config
	fields["derivedExclusions"] = entry.need.Exclusions
	c.records.AppendAt("need", fields, entry.need.Config, entry.need.Exclusions)
}

func (c *Coordinator) viaFieldsLocked(via []C.Proxy) []any {
	fields := make([]any, 0, len(via))
	for _, p := range via {
		fields = append(fields, map[string]any{"name": p.Name(), "object": c.objectIDLocked(p)})
	}
	return fields
}

func evidenceFields(ev *Evidence) map[string]any {
	fields := map[string]any{"state": ev.State}
	if ev.Delegated {
		fields["delegated"] = true
	}
	if ev.Terminal {
		fields["terminal"] = true
	}
	if !ev.Started.IsZero() {
		fields["probedAt"] = formatTime(ev.Started)
		fields["probe"] = ev.Exec
		fields["failures"] = ev.Failures
	}
	if !ev.NextAllowed.IsZero() {
		fields["nextAllowed"] = formatTime(ev.NextAllowed)
	}
	return fields
}

func classLimit(class Class) int {
	switch class {
	case ClassVerify:
		return VerifySlots
	case ClassSearch:
		return SearchSlots
	default:
		return MaintainSlots
	}
}

// kick runs the scheduler.
func (c *Coordinator) kick() {
	c.mutex.Lock()
	started := c.scheduleLocked()
	c.mutex.Unlock()
	for _, exec := range started {
		go c.execute(exec)
	}
}

type candidate struct {
	key   Key
	class Class
	tier  int
	rank  int
	at    time.Time
	group uint64
}

// sourceTier orders the sources sharing the maintenance slots: standbys first, then
// recovering preferred members, then the census.
func sourceTier(source string) int {
	switch source {
	case SourceRecover, SourceStandby:
		// a standby comes after the verifications of its class, a recovery after maintenance
		return 1
	case SourceCensus:
		return 2
	default:
		return 0
	}
}

// scheduleLocked starts the eligible needs the free slots allow, highest class first and,
// within a class, one slot at a time in turn across groups. The recovery tests of the dead
// nodes of a pool suspected out are merged into one at a time; while the local network seems
// down, one test starts at a time, in rounds.
func (c *Coordinator) scheduleLocked() []*execution {
	c.runNetworkLocked()
	now := c.now()
	best := map[Key]*candidate{}
	var nextWake time.Time
	wake := func(t time.Time) {
		if nextWake.IsZero() || t.Before(nextWake) {
			nextWake = t
		}
	}
	if c.publishing {
		return nil
	}
	// The table changes only with this lock held, so it stays as read until tests start.
	table := c.exclusions.Current()
	var evaluator *Evaluator
	if len(table.Pools) > 0 {
		evaluator = NewEvaluatorFor(table, c.Lookup)
	}
	excluded := map[Key]bool{}
	isExcluded := func(key Key) bool {
		if evaluator == nil {
			return false
		}
		out, seen := excluded[key]
		if !seen {
			out, _ = evaluator.Excluded(key.Target)
			excluded[key] = out
		}
		return out
	}
	referenced := map[Key]bool{}
	offer := func(key Key, need *Need, at time.Time, group uint64) {
		cand := best[key]
		tier := sourceTier(need.Source)
		if cand == nil || need.Class < cand.class || (need.Class == cand.class && (tier < cand.tier || (tier == cand.tier && need.Rank < cand.rank))) {
			best[key] = &candidate{key: key, class: need.Class, tier: tier, rank: need.Rank, at: at, group: group}
		}
	}
	merged := map[*outage]map[Key]*needEntry{}
	for _, reg := range c.registrations {
		if reg.state != stateActive {
			continue
		}
		for key, entry := range reg.needs {
			if entry.status != needPending {
				continue
			}
			if _, running := c.running[key]; running {
				continue
			}
			// Evidence that came while the need waited may do for it already.
			if ev := c.evidence[key]; ev != nil && satisfiedBy(ev, &entry.need, now) {
				entry.status = needDone
				entry.eligibleAt = time.Time{}
				entry.waiting = ""
				c.recordNeedLocked(entry, "reused", map[string]any{"evidence": evidenceFields(ev)})
				continue
			}
			if c.delegates[key] != nil {
				continue // the group judging it publishes what does
			}
			// A target no group refers to any more, as one a reload dropped, is not tested.
			refers, seen := referenced[key]
			if !seen {
				refers = c.referencedLocked(key)
				referenced[key] = refers
			}
			if !refers {
				continue
			}
			// The recovery of a dead node of a pool suspected out waits for the turn the
			// outage gives it, not for its own backoff; while the local network seems down
			// the throttle takes them all in turn.
			if isMerged, o := c.mergedRecoveryLocked(key, &entry.need); isMerged && c.network.state != NetworkDown {
				if isExcluded(key) {
					c.waitLocked(entry, "excluded", nil)
					continue
				}
				if merged[o] == nil {
					merged[o] = map[Key]*needEntry{}
				}
				if prev := merged[o][key]; prev == nil || entry.need.Rank < prev.need.Rank {
					merged[o][key] = entry
				}
				entry.eligibleAt = time.Time{}
				continue
			}
			// The evidence as it is now decides the wait: a test that found the target dead
			// backs off every automatic need of it, those queued before that test ended too,
			// and a later success ends the backoff.
			if ev := c.evidence[key]; ev != nil && ev.State == Dead && now.Before(ev.NextAllowed) {
				if !entry.eligibleAt.Equal(ev.NextAllowed) {
					entry.eligibleAt = ev.NextAllowed
					entry.waiting = ""
					c.waitLocked(entry, "backoff", map[string]any{"until": formatTime(ev.NextAllowed)})
				}
			} else {
				entry.eligibleAt = time.Time{}
			}
			// A test starting now could not do for a need asking for a later one.
			if entry.need.NotBefore.After(now) && entry.eligibleAt.Before(entry.need.NotBefore) {
				entry.eligibleAt = entry.need.NotBefore
			}
			if entry.eligibleAt.After(now) {
				wake(entry.eligibleAt)
				continue
			}
			// Excluded since the group asked: the group withdraws it once it hears of the
			// change, and it must not start meanwhile.
			if isExcluded(key) {
				c.waitLocked(entry, "excluded", nil)
				continue
			}
			offer(key, &entry.need, entry.eligibleAt, reg.id)
		}
	}
	for o, entries := range merged {
		keys := make([]Key, 0, len(entries))
		for key := range entries {
			keys = append(keys, key)
		}
		if running := c.recoveringLocked(o); running != nil {
			// one recovery at a time, however long its test takes
			for _, entry := range entries {
				c.waitLocked(entry, "outage", map[string]any{"pool": o.key.pool, "running": running.id})
			}
			continue
		}
		if now.Before(o.nextAt) {
			wake(o.nextAt)
			for _, entry := range entries {
				c.waitLocked(entry, "outage", map[string]any{"pool": o.key.pool, "until": formatTime(o.nextAt)})
			}
			continue
		}
		pick := c.representativeLocked(o, keys)
		for key, entry := range entries {
			if key == pick {
				offer(key, &entry.need, now, entry.reg.id)
			} else {
				c.waitLocked(entry, "outage", map[string]any{"pool": o.key.pool, "representative": c.nodeRef(pick.Target)})
			}
		}
	}
	var started []*execution
	if c.network.state == NetworkDown {
		started = c.throttledLocked(best, isExcluded, now, wake)
	} else {
		for class := Class(0); class < classCount; class++ {
			for c.slots[class] < classLimit(class) {
				cand := c.pickLocked(best, class)
				if cand == nil {
					break
				}
				delete(best, cand.key)
				started = append(started, c.startLocked(cand.key, class, now))
			}
		}
	}
	// What is left waits for a slot, or its turn.
	reason := "slots"
	if c.network.state == NetworkDown {
		reason = "throttled"
	}
	for key := range best {
		for _, reg := range c.registrations {
			if entry, ok := reg.needs[key]; ok && reg.state == stateActive && entry.status == needPending {
				c.waitLocked(entry, reason, nil)
			}
		}
	}
	c.armTimerLocked(nextWake)
	return started
}

// throttledLocked starts at most one of the eligible tests while the local network seems
// down: each round gives every target eligible when it began one test, in the order of their
// classes; what comes meanwhile waits for the next round. A repair that waited and is no
// longer asked for keeps its one chance.
func (c *Coordinator) throttledLocked(best map[Key]*candidate, isExcluded func(Key) bool, now time.Time, wake func(time.Time)) []*execution {
	n := c.network
	for key, reg := range n.retained {
		ev := c.evidence[key]
		if _, ok := c.registrations[reg.id]; !ok || reg.state != stateActive || !reg.group.References(key.Target, key.Cond) || isExcluded(key) || ev == nil || ev.State != Dead || c.delegates[key] != nil {
			delete(n.retained, key)
			continue
		}
		if _, ok := best[key]; !ok {
			best[key] = &candidate{key: key, class: ClassMaintain, tier: sourceTier(SourceRecover), rank: 1 << 30, at: now, group: reg.id}
		}
	}
	// one automatic test at a time, those started before the network seemed down included
	if n.running != nil || c.automaticInFlightLocked() {
		return nil
	}
	if now.Before(n.nextStart) {
		wake(n.nextStart)
		return nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		if exec := c.nextOfRoundLocked(best, now, wake); exec != nil {
			return []*execution{exec}
		}
		// the round ran out on targets no longer eligible: the next one begins now
		if len(best) == 0 {
			break
		}
	}
	return nil
}

// automaticInFlightLocked reports whether an automatic test is running.
func (c *Coordinator) automaticInFlightLocked() bool {
	for exec := range c.inflight {
		// one no longer wanted still runs until it returns
		if exec.manualAPI == "" {
			return true
		}
	}
	return false
}

// nextOfRoundLocked starts the next target of the round that is still eligible, beginning a
// round of the eligible targets when the last one ran out.
func (c *Coordinator) nextOfRoundLocked(best map[Key]*candidate, now time.Time, wake func(time.Time)) *execution {
	n := c.network
	if len(n.round) == 0 {
		cands := make([]*candidate, 0, len(best))
		for _, cand := range best {
			cands = append(cands, cand)
		}
		sort.Slice(cands, func(i, j int) bool {
			a, b := cands[i], cands[j]
			if a.class != b.class {
				return a.class < b.class
			}
			if a.tier != b.tier {
				return a.tier < b.tier
			}
			if a.rank != b.rank {
				return a.rank < b.rank
			}
			return c.objectIDLocked(a.key.Target) < c.objectIDLocked(b.key.Target)
		})
		for _, cand := range cands {
			n.round = append(n.round, cand.key)
		}
	}
	for len(n.round) > 0 {
		key := n.round[0]
		n.round = n.round[1:]
		cand, ok := best[key]
		if !ok {
			continue // no longer needed, or not eligible now
		}
		delete(best, key)
		delete(n.retained, key)
		exec := c.startLocked(key, cand.class, now)
		n.running = exec
		n.nextStart = now.Add(DownInterval)
		wake(n.nextStart)
		return exec
	}
	return nil
}

func (c *Coordinator) pickLocked(best map[Key]*candidate, class Class) *candidate {
	tier := -1
	for _, cand := range best {
		if cand.class == class && (tier < 0 || cand.tier < tier) {
			tier = cand.tier
		}
	}
	byGroup := map[uint64][]*candidate{}
	var groups []uint64
	for _, cand := range best {
		if cand.class != class || cand.tier != tier {
			continue
		}
		if _, ok := byGroup[cand.group]; !ok {
			groups = append(groups, cand.group)
		}
		byGroup[cand.group] = append(byGroup[cand.group], cand)
	}
	if len(groups) == 0 {
		return nil
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	chosen := groups[0]
	for _, id := range groups {
		if id > c.rotation[class] {
			chosen = id
			break
		}
	}
	c.rotation[class] = chosen
	cands := byGroup[chosen]
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].rank != cands[j].rank {
			return cands[i].rank < cands[j].rank
		}
		if !cands[i].at.Equal(cands[j].at) {
			return cands[i].at.Before(cands[j].at)
		}
		return c.objectIDLocked(cands[i].key.Target) < c.objectIDLocked(cands[j].key.Target)
	})
	return cands[0]
}

func (c *Coordinator) newExecutionLocked(key Key, class Class, now time.Time, api string) *execution {
	c.nextExec++
	exec := &execution{id: c.nextExec, key: key, class: class, started: now, manualAPI: api}
	exec.keys = c.executionKeysLocked(key)
	exec.identity = c.identityLocked(key.Target)
	exec.config, exec.exclusions = c.records.PinCurrent()
	c.inflight[exec] = struct{}{}
	return exec
}

// identityLocked identifies a target as the usage does, with its traffic pool and object
// number; a group has neither an address nor a pool.
func (c *Coordinator) identityLocked(target C.Proxy) map[string]any {
	info := target.ProxyInfo()
	identity := map[string]any{
		"provider": info.ProviderName,
		"name":     target.Name(),
		"object":   c.objectIDLocked(target),
		"type":     target.Type().String(),
		"addr":     "",
		"pool":     "",
	}
	if _, isGroup := target.Adapter().(groupView); !isGroup {
		identity["addr"] = target.Addr()
		if owner, ok := target.(C.TrafficPoolOwner); ok {
			identity["pool"] = owner.TrafficPool()
		}
	}
	return identity
}

func (c *Coordinator) startLocked(key Key, class Class, now time.Time) *execution {
	// a recovery test: of a node dead when it starts
	ev := c.evidence[key]
	recovery := class == ClassMaintain && ev != nil && ev.State == Dead
	if pk, ok := nodePoolKey(key); ok && recovery {
		if o := c.outages.active[pk]; o != nil {
			if _, dead := o.dead[key]; dead {
				o.last = c.objectIDLocked(key.Target)
				if c.network.state != NetworkDown {
					// as a node's, the backoff of an outage does not grow while the network
					// seems down
					o.failures++
					o.nextAt = now.Add(outageBackoff(o, now))
				}
			}
		}
	}
	exec := c.newExecutionLocked(key, class, now, "")
	exec.recovery = recovery
	c.running[key] = exec
	c.slots[class]++
	for _, reg := range c.registrations {
		if reg.state != stateActive {
			continue
		}
		entry, ok := reg.needs[key]
		if !ok || entry.status != needPending || entry.eligibleAt.After(now) {
			continue
		}
		entry.status = needRunning
		entry.waiting = ""
		c.attachLocked(exec, entry)
		c.recordNeedLocked(entry, "started", map[string]any{"probe": exec.id, "snapshot": entry.need.Snapshot})
		c.notifyBeganLocked(exec, entry)
	}
	exec.comparing = exec.comparesOnly()
	return exec
}

func (c *Coordinator) armTimerLocked(at time.Time) {
	if at.IsZero() || (!c.timerAt.IsZero() && !at.Before(c.timerAt) && c.timer != nil) {
		return
	}
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timerAt = at
	c.timer = time.AfterFunc(at.Sub(c.now()), func() {
		c.mutex.Lock()
		c.timer, c.timerAt = nil, time.Time{}
		c.mutex.Unlock()
		c.kick()
	})
}

func (c *Coordinator) execute(exec *execution) {
	timeout := exec.key.Cond.Timeout
	if timeout <= 0 {
		timeout = DefaultTestTimeout
	}
	base := context.Background()
	if exec.comparing {
		base = C.WithComparison(base)
	}
	ctx, cancel := context.WithTimeout(withExecution(base, exec.id), timeout)
	ctx, recorder := statistic.WithDialerRecorder(ctx)
	fast := time.AfterFunc(FastProbeFor(timeout), func() { c.stillRunning(exec) })
	result := c.runner(ctx, exec.key)
	fast.Stop()
	cancel()
	c.finish(exec, result, statistic.DialerRoute(dialedAdapter(exec.key.Target, result), recorder))
	c.kick()
}

// stillRunning tells the groups referring to the target of a test still running at the fast
// tier of tests.
func (c *Coordinator) stillRunning(exec *execution) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.inflight[exec]; !ok || exec.keys[exec.key] != keyReferenced {
		return
	}
	notice := Running{Exec: exec.id, Key: exec.key, Started: exec.started, Requests: append([]Request(nil), exec.requests...)}
	for _, reg := range c.registrations {
		if reg.group.References(exec.key.Target, exec.key.Cond) {
			reg.group.Deliver(notice)
		}
	}
}

func backoff(failures int) time.Duration {
	return backoffUpTo(failures, BackoffMax)
}

// backoffUpTo is the backoff after a number of failures, doubling from BackoffFirst up to limit.
func backoffUpTo(failures int, limit time.Duration) time.Duration {
	delay := BackoffFirst
	for i := 1; i < failures && delay < limit; i++ {
		delay *= 2
	}
	if delay > limit {
		delay = limit
	}
	return delay
}

// adoptLocked applies a result to the evidence and returns the evidence before and after.
// While the local network seems down, a failure does not lengthen the backoff.
func (c *Coordinator) adoptLocked(exec *execution, result C.ProbeResult) (adopted bool, before, after Evidence) {
	eligibility := exec.keys[exec.key]
	ev, ok := c.evidence[exec.key]
	if !ok {
		if eligibility != keyReferenced || !c.referencedLocked(exec.key) {
			return false, Evidence{}, Evidence{}
		}
		ev = c.evidenceLocked(exec.key)
	}
	before = *ev
	// Of two tests that started at the same time, as the clock tells, the later execution wins.
	earlier := exec.started.Before(ev.Started) || (exec.started.Equal(ev.Started) && exec.id < ev.Exec)
	if eligibility == keyRevoked || result.Canceled || earlier || eligibility == keyUnreadPrimary && c.referencedLocked(exec.key) {
		return false, before, before
	}
	ev.Started = exec.started
	ev.Exec = exec.id
	c.nextVersion++
	ev.Version = c.nextVersion
	now := c.now()
	if result.Satisfied {
		ev.State = Alive
		ev.Failures = 0
		ev.NextAllowed = time.Time{}
		ev.DeadSince = time.Time{}
		ev.charged, ev.chargedExec, ev.chargedLocal = time.Time{}, 0, false
		return true, before, *ev
	}
	if ev.State != Dead {
		ev.DeadSince = exec.started
	}
	ev.State = Dead
	if c.network.state == NetworkDown {
		// the throttle decides when it runs again
		if ev.Failures == 0 {
			ev.Failures = 1
		}
		ev.NextAllowed = now
		return true, before, *ev
	}
	ev.Failures++
	limit := BackoffMax
	if c.outageLocked(ev, now) > LongOutageAfter {
		limit = BackoffMaxLong
	}
	ev.NextAllowed = now.Add(backoffUpTo(ev.Failures, limit))
	return true, before, *ev
}

func (c *Coordinator) finish(exec *execution, result C.ProbeResult, route [][]string) {
	c.mutex.Lock()
	// a network not normal that expired meanwhile ends before the result is taken in
	c.expireNetworkLocked()
	if c.running[exec.key] == exec {
		delete(c.running, exec.key)
	}
	if exec.manualAPI == "" {
		c.slots[exec.class]--
	}
	// a failure of a test only comparisons asked for is kept out of the shared state
	compared := !result.Satisfied && !result.Canceled && !exec.void && exec.comparing
	var (
		adopted       bool
		before, after Evidence
		related       []relatedResult
	)
	if compared {
		if ev := c.evidence[exec.key]; ev != nil {
			before = *ev
		}
		after = before
	} else {
		adopted, before, after = c.adoptLocked(exec, result)
		related = c.adoptRelatedLocked(exec, result)
		exec.chargedKeys = c.chargedKeysLocked(exec, adopted, before, after, related)
	}
	completion := Completion{
		Exec: exec.id, Key: exec.key, Started: exec.started, Result: result,
		Adopted: adopted, Before: before, After: after, API: exec.manualAPI,
		Comparing: exec.comparing, Compared: compared, Ended: c.now(),
	}
	if c.network.running == exec {
		c.network.running = nil
	}
	if !result.Canceled {
		// what the test went through, also when another result decides the evidence: a group
		// compares it all the same
		completion.Pools, completion.UnknownPath = c.poolsOfRoute(exec.key.Target, result, route)
	}
	if adopted && after.State == Alive && before.State == Dead && !before.DeadSince.IsZero() {
		duration := exec.started.Sub(before.DeadSince) - c.network.excused(before.DeadSince, exec.started)
		completion.Episode = &Episode{Since: before.DeadSince, Until: exec.started, Duration: duration}
	}
	needIDs := make([]uint64, 0, len(exec.requests))
	groups := map[string]struct{}{}
	for _, request := range exec.requests {
		needIDs = append(needIDs, request.Need)
		groups[request.Group] = struct{}{}
	}
	completion.Requests = exec.requests
	for _, entry := range exec.needs {
		switch {
		case exec.keys[exec.key] == keyReferenced && (compared || entry.need.bound() && !result.Canceled && !exec.started.Before(entry.need.NotBefore)):
			// a round has its result, whichever result decides the evidence
			entry.status = needDone
		case !adopted || exec.started.Before(entry.need.NotBefore):
			// the test does not answer the need: run another
			entry.status = needPending
		case entry.need.NotBefore.IsZero() && after.State == Dead:
			// a need that only asks for the target to be tested again stays, as recovering
			// a member does, and runs again once the backoff allows
			entry.status = needPending
			entry.eligibleAt = after.NextAllowed
		default:
			entry.status = needDone
		}
		entry.waiting = ""
	}
	completion.Seq = c.records.AppendAt("probe", c.probeFieldsLocked(exec, result, route, adopted, before, after, needIDs, groups, related), exec.config, exec.exclusions)
	c.records.Unpin(exec.config, exec.exclusions)
	if adopted {
		// the evidence names the event of the result that decided it
		c.evidence[exec.key].Seq = completion.Seq
		completion.After.Seq = completion.Seq
		c.adoptedLocked(exec, before, after, &completion)
	}
	if !result.Canceled && !exec.void && !compared {
		c.poolUnderLocked(exec, result)
	}
	c.finishRelatedLocked(exec, related, &completion)
	// Keep its coverage until every key has taken the result and queued any failure.
	delete(c.inflight, exec)
	for _, o := range c.outages.active {
		c.settleVerdictLocked(o)
	}
	if adopted && after.State == Dead {
		c.deferWaitingLocked(exec.key, after)
	}
	// Delivered under the lock, which only queues the completion, so that every group gets
	// the completions in the order they changed the evidence.
	for _, reg := range c.registrations {
		if reg.group.References(exec.key.Target, exec.key.Cond) {
			reg.group.Deliver(completion)
		}
	}
	// a failure of a test that ran past the attribution delay is due already, after the
	// groups heard of it
	c.attributeDueLocked()
	c.mutex.Unlock()
}

// deferWaitingLocked defers the needs of the groups waiting for a target found dead under key
// by its backoff.
func (c *Coordinator) deferWaitingLocked(key Key, after Evidence) {
	for _, reg := range c.registrations {
		if entry, ok := reg.needs[key]; ok && entry.status == needPending && entry.eligibleAt.Before(after.NextAllowed) && entry.need.NotBefore.IsZero() {
			entry.eligibleAt = after.NextAllowed
			c.waitLocked(entry, "backoff", map[string]any{"until": formatTime(after.NextAllowed)})
		}
	}
}

// adoptedLocked takes an adopted result into the memory of failures, the checks of the local
// network and the outages of pools.
func (c *Coordinator) adoptedLocked(exec *execution, before, after Evidence, completion *Completion) {
	episode := completion.Episode
	if after.State == Alive {
		if exec.started.After(c.lastAlive[exec.key.Target]) {
			c.lastAlive[exec.key.Target] = exec.started
		}
		c.noteSuccessLocked(exec.started, false)
		if episode != nil {
			c.records.appendPerformance(map[string]any{
				"event": "episode", "target": c.nodeRef(exec.key.Target), "test": exec.key.Cond.URL,
				"since": formatTime(episode.Since), "until": formatTime(episode.Until),
				"duration": episode.Duration.Milliseconds(), "excused": (episode.Until.Sub(episode.Since) - episode.Duration).Milliseconds(),
				"persistent": episode.Persistent(), "probe": exec.id,
			})
		}
		return
	}
	first, down := c.noteFailureLocked(exec.key.Target, exec.started)
	if !exec.chargedKeys[exec.key] {
		// counted once, when the evidence turned dead
		if down {
			c.demandLocked(first, exec.id, "down")
		}
		return
	}
	completion.Charged = true
	reconfirmed := before.State == Dead
	if ev := c.evidence[exec.key]; ev != nil {
		ev.charged, ev.chargedExec, ev.chargedLocal = exec.started, exec.id, false
	}
	// a failure confirmed once the local network seemed normal again is a new one for the
	// pool as for the memory of the member
	c.poolFailureLocked(exec.key, exec.started)
	c.addPendingLocked(&pendingFailure{
		key: exec.key, exec: exec.id, started: exec.started, pools: completion.Pools, unknown: completion.UnknownPath,
		commitAt: exec.started.Add(AttributionDelay), object: c.objectIDLocked(exec.key.Target), reconfirmed: reconfirmed,
	})
	// one demand for what the test tells, the network seeming down included
	reason := "failure"
	if reconfirmed {
		reason = "reconfirm"
	}
	if down {
		c.demandLocked(first, exec.id, reason, "down")
	} else {
		c.demandLocked(exec.started, exec.id, reason)
	}
}

// chargedKeysLocked decides which adopted failures count, using the same coverage for all keys.
func (c *Coordinator) chargedKeysLocked(exec *execution, adopted bool, before, after Evidence, related []relatedResult) map[Key]bool {
	var keys map[Key]bool
	add := func(key Key, before, after Evidence) {
		if after.State == Dead && (before.State != Dead || c.reconfirmsLocked(&before, exec.started)) {
			if keys == nil {
				keys = map[Key]bool{}
			}
			keys[key] = true
		}
	}
	if adopted {
		add(exec.key, before, after)
	}
	for i := range related {
		add(related[i].key, related[i].before, related[i].after)
	}
	return keys
}

// reconfirmsLocked reports whether a failure found by a test that started then, of a proxy
// dead already as ev tells, is to be attributed anew: the latest failure attributed for this
// time dead started while the local network did not seem normal, and this one does not.
func (c *Coordinator) reconfirmsLocked(ev *Evidence, started time.Time) bool {
	if ev.charged.IsZero() || !started.After(ev.charged) {
		return false
	}
	if !ev.chargedLocal && !c.network.covers(ev.charged) {
		return false
	}
	return !c.network.covers(started)
}

func (c *Coordinator) probeFieldsLocked(exec *execution, result C.ProbeResult, route [][]string, adopted bool, before, after Evidence, needs []uint64, groups map[string]struct{}, related []relatedResult) map[string]any {
	identity := exec.identity
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	fields := map[string]any{
		"id":      exec.id,
		"target":  identity,
		"test":    map[string]any{"url": exec.key.Cond.URL, "expectedStatus": exec.key.Cond.Expected, "timeout": exec.key.Cond.Timeout.Milliseconds()},
		"started": formatTime(exec.started),
		"elapsed": result.Elapsed.Milliseconds(),
		"ok":      result.Satisfied,
		"adopted": adopted,
		"before":  evidenceFields(&before),
		"after":   evidenceFields(&after),
		"needs":   needs,
		"groups":  names,
		"class":   exec.class,
		// the versions in effect when the test started
		"startConfig":     exec.config,
		"startExclusions": exec.exclusions,
	}
	if len(exec.via) > 0 {
		fields["via"] = c.viaFieldsLocked(exec.via)
	}
	if len(route) > 0 {
		fields["dialers"] = route
	}
	if result.Satisfied {
		fields["delay"] = result.Delay
	}
	if result.Status != 0 {
		fields["status"] = result.Status
	}
	if !result.Satisfied {
		fields["error"] = result.ErrorClass
		if result.Error != "" {
			fields["message"] = result.Error
		}
	}
	if result.Canceled {
		fields["canceled"] = true
	} else {
		first := map[string]any{"ok": result.FirstSatisfied}
		if result.FirstSatisfied {
			first["elapsed"] = result.FirstElapsed.Milliseconds()
		}
		fields["first"] = first
	}
	if !result.Satisfied && !result.Canceled && !exec.void && exec.comparing {
		// a failure kept out of the evidence: only comparisons asked for the test
		fields["compared"] = true
	}
	if len(related) > 0 {
		// the conditions of other timeouts the result decided as well
		fields["related"] = relatedFields(related)
	}
	if exec.manualAPI != "" {
		// a manual test asks for nothing of the groups: it is shown with those referring to it,
		// under its condition or one it decided
		names = names[:0]
		for _, reg := range c.registrations {
			if reg.persistent || reg.state != stateActive {
				continue
			}
			refers := reg.group.References(exec.key.Target, exec.key.Cond)
			for i := 0; i < len(related) && !refers; i++ {
				refers = reg.group.References(related[i].key.Target, related[i].key.Cond)
			}
			if refers {
				names = append(names, reg.group.Name())
			}
		}
		sort.Strings(names)
		object, _ := exec.identity["object"].(uint64)
		fields["manual"] = exec.manualAPI
		text := Tf("手动测试 %s：存活，%d ms", "Manual test of %s: alive, %d ms", exec.key.Target.Name(), result.Delay)
		level := LevelOK
		if !result.Satisfied {
			text = Tf("手动测试 %s：失败（%s）", "Manual test of %s: failed (%s)", exec.key.Target.Name(), result.ErrorClass)
			level = LevelWarn
		}
		fields["display"] = Display("manual", level, names, []DisplayMember{{
			Name: exec.key.Target.Name(), Provider: exec.key.Target.ProxyInfo().ProviderName, Object: object,
		}}, text)
	}
	if exec.void {
		fields["void"] = true
	}
	return fields
}

// Manual runs a test right away for an API, outside the slots and backoff, and applies its
// result as any other. It is not merged with automatic tests.
func (c *Coordinator) Manual(ctx context.Context, target C.Proxy, cond Condition, api string) C.ProbeResult {
	c.mutex.Lock()
	// a group judging its own availability is tested by the path it would confirm itself
	target, via := c.manualTargetLocked(target, cond)
	exec := c.newExecutionLocked(Key{Target: target, Cond: cond}, ClassVerify, c.now(), api)
	exec.via = via
	c.mutex.Unlock()
	timeout := cond.Timeout
	if timeout <= 0 {
		timeout = DefaultTestTimeout
	}
	ctx, cancel := context.WithTimeout(withExecution(ctx, exec.id), timeout)
	ctx, recorder := statistic.WithDialerRecorder(ctx)
	result := c.runner(ctx, exec.key)
	cancel()
	c.finish(exec, result, statistic.DialerRoute(dialedAdapter(target, result), recorder))
	c.kick()
	return result
}

// commitExclusions makes a version of the table current under this lock: no test starts in
// between, the records name it from then on, and the groups hear of the versions in order.
// The exclusion lock is taken before this one, never after.
func (c *Coordinator) commitExclusions(entries []ExclusionEntry, version uint64, publish func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.records.SetExclusions(version, map[string]any{"pools": entries}, true)
	publish()
	table := c.exclusions.Current()
	for _, reg := range c.registrations {
		reg.group.Deliver(ExclusionsChanged{Table: table})
	}
}

// exclusionsChanged acts on a change of the table once it is current.
func (c *Coordinator) exclusionsChanged(table Table) {
	go c.census.tick()
	c.kick()
	log.Debugln("[Probe] exclusions changed: %d pools", len(table.Pools))
}
