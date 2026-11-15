package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

// ProbeOnDemand is the probe option of a fallback group that tests its members on demand.
const ProbeOnDemand = "on-demand"

// Why the selection of a group changed.
const (
	causeVerify     = "verify"    // a verification found the current member dead
	causeContinue   = "continue"  // the verification after a switch found the new member dead
	causeSearch     = "search"    // a test of the search found the current member dead
	causeExternal   = "external"  // another group's test, or a manual one, found it dead
	causeRecover    = "recover"   // a preferred member came back
	causeExclusion  = "exclusion" // the exclusions changed
	causePin        = "pin"       // the pinned member changed
	causeReload     = "reload"
	causeTopology   = "topology" // a provider changed the members
	causeProbe      = "probe"    // another test result
	causeSuspect    = "suspect"  // the preferred member was set aside for a while
	causeSuspectEnd = "suspectEnd"
	causeDemoted    = "demoted"  // a member yields after repeated anomalies
	causeTrial      = "trial"    // a member is put on trial
	causeRestored   = "restored" // a member passed its trial
	causeDelegate   = "delegate" // a member judging its own availability found nothing carries it
	causePolicy     = "policy"   // a member judging its own availability was pinned off, excluded inside or lost its members
)

var errNoAvailable = errors.New("no available member")

// rejectProxy stands for the selection of a group none of whose members is available.
var rejectProxy = adapter.NewProxy(outbound.NewReject())

// searchIDs numbers the searches of this process.
var searchIDs atomic.Uint64

// generations numbers the on-demand groups of this process.
var generations atomic.Uint64

type selectionCause struct {
	Reason string `json:"reason"`
	Seq    uint64 `json:"seq,omitempty"`
	Exec   uint64 `json:"probe,omitempty"`
	Group  string `json:"sourceGroup,omitempty"`
	API    string `json:"sourceApi,omitempty"`
}

// memberState is what a group keeps of one member.
type memberState struct {
	lastSuccessDial time.Time
	cooldown        time.Duration
	cooldownUntil   time.Time
	// cooldownExec is the test that last lengthened the cooldown: the signals one test
	// explains, in whatever rounds they come, count as one false alarm.
	cooldownExec uint64
	lastSignal   time.Time
	// signals are the times of the signals of the current member no test explained yet; the
	// earliest is the obligation a verification must start after.
	signals        []time.Time
	obligationDial time.Time // the latest dial time of the connections of those signals
	udpSilent      []udpSilentEntry
	cooldownReset  time.Time // when the cooldown was last reset
	jitter         float64
	jitterVersion  uint64
	excluded       bool
	trace          []probe.TraceStep
	lastDelay      uint16 // of the latest test that found it alive
}

type searchState struct {
	id      uint64
	t0      time.Time
	cause   uint64
	batch   uint64
	members []C.Proxy // of the current batch
	scope   []C.Proxy // last recorded
}

type refillState struct {
	batch   uint64
	members []C.Proxy
}

// snapshot is what the dial path reads without locks.
type snapshot struct {
	states   map[C.Proxy]probe.State
	excluded map[C.Proxy]bool
	cause    selectionCause
	// objects numbers the members, so that the dial path can tell which one it used
	// without keeping it.
	objects map[C.Proxy]uint64
	// order is the effective order under pinned; the preferred member is suspended while
	// suspended is set, and the route is certain until certainUntil.
	order        []C.Proxy
	pinned       string
	suspended    C.Proxy
	route        C.Proxy
	trusted      map[C.Proxy]bool
	certainUntil time.Time
	// leafBuiltin marks the members ending at a built-in outbound by the selection of the
	// groups along them, as of the round
	leafBuiltin map[C.Proxy]bool
	// probed marks the direct members the group tests and watches as it does its other ones
	probed map[C.Proxy]bool
	// terminal marks the members judging their own availability that end at a direct
	// outbound they do not test; certainChild is the group such a route is as certain as
	// when dialed
	terminal     map[C.Proxy]bool
	certainChild *onDemand
}

// dialedMember is the member a real dial went through, by name and object number: it keeps
// no reference to the member, which a reload may replace.
type dialedMember struct {
	name     string
	object   uint64
	provider string
}

// carriedMember is the member a real connection was last handed out through, and whether the
// fast tier called on it rather than the route.
type carriedMember struct {
	dialedMember
	hedged bool
}

// onDemand is the strategy of a fallback group with probe: on-demand. Every change of its
// state goes through its inbox and is handled in order by one goroutine.
type onDemand struct {
	f     *Fallback
	cond  probe.Condition
	coord *probe.Coordinator
	// strict keeps the members in their order: no dial races another member, no member is
	// set aside, yields or goes on trial.
	strict bool

	// self is the proxy of the group in the config, under which it publishes its availability
	// while delegating is set; the groups listing it read that as its evidence. published is
	// what it published last, forwarded what those groups need of it now. They are kept by
	// the rounds, as is whether it may delegate, by the topology it was decided for.
	self           C.Proxy
	delegating     bool
	published      atomic.Pointer[publication]
	forwarded      []probe.Forward
	delegable      bool
	delegableAt    uint64
	delegableKnown bool

	reg        *probe.Registration
	generation uint64 // tells the groups of successive configs apart
	started    atomic.Bool
	done       chan struct{} // closed to stop handling the inbox
	stopped    chan struct{} // closed once the inbox is no longer handled
	stop       sync.Once

	inboxMutex sync.Mutex
	inbox      []any
	wake       chan struct{}
	// sealed is set once a retired group took the last of its inbox: what its connections
	// still observe goes to forward, the group that replaced it, if any.
	sealed  bool
	forward *onDemand

	memberSet atomic.Pointer[map[C.Proxy]struct{}]
	// memberSetVersion is the topology version memberSet was read at: a set read earlier
	// does not replace it.
	memberSetMutex   sync.Mutex
	memberSetVersion uint64
	snap             atomic.Pointer[snapshot]
	// pin is the pinned member as set, read by the dial path; the strategy takes it over
	// from the message setting it.
	pin      atomic.Pointer[string]
	pinMutex sync.Mutex // orders the pins as set with the messages telling the strategy
	// forwardTo is the group that replaced this one once it is retired: use it observes
	// goes there.
	forwardTo atomic.Pointer[onDemand]

	lastDial   atomic.Int64 // unix nanoseconds of real dials and received data
	lastData   atomic.Int64
	lastActive atomic.Int64
	dialed     atomic.Pointer[dialedMember]

	// strategy lock: a leaf lock, never held while calling other groups or the network
	mutex        sync.Mutex
	members      []C.Proxy // in config order
	order        []C.Proxy // effective order
	view         map[C.Proxy]probe.Evidence
	states       map[C.Proxy]*memberState
	pinned       string
	current      C.Proxy
	currentSince time.Time
	cause        selectionCause
	continueAt   time.Time
	lastSwitch   time.Time
	repairUntil  time.Time
	inUse        bool
	search       *searchState
	refill       *refillState
	pool         []C.Proxy
	version      uint64
	timer        *time.Timer
	timerAt      time.Time
	evaluated    bool // the last round evaluated the exclusions
	// table is the version of the exclusion table the group heard of last; it is evaluated
	// against until the next notice.
	// activated is set once the config the group belongs to is activated and the group took
	// over the state of the group it replaces: only then does it ask for tests.
	activated bool
	closed    bool
	table     probe.Table
	// roundCurrent is the selection at the start of the round being handled.
	roundCurrent C.Proxy

	// route is where new dials start: the current member, or while it is suspended the member
	// after it.
	route          C.Proxy
	suspended      *suspendState
	suspendCheck   time.Time
	verifyRun      *verifyRun
	continueMember C.Proxy // the member continueAt asks to verify, the current one when nil
	network        probe.NetworkState
	outagePools    map[string]bool
	memberPools    map[C.Proxy][]string
	quality        map[C.Proxy]*memberQuality
	pairRuns       map[uint64]*pairRun
	degradations   []*degradation
	summaries      map[summaryKey]*summary
	summarized     int64 // the last minute summed up
	// attributed keeps the attributions of failures heard lately, for the trials a group
	// replacing this one takes over; attributedAt is when each was heard of
	attributed   map[uint64]probe.Attributed
	attributedAt map[uint64]time.Time
	leafBuiltin  map[C.Proxy]bool
	// probed marks the direct members tested and watched as the others are
	probed  map[C.Proxy]bool
	carried atomic.Pointer[carriedMember]
	// early are the tests and new connections heard of before the group took over from the
	// one it replaces, for the trials it inherits
	early          []probe.Completion
	earlySuccesses []successNote
	// routeCause is why the route last moved, which the dials it carries record
	routeCause selectionCause
	// standbys are the members a derivation tested alongside the verification of the
	// preferred one, whatever else their needs were for, with since when: a test of one
	// started since is paired as a standby
	standbys map[C.Proxy]time.Time

	// Guarded by the bindings lock: a retired group hands what its connections observe to
	// its successor, the group of the same name that replaced it, if any.
	retired   bool
	successor *onDemand
}

func newOnDemand(f *Fallback, url, expected string, timeout int, strict bool) *onDemand {
	od := &onDemand{
		f:           f,
		strict:      strict,
		generation:  generations.Add(1),
		cond:        probe.Condition{URL: url, Expected: normalizeExpected(expected), Timeout: time.Duration(timeout) * time.Millisecond},
		coord:       probe.Default,
		done:        make(chan struct{}),
		stopped:     make(chan struct{}),
		wake:        make(chan struct{}, 1),
		view:        map[C.Proxy]probe.Evidence{},
		states:      map[C.Proxy]*memberState{},
		outagePools: map[string]bool{},
		memberPools: map[C.Proxy][]string{},
		quality:     map[C.Proxy]*memberQuality{},
		pairRuns:    map[uint64]*pairRun{},
		summaries:   map[summaryKey]*summary{},
		attributed:  map[uint64]probe.Attributed{},
		leafBuiltin: map[C.Proxy]bool{},
		probed:      map[C.Proxy]bool{},
	}
	empty := map[C.Proxy]struct{}{}
	od.memberSet.Store(&empty)
	od.snap.Store(&snapshot{})
	return od
}

func (od *onDemand) name() string {
	return od.f.Name()
}

// unprobed reports whether a member is a built-in outbound the group neither tests nor
// watches, always available: any but a direct member some member not built in comes after,
// which the group tests as it does its other members.
func (od *onDemand) unprobed(p C.Proxy) bool {
	return probe.IsBuiltin(p) && !od.probed[p] || od.view[p].Terminal
}

// watched reports whether the dial path observes a member, as the latest round has it.
func (od *onDemand) watched(p C.Proxy) bool {
	return !probe.IsBuiltin(p) || od.snap.Load().probed[p]
}

// now is the time of the coordinator, which the group's state is kept by.
func (od *onDemand) now() time.Time {
	return od.coord.Now()
}

// Name implements probe.Group.
func (od *onDemand) Name() string {
	return od.name()
}

// References implements probe.Group.
func (od *onDemand) References(target C.Proxy, cond probe.Condition) bool {
	if cond != od.cond {
		return false
	}
	_, ok := (*od.memberSet.Load())[target]
	return ok
}

// Conditions implements probe.Conditioned.
func (od *onDemand) Conditions() []probe.Condition {
	return []probe.Condition{od.cond}
}

// Deliver implements probe.Group.
func (od *onDemand) Deliver(notice probe.Notice) {
	od.post(notice)
}

func (od *onDemand) post(msg any) {
	od.inboxMutex.Lock()
	if od.sealed {
		forward := od.forward
		od.inboxMutex.Unlock()
		if forward != nil && observed(msg) {
			forward.post(msg)
		}
		return
	}
	od.inbox = append(od.inbox, msg)
	od.inboxMutex.Unlock()
	select {
	case od.wake <- struct{}{}:
	default:
	}
}

// publish registers the group and starts handling its inbox; its needs count once the
// config is activated. It returns once the group routes by what the coordinator knows of its
// members, as it may be dialed through right away.
func (od *onDemand) publish() {
	if !od.started.CompareAndSwap(false, true) {
		return
	}
	od.reg = od.coord.Register(od)
	od.table = od.coord.Exclusions().Current()
	od.network, _ = od.coord.Network()
	for _, o := range od.coord.Outages() {
		if o.Cond == od.cond {
			od.outagePools[o.Pool] = true
		}
	}
	od.round(nil, false)
	go od.run()
}

// topologyVersion numbers the sets of members the groups were given, by a config or a
// provider update.
var topologyVersion atomic.Uint64

// observed reports whether a message tells what connections observed, which the group that
// replaced a retired one takes over.
func observed(msg any) bool {
	switch msg.(type) {
	case signalNote, successNote, udpSilentNote, useNote, probeDial, switchNote, dialNote, hedgeNote:
		return true
	}
	return false
}

// seal takes the last of the inbox of a retired group; what comes after goes to successor.
func (od *onDemand) seal(successor *onDemand) []any {
	od.inboxMutex.Lock()
	defer od.inboxMutex.Unlock()
	msgs := od.inbox
	od.inbox = nil
	od.sealed = true
	od.forward = successor
	od.forwardTo.Store(successor)
	return msgs
}

// close stops the group for good; what is left in its inbox is dropped.
func (od *onDemand) close() {
	od.halt()
	od.shut()
}

// retire stops the group replaced by a new config: it handles what its inbox still holds,
// which the coordinator no longer adds to, and hands its state to its successor, nil when the
// new config has no group of its name.
func (od *onDemand) retire(successor *onDemand) inheritedState {
	od.halt()
	if msgs := od.seal(successor); len(msgs) > 0 {
		od.round(msgs, false)
	}
	od.mutex.Lock()
	if successor == nil {
		for p := range od.states {
			od.revokeObligation(p, "groupRemoved")
		}
		od.flushSummaries(od.now(), true)
	} else {
		od.flushLeaving(successor)
	}
	od.mutex.Unlock()
	state := od.export()
	od.shut()
	return state
}

// halt stops handling the inbox, and waits until no round is running.
func (od *onDemand) halt() {
	od.stop.Do(func() { close(od.done) })
	if od.started.Load() {
		<-od.stopped
	}
}

func (od *onDemand) shut() {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	od.closed = true
	if od.timer != nil {
		od.timer.Stop()
		od.timer = nil
	}
}

func (od *onDemand) run() {
	defer close(od.stopped)
	for {
		select {
		case <-od.wake:
		case <-od.done:
			return
		}
		od.process()
	}
}

// Messages of the inbox, besides the notices of the coordinator.
type (
	recheck struct{}
	useNote struct{ reason string }
	// pinNote tells the pin as set, by whom: "user" through the API, "restore" from the
	// selection a config kept
	pinNote    struct{ name, source string }
	probeDial  struct{ exec uint64 }
	switchNote struct {
		from, to dialedMember
		cause    selectionCause
	}
	signalNote struct {
		member       C.Proxy
		cond         probe.Condition
		network      string
		strong       bool
		class        string
		err          string
		at           time.Time
		dialed       time.Time
		age          time.Duration
		sent         bool
		probe        bool
		beforeReload bool
		count        int
		// hedged is set for a member the fast tier called on: its failures form an
		// obligation while it stays among the standbys.
		hedged bool
	}
	successNote struct {
		member C.Proxy
		cond   probe.Condition
		dialed time.Time
		probe  bool
	}
	activateNote struct{}
	// topologyNote tells the group of a provider update, with the members it has after it.
	topologyNote struct{ members []C.Proxy }
)

func (od *onDemand) drain() []any {
	od.inboxMutex.Lock()
	defer od.inboxMutex.Unlock()
	msgs := od.inbox
	od.inbox = nil
	return msgs
}

type evaluation struct {
	excluded bool
	trace    []probe.TraceStep
}

func (od *onDemand) process() {
	if msgs := od.drain(); len(msgs) > 0 {
		od.round(msgs, true)
	}
}

// round handles messages of the inbox in order, then derives what the group needs and, with
// submit, asks the coordinator for it. The members and the exclusions read for the round take
// effect at the first message that tells of a change of them, or after the messages: a
// message that came before the change is handled as things were before it.
func (od *onDemand) round(msgs []any, submit bool) {
	version := topologyVersion.Load()
	members := od.f.GetProxies(false)
	od.updateMemberSet(members, version)
	// Evaluated outside the strategy lock, as evaluating reads other groups: the table the
	// group knows, for the members that change without notice, and the table of each notice.
	od.mutex.Lock()
	known := od.table
	candidates := slices.Clone(od.members)
	od.mutex.Unlock()
	candidates = append(candidates, members...)
	for _, msg := range msgs {
		if note, ok := msg.(topologyNote); ok {
			candidates = append(candidates, note.members...)
		}
	}
	evals := map[uint64]map[C.Proxy]evaluation{known.Version: od.evaluate(known, candidates)}
	// The pools the members go through, read outside the strategy lock too.
	pools := make(map[C.Proxy][]string, len(candidates))
	leaves := make(map[C.Proxy]bool, len(candidates))
	for _, p := range candidates {
		if _, done := pools[p]; !done && !probe.IsBuiltin(p) {
			// a member judging its own availability handles the outages along its paths
			if !od.coord.Delegated(probe.Key{Target: p, Cond: od.cond}) {
				pools[p] = od.coord.PoolsOf(p)
			} else {
				pools[p] = nil
			}
			leaves[p] = leafBuiltin(p, nil)
		}
	}
	if od.self == nil {
		if p := od.coord.Lookup(od.name()); p != nil && p.Adapter() == od.f {
			od.self = p
		}
	}
	if !od.delegableKnown || od.delegableAt != version {
		od.delegable, od.delegableAt, od.delegableKnown = od.isDelegable(), version, true
	}
	for _, msg := range msgs {
		if notice, ok := msg.(probe.ExclusionsChanged); ok {
			evals[notice.Table.Version] = od.evaluate(notice.Table, candidates)
		}
	}

	od.mutex.Lock()
	if od.closed {
		od.mutex.Unlock()
		return
	}
	now := od.now()
	od.memberPools = pools
	od.leafBuiltin = leaves
	od.roundCurrent = od.current
	defer func() { od.roundCurrent = nil }()
	setMembers := func(list []C.Proxy) {
		first := od.members == nil
		if od.applyMembers(list) {
			od.dropBindings()
			od.applyEligibility(evals[od.table.Version])
			od.reselect(now, selectionCause{Reason: causeTopology})
		}
		if first {
			od.applyEligibility(evals[od.table.Version])
			od.reselect(now, selectionCause{Reason: causeReload})
		}
	}
	if od.members == nil {
		setMembers(members)
	}
	// Each change takes effect where its message stands: a message that came before it is
	// handled as things were before it. What changes without a message, the selections of
	// the groups along a member, takes effect after the messages.
	for _, msg := range msgs {
		switch msg := msg.(type) {
		case probe.ExclusionsChanged:
			if msg.Table.Version > od.table.Version {
				od.table = msg.Table
				if od.applyEligibility(evals[msg.Table.Version]) {
					od.reselect(now, selectionCause{Reason: causeExclusion})
				}
			}
		case topologyNote:
			setMembers(msg.members)
		case activateNote, inheritNote:
			setMembers(members)
		}
		od.handle(msg, now)
	}
	if od.applyEligibility(evals[od.table.Version]) {
		od.reselect(now, selectionCause{Reason: causeExclusion})
	}
	od.update(now)
	change, next := od.delegationLocked(od.delegable)
	needs := od.deriveLocked(now)
	od.version++
	submitted := od.version
	ready := submit && od.activated
	od.publishSnapshot()
	od.armTimer(now)
	od.mutex.Unlock()

	if od.reg != nil && od.self != nil {
		key := probe.Key{Target: od.self, Cond: od.cond}
		if change != nil {
			od.reg.Delegate(key, *change)
		}
		if next != nil {
			od.reg.Publish(key, *next)
		}
	}
	if ready && od.reg != nil {
		od.reg.Submit(submitted, needs)
	}
	for _, msg := range msgs {
		if note, ok := msg.(settleNote); ok {
			close(note.done)
		}
	}
}

// evaluate finds which of members a version of the exclusion table excludes; nil when it is
// empty, which excludes none.
func (od *onDemand) evaluate(table probe.Table, members []C.Proxy) map[C.Proxy]evaluation {
	if len(table.Pools) == 0 {
		return nil
	}
	evaluator := probe.NewEvaluatorFor(table, od.coord.Lookup)
	evals := make(map[C.Proxy]evaluation, len(members))
	for _, p := range members {
		if _, done := evals[p]; done || probe.IsBuiltin(p) {
			continue
		}
		excluded, trace := evaluator.Excluded(p)
		evals[p] = evaluation{excluded: excluded, trace: trace}
	}
	return evals
}

// updateMemberSet publishes the members the coordinator delivers completions for, read at a
// topology version; a set read at an earlier version than the one published is ignored. It
// returns the members new to it.
func (od *onDemand) updateMemberSet(members []C.Proxy, version uint64) []C.Proxy {
	od.memberSetMutex.Lock()
	defer od.memberSetMutex.Unlock()
	if version < od.memberSetVersion {
		return nil
	}
	od.memberSetVersion = version
	old := *od.memberSet.Load()
	var added []C.Proxy
	set := make(map[C.Proxy]struct{}, len(members))
	for _, p := range members {
		set[p] = struct{}{}
		if _, ok := old[p]; !ok {
			added = append(added, p)
		}
	}
	if len(added) > 0 || len(set) != len(old) {
		od.memberSet.Store(&set)
	}
	return added
}

// applyMembers takes the members of a round and reports whether they changed since the last
// round; state of members that left is dropped.
func (od *onDemand) applyMembers(members []C.Proxy) bool {
	changed := !slices.Equal(members, od.members) && od.members != nil
	od.members = slices.Clone(members)
	od.probed = probe.ProbedDirects(od.members)
	set := make(map[C.Proxy]struct{}, len(members))
	for _, p := range members {
		set[p] = struct{}{}
	}
	for p := range od.view {
		if _, ok := set[p]; !ok {
			delete(od.view, p)
		}
	}
	for p := range od.states {
		if _, ok := set[p]; !ok {
			od.revokeObligation(p, "memberRemoved")
			delete(od.states, p)
		}
	}
	for p := range od.quality {
		if _, ok := set[p]; !ok {
			delete(od.quality, p)
		}
	}
	for exec, run := range od.pairRuns {
		if _, ok := set[run.member]; !ok {
			delete(od.pairRuns, exec)
		}
	}
	keptDegradations := od.degradations[:0]
	for _, d := range od.degradations {
		if _, ok := set[d.member]; ok {
			keptDegradations = append(keptDegradations, d)
		}
	}
	for i := len(keptDegradations); i < len(od.degradations); i++ {
		od.degradations[i] = nil
	}
	od.degradations = keptDegradations
	for exec, a := range od.attributed {
		if _, ok := set[a.Key.Target]; !ok {
			delete(od.attributed, exec)
			delete(od.attributedAt, exec)
		}
	}
	// Evidence of members new to the group, read after they joined the set the coordinator
	// delivers completions for, so that no completion falls between.
	for _, p := range members {
		if _, ok := od.view[p]; !ok {
			od.view[p] = od.coord.Evidence(probe.Key{Target: p, Cond: od.cond})
		}
	}
	od.computeOrder()
	return changed
}

func (od *onDemand) computeOrder() {
	order := make([]C.Proxy, 0, len(od.members))
	if od.pinned != "" {
		for _, p := range od.members {
			if p.Name() == od.pinned {
				order = append(order, p)
				break
			}
		}
	}
	// Members yielding come after the others before the first built-in member, in their
	// order; a pinned member keeps its place.
	var yielding []C.Proxy
	builtin := false
	for _, p := range od.members {
		if len(order) > 0 && order[0] == p && od.pinned != "" {
			continue
		}
		if !builtin && od.unprobed(p) {
			builtin = true
			order = append(order, yielding...)
			yielding = nil
		}
		if !builtin && od.standingOf(p) == standingYield {
			yielding = append(yielding, p)
			continue
		}
		order = append(order, p)
	}
	od.order = append(order, yielding...)
}

func (od *onDemand) member(p C.Proxy) *memberState {
	ms := od.states[p]
	if ms == nil {
		ms = &memberState{}
		od.states[p] = ms
	}
	return ms
}

func (od *onDemand) isMember(p C.Proxy) bool {
	return slices.Contains(od.members, p)
}

// applyEligibility takes the exclusion evaluation of a round, records the members whose
// eligibility changed, and reports whether any did.
func (od *onDemand) applyEligibility(evals map[C.Proxy]evaluation) bool {
	if evals == nil && !od.evaluated {
		return false
	}
	od.evaluated = evals != nil
	changed := false
	for _, p := range od.members {
		if probe.IsBuiltin(p) {
			continue
		}
		ms := od.member(p)
		eval := evals[p]
		if ms.excluded == eval.excluded {
			ms.trace = eval.trace
			continue
		}
		ms.excluded = eval.excluded
		ms.trace = eval.trace
		changed = true
		fields := od.event("eligibility")
		fields["member"] = od.ref(p)
		fields["excluded"] = eval.excluded
		fields["trace"] = eval.trace
		od.record(fields)
		if eval.excluded {
			od.revokeObligation(p, "excluded")
		}
	}
	return changed
}

func (od *onDemand) excluded(p C.Proxy) bool {
	if probe.IsBuiltin(p) {
		return false
	}
	if ms := od.states[p]; ms != nil {
		return ms.excluded
	}
	return false
}

func (od *onDemand) available(p C.Proxy) bool {
	if od.unprobed(p) {
		return true
	}
	return !od.excluded(p) && od.view[p].State != probe.Dead
}

func (od *onDemand) selectLocked() C.Proxy {
	for _, p := range od.order {
		if od.available(p) {
			return p
		}
	}
	return nil
}

// reselect moves the selection to the first available member in the effective order.
func (od *onDemand) reselect(now time.Time, cause selectionCause) {
	next := od.selectLocked()
	if next == od.current {
		return
	}
	prev := od.current
	od.current = next
	od.currentSince = now
	od.cause = cause
	od.routeCause = cause
	if prev != nil {
		od.lastSwitch = now
		od.revokeObligation(prev, "currentChanged")
	}
	od.continueAt, od.continueMember = time.Time{}, nil
	if next != nil && !od.unprobed(next) && !od.fresh(next, now) {
		switch cause.Reason {
		case causeVerify, causeContinue, causeSearch, causeExternal, causeDelegate:
			// a failure: the search it started confirms the new member, unless there is none
			if od.search == nil {
				od.continueAt = now
			}
		case causeExclusion, causePin, causeTopology, causeDemoted, causeTrial, causeRestored, causePolicy:
			od.continueAt = now
		}
	}
	fields := od.event("selection")
	fields["from"] = od.ref(prev)
	fields["to"] = od.ref(next)
	fields["cause"] = cause
	fields["display"] = od.selectionDisplay(prev, next, cause)
	od.record(fields)
	if s := od.suspended; s != nil && s.member != next {
		od.suspended = nil
		fields := od.event("suspendEnd")
		fields["member"] = od.ref(s.member)
		fields["reason"] = "notPreferred"
		od.record(fields)
	}
	od.route = nil
	od.setRoute(now, cause)
}

func (od *onDemand) freshness(now time.Time) time.Duration {
	if !od.lastSwitch.IsZero() && now.Sub(od.lastSwitch) < probe.SwitchedWindow {
		return probe.PoolFreshSwitched
	}
	return probe.PoolFresh
}

// fresh reports whether p is alive by a test recent enough to stand for a standby.
func (od *onDemand) fresh(p C.Proxy, now time.Time) bool {
	ev := od.view[p]
	return ev.State == probe.Alive && now.Sub(ev.Started) <= od.refreshAfter(p, now)
}

// refreshAfter is how long a success of p stands, with the jitter of p drawn for the test
// that decided it.
func (od *onDemand) refreshAfter(p C.Proxy, now time.Time) time.Duration {
	ms := od.member(p)
	ev := od.view[p]
	if ms.jitterVersion != ev.Version {
		ms.jitterVersion = ev.Version
		ms.jitter = (rand.Float64()*2 - 1) * probe.PoolJitter
	}
	return time.Duration(float64(od.freshness(now)) * (1 + ms.jitter))
}

func (od *onDemand) handle(msg any, now time.Time) {
	switch msg := msg.(type) {
	case probe.Completion:
		od.onCompletion(msg, now)
	case probe.ExclusionsChanged, topologyNote:
		// taken into effect by the round
	case probe.Forwarded:
		od.forwarded = msg.Needs
	case settleNote:
		// answered once the round is done
	case probe.Running:
		od.onRunning(msg, now)
	case probe.Attributed:
		od.onAttributed(msg, now)
	case probe.NetworkChanged:
		od.settleTrials(msg.Since)
		od.network = msg.State
		od.settleTrials(msg.Since)
		od.refreshDead()
	case probe.OutageChanged:
		od.onOutageChanged(msg, now)
		od.refreshDead()
	case hedgeNote:
		od.onHedge(msg, now)
	case dialNote:
		od.onDial(msg, now)
	case signalNote:
		od.onSignal(msg, now)
	case successNote:
		od.onSuccess(msg, now)
	case udpSilentNote:
		od.onUDPSilent(msg, now)
	case useNote:
		if !od.inUse {
			od.inUse = true
			fields := od.event("use")
			fields["inUse"] = true
			fields["reason"] = msg.reason
			od.record(fields)
			od.firstUse(now)
		}
	case pinNote:
		if msg.name != od.pinned {
			fields := od.event("pin")
			fields["from"] = od.pinned
			fields["to"] = msg.name
			fields["source"] = msg.source
			fields["display"] = od.pinDisplay(od.pinned, msg.name, msg.source)
			od.record(fields)
			od.pinned = msg.name
			od.computeOrder()
			od.reselect(now, selectionCause{Reason: causePin})
		}
	case probeDial:
		od.onProbeDial(msg, now)
	case switchNote:
		od.onSwitch(msg, now)
	case inheritNote:
		od.inherit(msg.state, now)
	case activateNote:
		od.activated = true
		od.early, od.earlySuccesses = nil, nil
	case recheck:
	}
}

func (od *onDemand) requestOf(c probe.Completion) (probe.Request, bool) {
	for _, request := range c.Requests {
		if request.Group == od.name() {
			return request, true
		}
	}
	return probe.Request{}, false
}

func (od *onDemand) causeOf(c probe.Completion) selectionCause {
	cause := selectionCause{Seq: c.Seq, Exec: c.Exec, API: c.API}
	if c.After.Delegated && c.After.State == probe.Dead {
		cause.Reason = causeDelegate
		if c.Policy {
			cause.Reason = causePolicy
		}
		return cause
	}
	if request, ok := od.requestOf(c); ok {
		cause.Group = request.Group
		switch request.Source {
		case probe.SourceVerify:
			cause.Reason = causeVerify
		case probe.SourceContinue:
			cause.Reason = causeContinue
		case probe.SourceSearch:
			cause.Reason = causeSearch
		default:
			cause.Reason = causeProbe
		}
		return cause
	}
	cause.Reason = causeExternal
	if len(c.Requests) > 0 {
		cause.Group = c.Requests[0].Group
	}
	return cause
}

// refreshDead reads again the backoffs of the dead members: the coordinator clears them when
// the network or an outage of their pool comes back, which no test tells. A newer evidence is
// left to the completion that brings it, which tells how it changed.
func (od *onDemand) refreshDead() {
	for p, ev := range od.view {
		if ev.State != probe.Dead {
			continue
		}
		if fresh := od.coord.Evidence(probe.Key{Target: p, Cond: od.cond}); fresh.Version == ev.Version && fresh.State == probe.Dead {
			ev.Failures, ev.NextAllowed = fresh.Failures, fresh.NextAllowed
			od.view[p] = ev
		}
	}
}

func (od *onDemand) onCompletion(c probe.Completion, now time.Time) {
	m := c.Key.Target
	if c.Key.Cond != od.cond || !od.isMember(m) {
		return
	}
	prev := od.view[m]
	wasCurrent := m == od.current
	if !od.activated {
		od.early = append(od.early, c)
	}
	if c.Adopted && c.After.Version > prev.Version {
		after := c.After
		if after.State == probe.Dead {
			// its backoff may have been cleared since, as an outage it ended came back
			if fresh := od.coord.Evidence(c.Key); fresh.Version == after.Version && fresh.State == probe.Dead {
				after.Failures, after.NextAllowed = fresh.Failures, fresh.NextAllowed
			}
		}
		od.view[m] = after
	}
	after := od.view[m]
	if after.Terminal != prev.Terminal {
		od.computeOrder() // the member is a bound of the order now, or no longer
	}
	ms := od.member(m)
	// A test explains the signals that came before it started; signals within the same tick
	// of the clock count as before, as the coordinator counts them.
	if len(ms.signals) > 0 && c.Adopted && !c.Started.Before(ms.signals[0]) {
		od.satisfyObligation(m, c.Started, c.Exec, c.Seq, c.Result.Satisfied, now)
	}
	od.trialCompletion(c)
	if c.Adopted && c.Result.Satisfied {
		ms.lastDelay = c.Result.Delay
	}
	if c.Episode.Persistent() {
		od.giveTrialReason(m, trialPersistent)
	}
	if request, ok := od.requestOf(c); ok && !c.Result.Canceled {
		switch {
		case request.Source == probe.SourceStandby || (od.askedAsStandby(m, c.Started) && m != od.current && !wasCurrent):
			delete(od.standbys, m)
			od.notePairRun(pairRunFrom(c, true), now)
		case (request.Source == probe.SourceVerify || request.Source == probe.SourceContinue) && (wasCurrent || m == od.roundCurrent):
			od.notePairRun(pairRunFrom(c, false), now)
		case od.pairRuns[c.Exec] != nil:
			// a test noted while it ran, for a member that is no longer preferred
			od.notePairRun(pairRunFrom(c, od.pairRuns[c.Exec].standby), now)
		}
	} else if run := od.pairRuns[c.Exec]; run != nil {
		delete(od.pairRuns, c.Exec)
	}
	od.suspendCompletion(c, now)
	failed := c.Adopted && prev.State != probe.Dead && after.State == probe.Dead
	if failed && ms.cooldown != 0 {
		// a real failure ends the run of false alarms, explained signals or not
		od.resetCooldown(m, "dead")
	}
	cause := od.causeOf(c)
	// A change of eligibility the round took in may have moved the selection off the member
	// before its failure was handled: the member it held at the start of the round counts.
	// a member judging its own availability that is pinned off or excluded inside is no
	// failure a search answers
	if failed && (wasCurrent || m == od.roundCurrent) && !c.Policy && !c.Started.IsZero() {
		if od.search == nil {
			od.search = &searchState{id: searchIDs.Add(1), t0: c.Started, cause: c.Exec}
			fields := od.event("searchStart")
			fields["search"] = od.search.id
			fields["t0"] = formatTime(c.Started)
			fields["member"] = od.ref(m)
			fields["cause"] = cause
			fields["display"] = od.searchDisplay(true, m, "")
			od.record(fields)
		} else {
			fields := od.event("searchAdvance")
			fields["search"] = od.search.id
			fields["member"] = od.ref(m)
			fields["cause"] = cause
			od.record(fields)
		}
	}
	if !failed && after.State == probe.Alive && prev.State != probe.Alive {
		cause.Reason = causeRecover
	}
	od.reselect(now, cause)
}

// satisfyObligation explains the signals of m that came before a test started; seq is the
// event of its result, 0 when the test was taken from evidence already held.
func (od *onDemand) satisfyObligation(m C.Proxy, started time.Time, exec, seq uint64, ok bool, now time.Time) {
	ms := od.member(m)
	var rest []time.Time
	for _, at := range ms.signals {
		if at.After(started) {
			rest = append(rest, at)
		}
	}
	fields := od.event("obligation")
	fields["member"] = od.ref(m)
	fields["end"] = "satisfied"
	fields["since"] = formatTime(ms.signals[0])
	fields["probe"] = exec
	if seq != 0 {
		fields["probeSeq"] = seq
	}
	fields["ok"] = ok
	od.recordAs("obligation", fields)
	ms.signals = rest
	if len(rest) == 0 {
		ms.obligationDial = time.Time{}
	}
	switch {
	case ok && exec != 0 && exec == ms.cooldownExec:
		// the test already counted as a false alarm
	case ok:
		ms.cooldownExec = exec
		if ms.cooldown == 0 {
			ms.cooldown = probe.CooldownFirst
		} else {
			ms.cooldown = minOf(ms.cooldown*2, probe.CooldownMax)
		}
		ms.cooldownUntil = now.Add(ms.cooldown)
		fields := od.event("cooldown")
		fields["member"] = od.ref(m)
		fields["cooldown"] = ms.cooldown.Milliseconds()
		fields["until"] = formatTime(ms.cooldownUntil)
		od.record(fields)
	case ms.cooldown != 0:
		od.resetCooldown(m, "dead")
	}
}

func (od *onDemand) resetCooldown(m C.Proxy, reason string) {
	ms := od.member(m)
	ms.cooldown = 0
	ms.cooldownUntil = time.Time{}
	ms.cooldownReset = time.Now()
	fields := od.event("cooldownReset")
	fields["member"] = od.ref(m)
	fields["reason"] = reason
	od.record(fields)
}

func (od *onDemand) revokeObligation(m C.Proxy, reason string) {
	ms := od.states[m]
	if ms == nil || len(ms.signals) == 0 {
		return
	}
	od.recordRevoked(m, ms.signals[0], reason)
	ms.signals = nil
	ms.obligationDial = time.Time{}
}

func (od *onDemand) recordRevoked(m C.Proxy, since time.Time, reason string) {
	fields := od.event("obligation")
	fields["member"] = od.ref(m)
	fields["end"] = "revoked"
	fields["reason"] = reason
	fields["since"] = formatTime(since)
	od.recordAs("obligation", fields)
}

func (od *onDemand) onSignal(s signalNote, now time.Time) {
	fields := signalFields(s, od.ref(s.member))
	if s.cond != od.cond || !od.isMember(s.member) {
		od.coord.Records().Signal(od.name(), "stale", fields)
		return
	}
	if od.delegated(s.member) {
		// it handles what went wrong along its paths itself
		od.coord.Records().Signal(od.name(), "delegated", fields)
		return
	}
	od.handleSignal(s, fields, now)
}

// signalFields are what is recorded of a signal.
func signalFields(s signalNote, member any) map[string]any {
	fields := map[string]any{
		"member":   member,
		"network":  s.network,
		"strong":   s.strong,
		"class":    s.class,
		"dialed":   formatTime(s.dialed),
		"age":      s.age.Milliseconds(),
		"sent":     s.sent,
		"probe":    s.probe,
		"reloaded": s.beforeReload,
	}
	if s.err != "" {
		fields["error"] = s.err
	}
	if s.count > 0 {
		fields["count"] = s.count
	}
	return fields
}

func (od *onDemand) handleSignal(s signalNote, fields map[string]any, now time.Time) {
	ms := od.member(s.member)
	if !ms.lastSuccessDial.IsZero() {
		fields["lastSuccessDial"] = formatTime(ms.lastSuccessDial)
	}
	if ms.cooldown != 0 {
		fields["cooldown"] = map[string]any{"cooldown": ms.cooldown.Milliseconds(), "until": formatTime(ms.cooldownUntil)}
	}
	ms.lastSignal = now
	var disposition string
	switch {
	case s.dialed.Before(ms.lastSuccessDial):
		disposition = "suppressed"
	case !od.obligationTarget(s):
		disposition = "notCurrent"
	case len(ms.signals) > 0:
		disposition = "merged"
		ms.signals = append(ms.signals, s.at)
		ms.obligationDial = maxTime(ms.obligationDial, s.dialed)
	default:
		ms.signals = append(ms.signals, s.at)
		ms.obligationDial = s.dialed
		if now.Before(ms.cooldownUntil) {
			disposition = "obligation"
		} else {
			disposition = "verify"
		}
	}
	od.coord.Records().Signal(od.name(), disposition, fields)
	if !s.probe && s.count == 0 {
		if phase := failurePhase(s.class); phase != "" {
			if sum := od.summaryOf(s.member, s.dialed); sum != nil {
				sum.failed[phase]++
			}
		}
	}
	// A route that lost to a candidate is set aside until the verification it calls for ends.
	if s.class == "slow" && s.member == od.current && od.route == od.current && (disposition == "verify" || disposition == "merged") && !now.Before(ms.cooldownUntil) {
		od.suspend(&suspendState{member: s.member, since: now, reason: "loss", after: s.at}, now)
	}
}

// obligationTarget reports whether a signal of a member forms an obligation: the preferred
// member, the route, and a member the fast tier called on while it stays among the standbys.
func (od *onDemand) obligationTarget(s signalNote) bool {
	if s.member == od.current || s.member == od.route {
		return true
	}
	if !s.hedged {
		return false
	}
	refill, _ := od.refillRange()
	return slices.Contains(refill, s.member)
}

func (od *onDemand) onSuccess(s successNote, now time.Time) {
	if s.probe || s.cond != od.cond || !od.isMember(s.member) {
		return
	}
	if !od.activated {
		od.earlySuccesses = append(od.earlySuccesses, s)
	}
	ms := od.member(s.member)
	ms.lastSuccessDial = maxTime(ms.lastSuccessDial, s.dialed)
	od.pruneUDPSilent(ms, now)
	// a member judging its own availability is witnessed by what it publishes only
	if q := od.quality[s.member]; q != nil && q.trial != nil && !s.dialed.Before(q.trial.started) && !od.delegated(s.member) {
		q.trial.witness = true
	}
	if sum := od.summaryOf(s.member, s.dialed); sum != nil {
		sum.firstData++
	}
	if len(ms.signals) > 0 && s.dialed.After(ms.obligationDial) {
		fields := od.event("obligation")
		fields["member"] = od.ref(s.member)
		fields["end"] = "suppressed"
		fields["since"] = formatTime(ms.signals[0])
		fields["successDial"] = formatTime(s.dialed)
		od.recordAs("obligation", fields)
		ms.signals = nil
		ms.obligationDial = time.Time{}
	}
}

// onProbeDial opens the dependency repair window when a probe passes through the group while
// a member it prefers is dead, so that the routes through it can come back.
func (od *onDemand) onProbeDial(msg probeDial, now time.Time) {
	if !od.hasDeadBeforeCurrent() {
		return
	}
	opened := !now.Before(od.repairUntil)
	od.repairUntil = now.Add(probe.RepairWindow)
	if opened {
		fields := od.event("repair")
		fields["open"] = true
		fields["probe"] = msg.exec
		fields["until"] = formatTime(od.repairUntil)
		od.record(fields)
	}
}

func (od *onDemand) hasDeadBeforeCurrent() bool {
	for _, p := range od.order {
		if p == od.current {
			return false
		}
		if !od.unprobed(p) && !od.excluded(p) && od.view[p].State == probe.Dead {
			return true
		}
	}
	return true // no member is available
}

func (od *onDemand) onSwitch(s switchNote, now time.Time) {
	fields := od.recordFieldsFor("switch")
	fields["from"] = map[string]any{"name": s.from.name, "object": s.from.object}
	fields["to"] = map[string]any{"name": s.to.name, "object": s.to.object}
	fields["cause"] = s.cause
	fields["fromEvidence"] = od.evidenceOf(od.memberOf(s.from))
	fields["toEvidence"] = od.evidenceOf(od.memberOf(s.to))
	fields["pool"] = od.refs(od.pool)
	if od.search != nil {
		fields["search"] = od.search.id
	}
	fields["display"] = od.switchDisplay(s.from, s.to, s.cause)
	od.recordAs("switch", fields)
}

// memberOf finds the member a dial went through, nil when it is no longer a member.
func (od *onDemand) memberOf(d dialedMember) C.Proxy {
	for _, p := range od.members {
		if p.Name() == d.name && od.coord.KnownObjectID(p) == d.object {
			return p
		}
	}
	return nil
}

// update advances the search and the standby pool after the messages of a round.
func (od *onDemand) update(now time.Time) {
	od.updateUse(now)
	if !od.repairUntil.IsZero() && !now.Before(od.repairUntil) {
		od.repairUntil = time.Time{}
		fields := od.event("repair")
		fields["open"] = false
		od.record(fields)
	}
	for p, ms := range od.states {
		if ms.cooldown != 0 && !ms.lastSignal.IsZero() && now.Sub(ms.lastSignal) >= probe.CooldownReset {
			od.resetCooldown(p, "quiet")
		}
	}
	// A test after the switch confirms the member, and so does a success fresh enough that
	// the switch would not have asked for one, such as that of a test running meanwhile.
	if cur := od.continueTarget(); cur != nil && !od.continueAt.IsZero() && (!od.view[cur].Started.Before(od.continueAt) || od.fresh(cur, now)) {
		od.continueAt, od.continueMember = time.Time{}, nil
	}
	// A signal that came late, after a test that started later than it was handled, is
	// explained by that test: no completion is coming for it.
	for p, ms := range od.states {
		if od.unprobed(p) || len(ms.signals) == 0 {
			continue
		}
		if ev := od.view[p]; ev.State != probe.Unknown && !ev.Started.Before(ms.signals[0]) {
			od.satisfyObligation(p, ev.Started, ev.Exec, ev.Seq, ev.State == probe.Alive, now)
		}
	}
	od.commitDegradations(now)
	if changed, cause := od.updateStandings(now); changed {
		od.computeOrder()
		od.reselect(now, selectionCause{Reason: cause})
		od.setRoute(now, selectionCause{Reason: cause})
	}
	od.updateSuspend(now)
	// The obligations of members other than the current one and the route last while they
	// stay among the standbys.
	refill, _ := od.refillRange()
	for p, ms := range od.states {
		if len(ms.signals) > 0 && p != od.current && p != od.route && !slices.Contains(refill, p) {
			od.revokeObligation(p, "leftStandbys")
		}
	}
	od.updateSearch()
	od.updatePool(now, od.inUseLocked(now))
	od.flushSummaries(now, false)
}

// continueTarget is the member a verification after a switch is for.
func (od *onDemand) continueTarget() C.Proxy {
	if od.continueMember != nil {
		return od.continueMember
	}
	return od.current
}

// certainUntil tells until when the route is certain: no obligation and no verification of it,
// not on trial, and known alive lately or a new connection through it received data lately.
func (od *onDemand) certainUntil(now time.Time) time.Time {
	r := od.route
	if !od.mayBeCertain(r) {
		return time.Time{}
	}
	if child := od.delegateOf(r); child != nil {
		// as certain as the group finds the route it selects
		if until := child.snap.Load().certainNow(); until.After(now) {
			return until
		}
		return time.Time{}
	}
	var until time.Time
	if ev := od.view[r]; ev.State == probe.Alive && od.fresh(r, now) {
		until = ev.Started.Add(od.refreshAfter(r, now))
	}
	if ms := od.states[r]; ms != nil && !ms.lastSuccessDial.IsZero() {
		until = maxTime(until, ms.lastSuccessDial.Add(probe.CertainWindow))
	}
	if !until.After(now) {
		return time.Time{}
	}
	return until
}

// mayBeCertain reports whether nothing the group knows of a route keeps it from being certain.
func (od *onDemand) mayBeCertain(r C.Proxy) bool {
	if r == nil || od.unprobed(r) || od.onTrial(r) {
		return false
	}
	if ms := od.states[r]; ms != nil && len(ms.signals) > 0 {
		return false
	}
	return od.continueTarget() != r || od.continueAt.IsZero()
}

// firstUse verifies the route once the group comes into use with a route not certain, as
// after a start or a reload.
func (od *onDemand) firstUse(now time.Time) {
	r := od.route
	if r == nil || od.unprobed(r) || !od.certainUntil(now).IsZero() {
		return
	}
	od.continueMember, od.continueAt = r, now
}

func (od *onDemand) updateUse(now time.Time) {
	active := time.Unix(0, od.lastActive.Load())
	inUse := od.lastActive.Load() != 0 && now.Sub(active) < probe.UseWindow
	if od.inUse && !inUse {
		od.inUse = false
		fields := od.event("use")
		fields["inUse"] = false
		fields["reason"] = "expired"
		od.record(fields)
	} else if !od.inUse && inUse {
		od.inUse = true
		fields := od.event("use")
		fields["inUse"] = true
		fields["reason"] = "inherited"
		od.record(fields)
		od.firstUse(now)
	}
}

// candidates of the search: available non-built-in members before the first built-in one,
// in the effective order.
func (od *onDemand) searchable() []C.Proxy {
	var list []C.Proxy
	for _, p := range od.order {
		if od.unprobed(p) {
			break
		}
		if od.available(p) {
			list = append(list, p)
		}
	}
	return list
}

func (od *onDemand) confirmed(p C.Proxy) bool {
	ev := od.view[p]
	return ev.State != probe.Unknown && !ev.Started.Before(od.search.t0)
}

func (od *onDemand) updateSearch() {
	s := od.search
	if s == nil {
		return
	}
	list := od.searchable()
	if len(list) == 0 {
		od.endSearch("exhausted")
		return
	}
	_, pool := od.refillRange()
	scope := list[:minOf(len(list), 1+maxInt(probe.PoolSize, len(pool)))]
	found := true
	for _, p := range scope {
		if !od.confirmed(p) {
			found = false
			break
		}
	}
	if found {
		od.endSearch("found")
		return
	}
	window := od.searchWindow(list)
	if !slices.Equal(scope, s.scope) {
		// The members of the batch the new scope does not need are not waited for.
		kept := s.members[:0]
		for _, p := range s.members {
			if slices.Contains(window, p) {
				kept = append(kept, p)
			}
		}
		s.members = kept
		s.scope = slices.Clone(scope)
		fields := od.event("searchScope")
		fields["search"] = s.id
		fields["scope"] = od.refs(scope)
		od.record(fields)
	}
	for _, p := range s.members {
		if od.isMember(p) && od.available(p) && !od.confirmed(p) {
			return // the batch still waits for a result
		}
	}
	var batch []C.Proxy
	for _, p := range window {
		if !od.confirmed(p) {
			batch = append(batch, p)
		}
	}
	s.batch++
	s.members = batch
	fields := od.event("searchBatch")
	fields["search"] = s.id
	fields["batch"] = s.batch
	fields["members"] = od.refs(batch)
	od.record(fields)
}

// searchWindow is what a batch of the search is taken from: the members of the scope, and one
// more past it in case the cause of the failure takes them down too, as long as at most a
// batch of them is still to confirm.
func (od *onDemand) searchWindow(list []C.Proxy) []C.Proxy {
	_, pool := od.refillRange()
	var window []C.Proxy
	unconfirmed := 0
	for _, p := range list {
		window = append(window, p)
		if !od.confirmed(p) {
			unconfirmed++
		}
		if len(window) >= 2+maxInt(probe.PoolSize, len(pool)) || unconfirmed >= probe.BatchSize {
			break
		}
	}
	return window
}

func (od *onDemand) endSearch(result string) {
	fields := od.event("searchEnd")
	fields["search"] = od.search.id
	fields["result"] = result
	fields["current"] = od.ref(od.current)
	od.search = nil
	od.refill = nil
	_, pool := od.refillRange()
	fields["pool"] = od.refs(pool)
	fields["display"] = od.searchDisplay(false, od.current, result)
	od.record(fields)
}

// refillRange returns the members after the current one, up to the first built-in member,
// that standbys are kept among, and the standbys. The range holds RefillRange members in good
// standing and not persistently dead, at most RefillMax members; the standbys are the
// available members of it up to the trusted ones the pool needs.
func (od *onDemand) refillRange() (refill, pool []C.Proxy) {
	if od.current == nil || od.unprobed(od.current) {
		// past a built-in member nothing is routed through: no standbys
		return nil, nil
	}
	now := od.now()
	after := false
	healthy := 0
	for _, p := range od.order {
		if !after {
			after = p == od.current
			continue
		}
		if od.unprobed(p) {
			break
		}
		if od.excluded(p) {
			continue
		}
		refill = append(refill, p)
		if od.standingOf(p) == standingNormal && !od.persistentlyDead(p, now) {
			healthy++
		}
		if healthy == probe.RefillRange || len(refill) == probe.RefillMax {
			break
		}
	}
	pool, _, _, _ = od.dynamicPool(refill, now)
	return refill, pool
}

// refillCandidate reports whether p should be tested to fill the pool: unknown, alive by an
// old test, or dead with its backoff over.
func (od *onDemand) refillCandidate(p C.Proxy, now time.Time) bool {
	ev := od.view[p]
	switch ev.State {
	case probe.Unknown:
		return true
	case probe.Alive:
		return !od.fresh(p, now)
	default:
		return !now.Before(ev.NextAllowed)
	}
}

func (od *onDemand) updatePool(now time.Time, inUse bool) {
	refill, pool := od.refillRange()
	if !slices.Equal(pool, od.pool) {
		_, trusted, target, limited := od.dynamicPool(refill, now)
		fields := od.event("pool")
		fields["from"] = od.refs(od.pool)
		fields["to"] = od.refs(pool)
		var members []any
		for _, p := range pool {
			members = append(members, map[string]any{"member": od.ref(p), "trusted": od.trusted(p, now), "standing": od.standingOf(p).String(), "counts": od.countsOf(p, now), "fresh": od.fresh(p, now)})
		}
		fields["members"] = members
		fields["trusted"] = trusted
		fields["target"] = target
		fields["limit"] = probe.PoolMax
		fields["limited"] = limited
		od.record(fields)
		od.pool = pool
	}
	if od.search != nil || od.current == nil || !inUse || !od.poolGap(pool) {
		od.refill = nil
		return
	}
	var candidates []C.Proxy
	for _, p := range refill {
		if od.refillCandidate(p, now) {
			candidates = append(candidates, p)
		}
	}
	if od.refill != nil {
		// The batch waits for its members the range still has as candidates.
		kept := od.refill.members[:0]
		for _, p := range od.refill.members {
			if slices.Contains(candidates, p) {
				kept = append(kept, p)
			}
		}
		od.refill.members = kept
		if len(kept) > 0 {
			return
		}
	}
	if len(candidates) == 0 {
		// nothing to test now; the members backing off are tried again when they may be
		return
	}
	batch := candidates[:minOf(len(candidates), probe.BatchSize)]
	if od.refill == nil {
		od.refill = &refillState{}
	}
	od.refill.batch++
	od.refill.members = slices.Clone(batch)
	fields := od.event("refillBatch")
	fields["batch"] = od.refill.batch
	fields["members"] = od.refs(batch)
	od.record(fields)
}

// poolGap reports whether the standbys fall short: fewer than the pool holds, or one of them
// not known alive. The pool is filled in batches then.
func (od *onDemand) poolGap(pool []C.Proxy) bool {
	if len(pool) < probe.PoolSize {
		return true
	}
	for _, p := range pool {
		if od.view[p].State != probe.Alive {
			return true
		}
	}
	return false
}

func (od *onDemand) inUseLocked(now time.Time) bool {
	active := od.lastActive.Load()
	return active != 0 && now.Sub(time.Unix(0, active)) < probe.UseWindow
}

// deriveLocked returns the tests the group needs in its current state.
func (od *onDemand) deriveLocked(now time.Time) []probe.Need {
	needs := map[C.Proxy]*probe.Need{}
	base := od.snapshotFields(now)
	add := func(p C.Proxy, class probe.Class, rank int, source string, notBefore time.Time, search, batch uint64) {
		if p == nil || od.unprobed(p) || od.excluded(p) {
			return
		}
		if need, ok := needs[p]; ok {
			switch {
			case source == probe.SourceStandby && need.Source != probe.SourceStandby:
				// the need of the member itself stands; the test serves as the standby too
			case need.Source == probe.SourceStandby && source != probe.SourceStandby && class <= need.Class,
				class < need.Class || (class == need.Class && rank < need.Rank):
				need.Class, need.Rank, need.Source = class, rank, source
			}
			if notBefore.After(need.NotBefore) {
				need.NotBefore = notBefore
			}
			if search != 0 {
				need.Search, need.Batch = search, batch
			}
			return
		}
		needs[p] = &probe.Need{Target: p, Cond: od.cond, Class: class, Rank: rank, Source: source, NotBefore: notBefore, Search: search, Batch: batch}
	}
	cur := od.current
	var preferredSince time.Time // what the verification of the preferred member starts after
	if cur != nil && !od.unprobed(cur) {
		ms := od.member(cur)
		if len(ms.signals) > 0 && !now.Before(ms.cooldownUntil) {
			add(cur, probe.ClassVerify, 0, probe.SourceVerify, ms.signals[0], 0, 0)
			preferredSince = ms.signals[0]
		}
	}
	if target := od.continueTarget(); target != nil && !od.unprobed(target) && !od.continueAt.IsZero() && od.view[target].Started.Before(od.continueAt) {
		add(target, probe.ClassVerify, 1, probe.SourceContinue, od.continueAt, 0, 0)
		if target == cur && preferredSince.IsZero() {
			preferredSince = od.continueAt
		}
	}
	// The route on trial is verified once it carries traffic.
	if r := od.route; r != nil && !od.unprobed(r) {
		if q := od.quality[r]; q != nil && q.trial != nil && od.view[r].Started.Before(q.trial.started) {
			add(r, probe.ClassVerify, 1, probe.SourceContinue, q.trial.started, 0, 0)
			if r == cur && preferredSince.IsZero() {
				preferredSince = q.trial.started
			}
		}
	}
	// The obligations of the route and of the members the fast tier called on.
	for p, ms := range od.states {
		if p == cur || od.unprobed(p) || len(ms.signals) == 0 || now.Before(ms.cooldownUntil) {
			continue
		}
		rank := 3
		if p == od.route {
			rank = 1
		}
		add(p, probe.ClassVerify, rank, probe.SourceVerify, ms.signals[0], 0, 0)
	}
	// A standby is tested alongside the verification of the preferred member, to compare
	// with it and to confirm it.
	if !preferredSince.IsZero() {
		if standby := od.standbyFor(); standby != nil && !od.delegated(standby) {
			add(standby, probe.ClassVerify, 2, probe.SourceStandby, preferredSince, 0, 0)
			if od.standbys == nil {
				od.standbys = map[C.Proxy]time.Time{}
			}
			if since, ok := od.standbys[standby]; !ok || preferredSince.Before(since) {
				od.standbys[standby] = preferredSince
			}
		}
	}
	if s := od.search; s != nil {
		for i, p := range s.members {
			if od.isMember(p) && od.available(p) && !od.confirmed(p) {
				add(p, probe.ClassSearch, i, probe.SourceSearch, s.t0, s.id, s.batch)
			}
		}
	}
	inUse := od.inUseLocked(now)
	if inUse && od.search == nil && cur != nil {
		refill, pool := od.refillRange()
		if od.refill != nil {
			for i, p := range od.refill.members {
				if od.refillCandidate(p, now) {
					add(p, probe.ClassMaintain, i, probe.SourcePool, time.Time{}, 0, od.refill.batch)
				}
			}
		} else {
			for i, p := range pool {
				if od.view[p].State == probe.Alive && !od.fresh(p, now) {
					add(p, probe.ClassMaintain, i, probe.SourcePool, time.Time{}, 0, 0)
				}
			}
		}
		if len(pool) >= probe.PoolSize {
			last := pool[len(pool)-1]
			for i, p := range refill {
				if p == last {
					break
				}
				if od.view[p].State == probe.Dead {
					add(p, probe.ClassMaintain, 20+i, probe.SourcePool, time.Time{}, 0, 0)
				}
			}
		}
	}
	recover, forwardedRepair := od.forwardedNeeds(func(p C.Proxy, class probe.Class, notBefore time.Time) {
		add(p, class, 0, probe.SourceDelegate, notBefore, 0, 0)
	})
	repairing := now.Before(od.repairUntil)
	if inUse || repairing || recover {
		// asked for by repair windows only, this one or those of the groups that forwarded it
		var repairUntil time.Time
		if !inUse && (!recover || !forwardedRepair.IsZero()) {
			repairUntil = forwardedRepair
			if repairing && od.repairUntil.After(repairUntil) {
				repairUntil = od.repairUntil
			}
		}
		for i, p := range od.order {
			if p == cur {
				break
			}
			if od.view[p].State == probe.Dead {
				add(p, probe.ClassMaintain, 40+i, probe.SourceRecover, time.Time{}, 0, 0)
				if need := needs[p]; need != nil && need.Source == probe.SourceRecover && !repairUntil.IsZero() {
					need.RepairUntil = repairUntil
				}
			}
		}
	}
	// the versions of what the derivation read: the config, and the table it evaluated
	config, _ := od.coord.Records().Versions()
	exclusions := od.table.Version
	list := make([]probe.Need, 0, len(needs))
	for _, p := range od.order {
		need, ok := needs[p]
		if !ok {
			continue
		}
		need.Config, need.Exclusions = config, exclusions
		snapshot := make(map[string]any, len(base)+2)
		for k, v := range base {
			snapshot[k] = v
		}
		snapshot["position"] = slices.Index(od.order, p)
		snapshot["excluded"] = od.excluded(p)
		need.Snapshot = snapshot
		list = append(list, *need)
	}
	return list
}

func (od *onDemand) publishSnapshot() {
	snap := &snapshot{
		states:   make(map[C.Proxy]probe.State, len(od.view)),
		excluded: map[C.Proxy]bool{},
		cause:    od.routeCause,
		objects:  make(map[C.Proxy]uint64, len(od.members)),
	}
	for _, p := range od.members {
		snap.objects[p] = od.coord.KnownObjectID(p)
	}
	for p, ev := range od.view {
		snap.states[p] = ev.State
	}
	for p, ms := range od.states {
		if ms.excluded {
			snap.excluded[p] = true
		}
	}
	now := od.now()
	snap.order = slices.Clone(od.order)
	snap.pinned = od.pinned
	snap.route = od.route
	if od.suspended != nil {
		snap.suspended = od.suspended.member
	}
	snap.trusted = map[C.Proxy]bool{}
	for _, p := range od.pool {
		if od.trusted(p, now) {
			snap.trusted[p] = true
		}
	}
	snap.certainUntil = od.certainUntil(now)
	snap.leafBuiltin = od.leafBuiltin
	snap.probed = od.probed
	snap.terminal = map[C.Proxy]bool{}
	for p, ev := range od.view {
		if ev.Terminal {
			snap.terminal[p] = true
		}
	}
	if od.mayBeCertain(od.route) {
		snap.certainChild = od.delegateOf(od.route)
	}
	od.snap.Store(snap)
}

// certainNow is until when the route of the snapshot is certain, read down through the
// groups judging their own availability it goes through, as they find their routes now.
func (snap *snapshot) certainNow() time.Time {
	for i := 0; i < 16 && snap.certainChild != nil; i++ {
		snap = snap.certainChild.snap.Load()
	}
	return snap.certainUntil
}

func (od *onDemand) armTimer(now time.Time) {
	var next time.Time
	consider := func(t time.Time) {
		if t.After(now) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	if active := od.lastActive.Load(); active != 0 {
		consider(time.Unix(0, active).Add(probe.UseWindow))
	}
	consider(od.repairUntil)
	for _, ms := range od.states {
		if len(ms.signals) > 0 {
			consider(ms.cooldownUntil)
		}
		if ms.cooldown != 0 && !ms.lastSignal.IsZero() {
			consider(ms.lastSignal.Add(probe.CooldownReset))
		}
	}
	for _, p := range od.pool {
		if od.view[p].State == probe.Alive {
			consider(od.view[p].Started.Add(od.refreshAfter(p, now)))
		}
	}
	for _, q := range od.quality {
		switch q.standing {
		case standingYield:
			consider(q.readmitAt)
		case standingTrial:
			if t := q.trial; t != nil && !t.resumed.IsZero() {
				consider(t.resumed.Add(probe.TrialLength - t.effective))
			}
		}
	}
	for p, q := range od.quality {
		if q.standing != standingYield {
			continue
		}
		// the counts fall as the anomalies leave the windows
		for _, event := range od.coord.Failures(probe.Key{Target: p, Cond: od.cond}) {
			consider(event.At.Add(probe.RecentWindow))
			consider(event.At.Add(probe.LongWindow))
		}
		for _, event := range q.degradations {
			consider(event.At.Add(probe.RecentWindow))
			consider(event.At.Add(probe.LongWindow))
		}
		for _, pool := range od.memberPools[p] {
			for _, event := range od.coord.PoolEvents(pool, od.cond) {
				consider(event.At.Add(probe.RecentWindow))
				consider(event.At.Add(probe.LongWindow))
			}
		}
	}
	for _, d := range od.degradations {
		consider(d.commitAt)
	}
	consider(od.suspendCheck)
	if len(od.summaries) > 0 {
		consider(time.Unix((od.summarized+2)*60, 0).Add(probe.SummaryDelay))
	}
	if refill, pool := od.refillRange(); od.search == nil && od.poolGap(pool) && od.inUseLocked(now) {
		for _, p := range refill {
			if ev := od.view[p]; ev.State == probe.Dead {
				consider(ev.NextAllowed)
			}
		}
	}
	if od.closed || next.IsZero() || (!od.timerAt.IsZero() && od.timerAt.Equal(next)) {
		return
	}
	if od.timer != nil {
		od.timer.Stop()
	}
	od.timerAt = next
	od.timer = time.AfterFunc(next.Sub(now)+10*time.Millisecond, func() {
		od.mutex.Lock()
		od.timerAt = time.Time{}
		od.mutex.Unlock()
		od.post(recheck{})
	})
}

// selectProxy returns the member the group routes through now: the route, the first available
// member in the effective order unless the preferred one is suspended. Members are excluded by
// the table as it is now, with the groups that do not avoid excluded members read again.
func (od *onDemand) selectProxy(metadata *C.Metadata, touch bool) C.Proxy {
	return od.planDial(metadata, touch, false).route
}

// planDial finds the route as selectProxy does and, with candidates, the members after it a
// dial may call on: available, before the first built-in member, the trusted standbys first.
func (od *onDemand) planDial(metadata *C.Metadata, touch, candidates bool) dialPlan {
	proxies := od.f.GetProxies(touch)
	snap := od.snap.Load()
	probed := probe.ProbedDirects(proxies)
	unprobed := func(p C.Proxy) bool { return probe.IsBuiltin(p) && !probed[p] || snap.terminal[p] }
	pinned := ""
	if pin := od.pin.Load(); pin != nil {
		pinned = *pin
	}
	var evaluator *probe.Evaluator
	if !od.coord.Exclusions().Empty() {
		evaluator = od.coord.NewEvaluator()
	}
	try := func(p C.Proxy) bool {
		if unprobed(p) {
			return true
		}
		if snap.states[p] == probe.Dead {
			return false
		}
		if evaluator == nil {
			if snap.excluded[p] {
				od.post(recheck{})
			}
			return true
		}
		excluded, _ := evaluator.Excluded(p)
		if excluded != snap.excluded[p] {
			od.post(recheck{})
		}
		return !excluded
	}
	plan := dialPlan{eligible: func(p C.Proxy) bool {
		// read again when the candidate is about to be dialed
		latest := od.snap.Load()
		if latest.states[p] == probe.Dead || p == latest.suspended {
			return false
		}
		if _, member := (*od.memberSet.Load())[p]; !member {
			return false
		}
		if od.coord.Exclusions().Empty() {
			return true
		}
		excluded, _ := od.coord.NewEvaluator().Excluded(p)
		return !excluded
	}}
	// The effective order of the last round, with the pin as set now and the members a
	// provider added since.
	present := make(map[C.Proxy]bool, len(proxies))
	for _, p := range proxies {
		present[p] = true
	}
	order := make([]C.Proxy, 0, len(proxies))
	var pinnedMember C.Proxy
	if pinned != "" {
		for _, p := range proxies {
			if p.Name() == pinned {
				pinnedMember = p
				order = append(order, p)
				break
			}
		}
	}
	listed := map[C.Proxy]bool{}
	for _, p := range snap.order {
		if present[p] && p != pinnedMember {
			order = append(order, p)
			listed[p] = true
		}
	}
	for _, p := range proxies {
		if !listed[p] && p != pinnedMember {
			order = append(order, p)
		}
	}
	skip := snap.suspended
	if pinned != snap.pinned {
		skip = nil
	}
	for _, p := range order {
		if !try(p) {
			continue
		}
		if plan.preferred == nil {
			plan.preferred = p
			if p == skip && !unprobed(p) {
				continue
			}
		}
		if plan.route == nil {
			if p != plan.preferred {
				if unprobed(p) {
					// nothing but a built-in outbound to set the preferred member aside for
					plan.route = plan.preferred
					break
				}
				if probe.IsBuiltin(p) {
					continue // a direct member carries nothing set aside
				}
				if leafBuiltin(p, metadata) {
					continue // it ends at a built-in outbound by the selection along it
				}
			}
			plan.route = p
			if !candidates || unprobed(p) {
				break
			}
			continue
		}
		if unprobed(p) {
			break
		}
		if probe.IsBuiltin(p) {
			continue // a dial is not moved to a direct member
		}
		plan.candidates = append(plan.candidates, p)
	}
	if plan.route == nil {
		plan.route = plan.preferred
	}
	if plan.route == nil {
		plan.route = rejectProxy
	}
	// the trusted standbys first, each part in order
	sorted := make([]C.Proxy, 0, len(plan.candidates))
	for _, p := range plan.candidates {
		if snap.trusted[p] {
			sorted = append(sorted, p)
		}
	}
	for _, p := range plan.candidates {
		if !snap.trusted[p] {
			sorted = append(sorted, p)
		}
	}
	plan.candidates = sorted
	plan.certain = plan.route == snap.route && time.Now().Before(snap.certainNow())
	return plan
}

// noteUse renews the use of the group by real traffic. A retired group passes it on to the
// group that replaced it, also when it was retired while this ran.
func (od *onDemand) noteUse(reason string) {
	od.noteUseAt(reason, od.now().UnixNano(), nil)
}

// noteUseAt renews the use at a time; data a connection received carries its binding, which
// passes the renewal on only while it is valid.
func (od *onDemand) noteUseAt(reason string, now int64, b *binding) {
	if b == nil || !b.probe {
		od.coord.NoteTraffic(now) // the local network is in use, whichever group takes it
	}
	if forward := od.forwardTo.Load(); forward != nil {
		if b == nil || b.target.Load() != nil && forward.References(b.member, b.cond) {
			forward.noteUseAt(reason, now, b)
		}
		return
	}
	if reason == "dial" {
		storeMax(&od.lastDial, now)
	} else {
		storeMax(&od.lastData, now)
	}
	prev := storeMax(&od.lastActive, now)
	if prev == 0 || time.Duration(now-prev) >= probe.UseWindow {
		od.post(useNote{reason: reason})
	}
	if forward := od.forwardTo.Load(); forward != nil && (b == nil || b.target.Load() != nil && forward.References(b.member, b.cond)) {
		forward.noteUseAt(reason, now, b)
	}
}

// storeMax raises v to at least value and returns what it held before.
func storeMax(v *atomic.Int64, value int64) int64 {
	for {
		old := v.Load()
		if old >= value || v.CompareAndSwap(old, value) {
			return old
		}
	}
}

// noteDialed records the member a real dial went through, and the switch when it is not the
// one the previous real dial went through.
func (od *onDemand) noteDialed(p C.Proxy) {
	snap := od.snap.Load()
	od.noteDialedAs(dialedMember{name: p.Name(), object: snap.objects[p], provider: p.ProxyInfo().ProviderName}, snap.cause)
}

// noteDialedAs records the member a dial went through by its identity; a retired group passes
// it on to the group that replaced it. An object number of 0 is one not known yet, as for the
// members of a config being published: it matches any object of the same name.
func (od *onDemand) noteDialedAs(current dialedMember, cause selectionCause) {
	if forward := od.forwardTo.Load(); forward != nil {
		forward.noteDialedAs(current, cause)
		return
	}
	for {
		prev := od.dialed.Load()
		if prev != nil && prev.name == current.name && (current.object == 0 || prev.object == current.object) {
			return
		}
		if od.dialed.CompareAndSwap(prev, &current) {
			if prev != nil && (prev.name != current.name || prev.object != 0) {
				od.post(switchNote{from: *prev, to: current, cause: cause})
			}
			return
		}
	}
}

func (od *onDemand) dialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	isProbe := C.IsProbe(ctx)
	plan := od.planDial(metadata, true, !isProbe && !od.strict)
	proxy := plan.route
	od.beforeDial(ctx, proxy, isProbe)
	if proxy == rejectProxy {
		return nil, fmt.Errorf("%s: %w", od.name(), errNoAvailable)
	}
	if !isProbe && od.watched(proxy) && len(plan.candidates) > 0 {
		return od.hedgedDial(ctx, metadata, plan)
	}
	dialed := time.Now()
	c, err := proxy.DialContext(ctx, metadata)
	canceled := err != nil && errors.Is(ctx.Err(), context.Canceled)
	if !isProbe && od.watched(proxy) {
		od.post(dialNote{member: proxy, dialed: dialed, ok: err == nil, canceled: canceled})
	}
	if err != nil {
		switch {
		case canceled:
			// canceled by the caller: it tells nothing of the member
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			od.dialFailedAs(proxy, err, dialed, "tcp", "timeout", false, isProbe)
		default:
			od.dialFailedAs(proxy, err, dialed, "tcp", "dial", false, isProbe)
		}
		return c, err
	}
	if isProbe {
		c.AppendToChains(od.f)
		if !od.watched(proxy) {
			return c, nil
		}
		return newObservedConn(c, od.observe(proxy, dialed, true)), nil
	}
	return od.wrap(c, proxy, dialed, false), nil
}

// wrap hands out a connection a member carried, observed for the member.
func (od *onDemand) wrap(c C.Conn, member C.Proxy, dialed time.Time, hedged bool) C.Conn {
	c.AppendToChains(od.f)
	if !od.watched(member) {
		return &trafficConn{Conn: c, binding: newTrafficBinding(od, member, dialed)}
	}
	od.noteCarried(member, hedged)
	obs := od.observe(member, dialed, false)
	obs.hedged = hedged
	return newObservedConn(c, obs)
}

// askedAsStandby reports whether a test of a member that started then serves as the standby
// a derivation asked for; those asked for too long ago are forgotten.
func (od *onDemand) askedAsStandby(p C.Proxy, started time.Time) bool {
	keep := od.cond.Timeout
	if keep <= 0 {
		keep = probe.DefaultTestTimeout
	}
	keep = 2*keep + probe.PairWindow + time.Minute
	now := od.now()
	for member, since := range od.standbys {
		if now.Sub(since) > keep {
			delete(od.standbys, member)
		}
	}
	since, ok := od.standbys[p]
	return ok && !started.Before(since)
}

// noteCarried records the member a real connection was handed out through.
func (od *onDemand) noteCarried(member C.Proxy, hedged bool) {
	snap := od.snap.Load()
	od.carried.Store(&carriedMember{
		dialedMember: dialedMember{name: member.Name(), object: snap.objects[member], provider: member.ProxyInfo().ProviderName},
		hedged:       hedged,
	})
}

func (od *onDemand) listenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := od.selectProxy(metadata, true)
	isProbe := C.IsProbe(ctx)
	od.beforeDial(ctx, proxy, isProbe)
	if proxy == rejectProxy {
		return nil, fmt.Errorf("%s: %w", od.name(), errNoAvailable)
	}
	dialed := time.Now()
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	canceled := err != nil && errors.Is(ctx.Err(), context.Canceled)
	if !isProbe && od.watched(proxy) {
		od.post(dialNote{member: proxy, dialed: dialed, ok: err == nil, canceled: canceled})
	}
	if err != nil {
		if !canceled {
			od.dialFailedAs(proxy, err, dialed, "udp", "dial", false, isProbe)
		}
		return pc, err
	}
	od.noteCarried(proxy, false)
	pc.AppendToChains(od.f)
	if !od.watched(proxy) {
		if isProbe {
			return pc, nil
		}
		return &trafficPacketConn{PacketConn: pc, binding: newTrafficBinding(od, proxy, dialed)}, nil
	}
	return newObservedPacketConn(pc, od.observe(proxy, dialed, isProbe)), nil
}

func (od *onDemand) beforeDial(ctx context.Context, proxy C.Proxy, isProbe bool) {
	if isProbe {
		od.post(probeDial{exec: probe.ExecutionOf(ctx)})
		return
	}
	od.noteUse("dial")
	if proxy != rejectProxy {
		od.noteDialed(proxy)
	}
}

// dialFailedAs tells the group of a dial through a member that failed; a real one may also
// lead the coordinator to check the local network.
func (od *onDemand) dialFailedAs(proxy C.Proxy, err error, dialed time.Time, network, class string, hedged bool, isProbe ...bool) {
	probeDial := len(isProbe) > 0 && isProbe[0]
	if errors.Is(err, C.ErrNotSupport) {
		return
	}
	if !od.watched(proxy) {
		// a member the group does not observe, such as the DIRECT it ends with, still tells of
		// the local network
		if !probeDial && proxy != rejectProxy {
			od.coord.NoteDialFailure(proxy, dialed)
		}
		return
	}
	note := signalNote{
		member: proxy, cond: od.cond, network: network, strong: true, class: class,
		at: time.Now(), dialed: dialed, probe: probeDial, hedged: hedged,
	}
	if err != nil {
		note.err = shortError(err)
	}
	od.post(note)
	if !probeDial {
		od.coord.NoteDialFailure(proxy, dialed)
	}
}

func shortError(err error) string {
	text := err.Error()
	if len(text) > 200 {
		text = text[:200]
	}
	return text
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// setPinned changes the pinned member, as set by source; the effective order puts it first.
func (od *onDemand) setPinned(name, source string) {
	od.pinMutex.Lock()
	defer od.pinMutex.Unlock()
	od.f.selected = name
	od.pin.Store(&name)
	od.post(pinNote{name: name, source: source})
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minOf[T int | time.Duration](a, b T) T {
	if a < b {
		return a
	}
	return b
}
