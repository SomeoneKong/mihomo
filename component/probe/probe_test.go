package probe

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// testAdapter is a node of a given type, with an optional dialer proxy.
type testAdapter struct {
	*outbound.Direct
	kind   C.AdapterType
	dialer string
}

func (a *testAdapter) Type() C.AdapterType { return a.kind }

func (a *testAdapter) ProxyInfo() C.ProxyInfo {
	info := a.Direct.ProxyInfo()
	info.DialerProxy = a.dialer
	return info
}

// pooled is a proxy in a traffic pool.
type pooled struct {
	C.Proxy
	pool string
}

func (p *pooled) TrafficPool() string { return p.pool }

func node(name string) C.Proxy {
	return adapter.NewProxy(&testAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name}), kind: C.Vless})
}

func pooledNode(name, pool, dialer string) C.Proxy {
	a := &testAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name}), kind: C.Vless, dialer: dialer}
	return &pooled{Proxy: adapter.NewProxy(a), pool: pool}
}

// testGroup is a group adapter for the exclusion evaluation.
type testGroup struct {
	*testAdapter
	members  []C.Proxy
	now      string
	onDemand bool
}

func (g *testGroup) Proxies() []C.Proxy { return g.members }
func (g *testGroup) OnDemand() bool     { return g.onDemand }

func (g *testGroup) Unwrap(*C.Metadata, bool) C.Proxy {
	for _, member := range g.members {
		if member.Name() == g.now {
			return member
		}
	}
	return nil
}

func group(name string, kind C.AdapterType, now string, onDemand bool, members ...C.Proxy) C.Proxy {
	a := &testAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name}), kind: kind}
	return adapter.NewProxy(&testGroup{testAdapter: a, members: members, now: now, onDemand: onDemand})
}

// fakeRunner answers tests from a table, optionally holding them until released.
type fakeRunner struct {
	mutex   sync.Mutex
	alive   map[C.Proxy]bool
	hold    map[C.Proxy]chan struct{}
	started map[C.Proxy]int
	running chan C.Proxy
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{alive: map[C.Proxy]bool{}, hold: map[C.Proxy]chan struct{}{}, started: map[C.Proxy]int{}, running: make(chan C.Proxy, 100)}
}

func (r *fakeRunner) run(ctx context.Context, key Key) C.ProbeResult {
	r.mutex.Lock()
	r.started[key.Target]++
	hold := r.hold[key.Target]
	r.mutex.Unlock()
	r.running <- key.Target
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
		}
	}
	r.mutex.Lock()
	alive := r.alive[key.Target]
	r.mutex.Unlock()
	if alive {
		return C.ProbeResult{Satisfied: true, Delay: 10, Status: 204}
	}
	return C.ProbeResult{ErrorClass: C.ProbeErrorDial, Error: "down"}
}

func (r *fakeRunner) set(p C.Proxy, alive bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.alive[p] = alive
}

func (r *fakeRunner) holdOn(p C.Proxy) chan struct{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	ch := make(chan struct{})
	r.hold[p] = ch
	return ch
}

func (r *fakeRunner) count(p C.Proxy) int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.started[p]
}

// recorder is a group that refers to a fixed set of proxies and keeps what it is told.
type recorder struct {
	name    string
	targets map[C.Proxy]bool
	mutex   sync.Mutex
	notices []Notice
}

func newRecorder(name string, targets ...C.Proxy) *recorder {
	r := &recorder{name: name, targets: map[C.Proxy]bool{}}
	for _, p := range targets {
		r.targets[p] = true
	}
	return r
}

func (r *recorder) Name() string                                { return r.name }
func (r *recorder) References(target C.Proxy, _ Condition) bool { return r.targets[target] }
func (r *recorder) Deliver(n Notice) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.notices = append(r.notices, n)
}

func (r *recorder) completions() []Completion {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var list []Completion
	for _, n := range r.notices {
		if c, ok := n.(Completion); ok {
			list = append(list, c)
		}
	}
	return list
}

var testCond = Condition{URL: "http://test/", Expected: "*", Timeout: time.Second}

func newTestCoordinator(t *testing.T) (*Coordinator, *fakeRunner) {
	t.Helper()
	runner := newFakeRunner()
	return New(time.Now, runner.run), runner
}

func activeRegistration(c *Coordinator, g Group) *Registration {
	reg := c.Register(g)
	c.Activate(nil, nil)
	return reg
}

func TestEvidenceKeepsTheTestThatStartedLast(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	activeRegistration(c, g)
	runner.set(x, false)
	release := runner.holdOn(x)
	done := make(chan struct{})
	go func() {
		c.Manual(context.Background(), x, testCond, "group")
		close(done)
	}()
	<-runner.running
	// A second test starts later and finds x alive while the first one is still running.
	runner.set(x, true)
	runner.mutex.Lock()
	delete(runner.hold, x)
	runner.mutex.Unlock()
	c.Manual(context.Background(), x, testCond, "proxies")
	require.Equal(t, Alive, c.Evidence(Key{x, testCond}).State)
	runner.set(x, false)
	close(release)
	<-done
	// The first test started earlier: its result is recorded but changes nothing.
	require.Equal(t, Alive, c.Evidence(Key{x, testCond}).State)
	completions := g.completions()
	require.Len(t, completions, 2)
	require.True(t, completions[0].Adopted)
	require.Equal(t, "proxies", completions[0].API)
	require.False(t, completions[1].Adopted)
	require.Equal(t, "group", completions[1].API)
}

func TestAMergedNeedRunsInTheSlotsOfItsMostUrgentRequest(t *testing.T) {
	c, runner := newTestCoordinator(t)
	var blockers []C.Proxy
	for i := 0; i < MaintainSlots; i++ {
		blockers = append(blockers, node("b"))
	}
	x := node("x")
	a := newRecorder("a", append(blockers, x)...)
	b := newRecorder("b", x)
	regA := c.Register(a)
	regB := c.Register(b)
	c.Activate(nil, nil)
	var needs []Need
	for i, p := range blockers {
		runner.holdOn(p)
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassMaintain, Rank: i, Source: SourcePool})
	}
	needs = append(needs, Need{Target: x, Cond: testCond, Class: ClassMaintain, Rank: 100, Source: SourcePool})
	regA.Submit(1, needs)
	for range blockers {
		<-runner.running
	}
	require.Zero(t, runner.count(x), "the maintenance slots are full")
	runner.set(x, true)
	regB.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now().Add(-time.Second)}})
	require.Equal(t, x, <-runner.running)
	require.Eventually(t, func() bool { return len(b.completions()) == 1 }, time.Second, time.Millisecond)
	completion := b.completions()[0]
	require.Len(t, completion.Requests, 2, "one execution serves both groups")
	require.Equal(t, 1, runner.count(x))
}

func TestVerificationDoesNotWaitForSearches(t *testing.T) {
	c, runner := newTestCoordinator(t)
	var searched []C.Proxy
	var needs []Need
	for i := 0; i < SearchSlots+2; i++ {
		p := node("s")
		searched = append(searched, p)
		runner.holdOn(p)
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassSearch, Rank: i, Source: SourceSearch, NotBefore: time.Now()})
	}
	x := node("x")
	g := newRecorder("g", append(searched, x)...)
	reg := activeRegistration(c, g)
	reg.Submit(1, needs)
	for i := 0; i < SearchSlots; i++ {
		<-runner.running
	}
	runner.set(x, true)
	reg.Submit(2, append(needs, Need{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}))
	select {
	case p := <-runner.running:
		require.Equal(t, x, p)
	case <-time.After(time.Second):
		t.Fatal("the verification waited for the search")
	}
}

func TestEveryGroupReferringToATargetHearsOfItsTests(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	asking := newRecorder("asking", x)
	other := newRecorder("other", x)
	unrelated := newRecorder("unrelated")
	reg := c.Register(asking)
	c.Register(other)
	c.Register(unrelated)
	c.Activate(nil, nil)
	runner.set(x, true)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	require.Eventually(t, func() bool { return len(other.completions()) == 1 }, time.Second, time.Millisecond)
	// A second test that finds the same state is told as well.
	time.Sleep(20 * time.Millisecond)
	reg.Submit(2, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	require.Eventually(t, func() bool { return len(other.completions()) == 2 }, time.Second, time.Millisecond)
	second := other.completions()[1]
	require.Equal(t, Alive, second.Before.State)
	require.Equal(t, Alive, second.After.State)
	require.Empty(t, unrelated.completions())
}

func TestADeadTargetIsTestedAgainAfterItsBackoff(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	runner := newFakeRunner()
	c := New(func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, runner.run)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	recover := []Need{{Target: x, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover}}
	reg.Submit(1, recover)
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, time.Second, time.Millisecond)
	ev := c.Evidence(Key{x, testCond})
	require.Equal(t, Dead, ev.State)
	require.Equal(t, now.Add(BackoffFirst), ev.NextAllowed)
	reg.Submit(2, recover)
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, runner.count(x), "the backoff holds the test")
	clock.Lock()
	now = now.Add(BackoffFirst)
	clock.Unlock()
	runner.set(x, true)
	c.kick()
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, time.Second, time.Millisecond)
	require.Equal(t, Alive, c.Evidence(Key{x, testCond}).State)
}

func TestBackoffDoubles(t *testing.T) {
	require.Equal(t, BackoffFirst, backoff(1))
	require.Equal(t, 2*BackoffFirst, backoff(2))
	require.Equal(t, BackoffMax, backoff(100))
}

func TestOlderSubmissionsAndInactiveGroupsAreIgnored(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := c.Register(g)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, runner.count(x), "an inactive group asks for nothing")
	c.Activate(nil, nil)
	reg.Submit(5, nil)
	reg.Submit(3, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, runner.count(x), "an older submission changes nothing")
}

func TestRetiringGroupsHearOfTestsUntilActivation(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	old := newRecorder("g", x)
	activeRegistration(c, old)
	c.BeginPublish()
	fresh := newRecorder("g", x)
	c.Register(fresh)
	require.Equal(t, Group(old), c.Retiring("g"))
	runner.set(x, true)
	c.Manual(context.Background(), x, testCond, "proxies")
	require.Len(t, old.completions(), 1)
	require.Len(t, fresh.completions(), 1)
	c.Activate([]Key{{x, testCond}}, nil)
	require.Nil(t, c.Retiring("g"))
	c.Manual(context.Background(), x, testCond, "proxies")
	require.Len(t, old.completions(), 1)
	require.Len(t, fresh.completions(), 2)
}

func TestPruneDropsTheEvidenceOfProxiesNoLongerReferred(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	activeRegistration(c, g)
	runner.set(x, true)
	c.Manual(context.Background(), x, testCond, "proxies")
	require.Equal(t, Alive, c.Evidence(Key{x, testCond}).State)
	id := c.ObjectID(x)
	c.Prune(nil, []C.Proxy{x})
	require.Equal(t, Unknown, c.Evidence(Key{x, testCond}).State)
	require.Equal(t, id, c.ObjectID(x), "the proxy is still in the config")
	c.Prune(nil, nil)
	require.NotEqual(t, id, c.ObjectID(x))
}

func TestRecordsKeepTheContextsTheirEventsReferTo(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	r := newRecords("instance", now, func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, 3)
	r.setParams(params())
	v1 := r.SetConfig(map[string]any{"source": "load"})
	r.Append("group", nil)
	v2 := r.SetConfig(map[string]any{"source": "load"})
	require.Equal(t, uint64(2), v2)
	page := r.Contexts([]uint64{v1, v2}, nil)
	require.Len(t, page.Contexts, 3, "params and both configs")
	clock.Lock()
	now = now.Add(contextGrace)
	clock.Unlock()
	for i := 0; i < 3; i++ {
		r.Append("group", nil)
	}
	page = r.Contexts([]uint64{v1, v2}, nil)
	require.Len(t, page.Contexts, 2, "no event refers to the first config any more")
	require.Equal(t, []ContextRef{{Kind: ContextConfig, Version: v1}}, page.Unavailable)
	events := r.Events(0, 10)
	require.Equal(t, uint64(2), events.Oldest)
	require.Len(t, events.Events, 3)
	var first map[string]any
	require.NoError(t, json.Unmarshal(events.Events[0], &first))
	require.EqualValues(t, 2, first["seq"])
	require.EqualValues(t, v2, first["config"])
}

func TestSignalsBeyondTheCapAreCounted(t *testing.T) {
	now := time.Unix(600, 0)
	var clock sync.Mutex
	r := newRecords("instance", now, func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, 1000)
	for i := 0; i < SignalsPerMinute+5; i++ {
		r.Signal("g", "suppressed", nil)
	}
	r.Signal("g", "verify", nil)
	require.Len(t, r.Events(0, 1000).Events, SignalsPerMinute+1)
	clock.Lock()
	now = now.Add(time.Minute)
	clock.Unlock()
	r.FlushSignals()
	events := r.Events(0, 1000).Events
	var last map[string]any
	require.NoError(t, json.Unmarshal(events[len(events)-1], &last))
	require.Equal(t, "dropped", last["disposition"])
	require.EqualValues(t, 5, last["count"])
}

func TestExclusionEvaluation(t *testing.T) {
	jk1 := pooledNode("jk1", "jinkela", "")
	mx1 := pooledNode("mx1", "maoxiong", "")
	reject := adapter.NewProxy(outbound.NewReject())
	dialers := group("JkDialer", C.Fallback, "", true, jk1, reject)
	exit := pooledNode("E1@JkDialer", "", "JkDialer")
	selector := group("Pick", C.Selector, "jk1", false, jk1, mx1)
	balance := group("Balance", C.LoadBalance, "", false, mx1, jk1)
	proxies := map[string]C.Proxy{"JkDialer": dialers, "Pick": selector, "Balance": balance}
	lookup := func(name string) C.Proxy { return proxies[name] }

	table := newExclusions(time.Now, func(_ []ExclusionEntry, _ uint64, publish func()) { publish() }, func(Table) {})
	require.True(t, table.Empty())
	table.Replace([]ExclusionEntry{{Pool: "jinkela"}})
	require.False(t, table.Empty())
	ev := table.NewEvaluator(lookup)

	excluded, trace := ev.Excluded(jk1)
	require.True(t, excluded)
	require.Equal(t, "pool", trace[len(trace)-1].By)
	excluded, _ = ev.Excluded(mx1)
	require.False(t, excluded)
	excluded, _ = ev.Excluded(dialers)
	require.True(t, excluded, "every non-built-in member of the on-demand group is excluded")
	excluded, trace = ev.Excluded(exit)
	require.True(t, excluded)
	require.Equal(t, "dialer", trace[len(trace)-1].By)
	excluded, _ = ev.Excluded(selector)
	require.True(t, excluded, "the selector selects an excluded node")
	excluded, _ = ev.Excluded(balance)
	require.True(t, excluded, "a load balance may pick the excluded node")
	excluded, _ = ev.Excluded(reject)
	require.False(t, excluded)

	table.Replace(nil)
	require.True(t, table.Empty())
}

func TestExclusionEvaluationEndsCycles(t *testing.T) {
	a := pooledNode("a", "", "B")
	b := pooledNode("b", "", "A")
	proxies := map[string]C.Proxy{"A": a, "B": b}
	table := newExclusions(time.Now, func(_ []ExclusionEntry, _ uint64, publish func()) { publish() }, func(Table) {})
	table.Replace([]ExclusionEntry{{Pool: "other"}})
	excluded, trace := table.NewEvaluator(func(name string) C.Proxy { return proxies[name] }).Excluded(a)
	require.True(t, excluded)
	require.Equal(t, "cycle", trace[0].By)
}

func TestExclusionsExpire(t *testing.T) {
	changed := make(chan Table, 4)
	table := newExclusions(time.Now, func(_ []ExclusionEntry, _ uint64, publish func()) { publish() }, func(t Table) { changed <- t })
	table.Replace([]ExclusionEntry{{Pool: "jinkela", Until: time.Now().Add(30 * time.Millisecond)}, {Pool: "kept"}})
	require.Len(t, (<-changed).Pools, 2)
	select {
	case current := <-changed:
		require.Equal(t, map[string]struct{}{"kept": {}}, current.Pools)
		require.Equal(t, []ExclusionEntry{{Pool: "kept"}}, table.List())
	case <-time.After(time.Second):
		t.Fatal("the entry did not expire")
	}
}

func TestCensusAsksForEachMemberOnceAnInterval(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	runner.set(x, true)
	activeRegistration(c, newRecorder("g", x)) // the census covers members of groups
	c.census.mutex.Lock()
	c.census.interval = MinCensusInterval
	c.census.reg = c.registerPersistent(censusGroup{})
	c.census.due = map[Key]time.Time{{x, testCond}: time.Now().Add(-time.Second)}
	c.census.tickLocked()
	c.census.mutex.Unlock()
	require.Equal(t, x, <-runner.running)
	require.Eventually(t, func() bool { return c.Evidence(Key{x, testCond}).State == Alive }, time.Second, time.Millisecond)
	c.census.tick()
	c.census.mutex.Lock()
	due := c.census.due[Key{x, testCond}]
	c.census.timer.Stop()
	c.census.mutex.Unlock()
	require.True(t, due.After(time.Now().Add(MinCensusInterval-time.Minute)), "the next census is an interval after the test")
}

func TestADoneNeedRunsAgainOnceLaterEvidenceUndoesIt(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	runner.set(x, true)
	recover := []Need{{Target: x, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover}}
	reg.Submit(1, recover)
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, time.Second, time.Millisecond)
	// a shared manual test finds x dead right after
	runner.set(x, false)
	c.Manual(context.Background(), x, testCond, "proxies")
	reg.Submit(2, recover)
	c.mutex.Lock()
	entry := reg.needs[Key{x, testCond}]
	status, eligible := entry.status, entry.eligibleAt
	c.mutex.Unlock()
	require.Equal(t, needPending, status, "the need is wanted again")
	require.False(t, eligible.IsZero(), "after the backoff of the failure")
}

func TestNoAutomaticTestStartsWithinABackoff(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	runner := newFakeRunner()
	c := New(func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, runner.run)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	c.Manual(context.Background(), x, testCond, "proxies")
	clock.Lock()
	now = now.Add(time.Second)
	clock.Unlock()
	// a search that asks for a test after the failure must still wait for its backoff
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassSearch, Source: SourceSearch, NotBefore: now}})
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, runner.count(x))
	clock.Lock()
	now = now.Add(BackoffFirst)
	clock.Unlock()
	c.kick()
	require.Eventually(t, func() bool { return runner.count(x) == 2 }, time.Second, time.Millisecond)
}

func TestExclusionContextsFollowTheTableVersions(t *testing.T) {
	c, _ := newTestCoordinator(t)
	c.exclusions.Replace([]ExclusionEntry{{Pool: "c"}})
	_, exclusions := c.records.Versions()
	require.Equal(t, c.exclusions.Current().Version, exclusions, "the table and its context are made current together")
	c.commitExclusions([]ExclusionEntry{{Pool: "b"}}, 9, func() {})
	c.commitExclusions([]ExclusionEntry{{Pool: "a"}}, 8, func() {})
	_, exclusions = c.records.Versions()
	require.EqualValues(t, 9, exclusions, "an older table recorded late does not replace a newer one")
}

func TestContextsWithoutEventsDoNotPileUp(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	advance := func() {
		clock.Lock()
		now = now.Add(contextGrace)
		clock.Unlock()
	}
	r := newRecords("instance", now, func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, 100)
	for i := 0; i < 100; i++ {
		r.SetConfig(map[string]any{})
		advance()
	}
	r.SetConfig(map[string]any{})
	require.Len(t, r.Contexts(nil, nil).Contexts, 2, "only the current config and the one it just replaced")
	r.SetConfig(map[string]any{})
	page := r.Contexts([]uint64{101}, nil)
	require.Empty(t, page.Unavailable, "a version just replaced stays a while")
	r.Pin(101, 0)
	advance()
	r.SetConfig(map[string]any{})
	page = r.Contexts([]uint64{101}, nil)
	require.Empty(t, page.Unavailable, "a pinned version stays")
	r.Unpin(101, 0)
	page = r.Contexts([]uint64{101}, nil)
	require.Len(t, page.Unavailable, 1)
}

func TestExclusionEvaluationDoesNotDependOnTheOrder(t *testing.T) {
	healthy := pooledNode("healthy", "", "")
	proxies := map[string]C.Proxy{}
	a := pooledNode("a", "", "G")
	g := group("G", C.Fallback, "", true, a, healthy)
	proxies["G"] = g
	table := newExclusions(time.Now, func(_ []ExclusionEntry, _ uint64, publish func()) { publish() }, func(Table) {})
	table.Replace([]ExclusionEntry{{Pool: "other"}})
	lookup := func(name string) C.Proxy { return proxies[name] }
	first := table.NewEvaluator(lookup)
	gExcluded, _ := first.Excluded(g)
	aAfterG, _ := first.Excluded(a)
	second := table.NewEvaluator(lookup)
	aAlone, trace := second.Excluded(a)
	require.False(t, gExcluded)
	require.Equal(t, aAlone, aAfterG, "the result of a member does not depend on what was evaluated before")
	require.NotEmpty(t, trace)
}

func TestTheCensusLeavesExcludedMembersAlone(t *testing.T) {
	c, runner := newTestCoordinator(t)
	var blockers []C.Proxy
	var needs []Need
	for i := 0; i < MaintainSlots; i++ {
		p := node("b")
		blockers = append(blockers, p)
		runner.holdOn(p)
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassMaintain, Rank: i, Source: SourcePool})
	}
	x := pooledNode("x", "jinkela", "")
	g := newRecorder("g", append(blockers, x)...)
	reg := activeRegistration(c, g)
	reg.Submit(1, needs)
	for range blockers {
		<-runner.running
	}
	c.census.mutex.Lock()
	c.census.interval = MinCensusInterval
	c.census.reg = c.registerPersistent(censusGroup{})
	c.census.due = map[Key]time.Time{{x, testCond}: time.Now().Add(-time.Second)}
	c.census.tickLocked()
	c.census.mutex.Unlock()
	c.exclusions.Replace([]ExclusionEntry{{Pool: "jinkela"}})
	for _, p := range blockers {
		runner.mutex.Lock()
		close(runner.hold[p])
		delete(runner.hold, p)
		runner.mutex.Unlock()
	}
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, runner.count(x), "an excluded member is not tested")
	c.census.mutex.Lock()
	c.census.timer.Stop()
	c.census.mutex.Unlock()
}

func TestRecordingADroppedTargetDoesNotNumberItAgain(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	runner.holdOn(x)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	<-runner.running
	c.Prune(nil, nil)
	c.mutex.Lock()
	_, kept := c.objects[x]
	c.mutex.Unlock()
	require.False(t, kept)
	runner.mutex.Lock()
	close(runner.hold[x])
	runner.mutex.Unlock()
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, time.Second, time.Millisecond)
	c.mutex.Lock()
	_, kept = c.objects[x]
	c.mutex.Unlock()
	require.False(t, kept, "the late result is recorded with the number it started with")
}

func TestAQueuedNeedTakesEvidenceThatCameMeanwhile(t *testing.T) {
	c, runner := newTestCoordinator(t)
	var blockers []C.Proxy
	var needs []Need
	for i := 0; i < MaintainSlots; i++ {
		p := node("b")
		blockers = append(blockers, p)
		runner.holdOn(p)
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassMaintain, Rank: i, Source: SourcePool})
	}
	x := node("x")
	runner.set(x, true)
	g := newRecorder("g", append(blockers, x)...)
	reg := activeRegistration(c, g)
	reg.Submit(1, append(needs, Need{Target: x, Cond: testCond, Class: ClassMaintain, Rank: 100, Source: SourcePool}))
	for range blockers {
		<-runner.running
	}
	c.Manual(context.Background(), x, testCond, "proxies")
	<-runner.running
	for _, p := range blockers {
		runner.mutex.Lock()
		close(runner.hold[p])
		delete(runner.hold, p)
		runner.mutex.Unlock()
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, runner.count(x), "the manual success does for the queued need")
}

func TestPruneVoidsManualTestsToo(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	activeRegistration(c, g)
	runner.set(x, false)
	release := runner.holdOn(x)
	done := make(chan struct{})
	go func() {
		c.Manual(context.Background(), x, testCond, "proxies")
		close(done)
	}()
	<-runner.running
	c.Prune(nil, nil)
	c.Prune([]Key{{x, testCond}}, nil)
	close(release)
	<-done
	got := g.completions()[0]
	require.False(t, got.Adopted, "a test of a target dropped meanwhile changes nothing")
	require.Equal(t, Unknown, c.Evidence(Key{x, testCond}).State)
	record := c.Records().Events(got.Seq-1, 1)
	require.Len(t, record.Events, 1)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(record.Events[0], &fields))
	require.Equal(t, true, fields["void"])
	require.NotContains(t, fields, "related")
}

func TestStandbysComeBeforeRecoveryAndTheCensus(t *testing.T) {
	c, runner := newTestCoordinator(t)
	var blockers []C.Proxy
	var needs []Need
	for i := 0; i < MaintainSlots; i++ {
		p := node("b")
		blockers = append(blockers, p)
		runner.holdOn(p)
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassMaintain, Rank: i, Source: SourcePool})
	}
	pool, census := node("pool"), node("census")
	a := newRecorder("a", append(blockers, pool)...)
	b := newRecorder("b", census)
	regA := c.Register(a)
	regB := c.Register(b)
	c.Activate(nil, nil)
	regA.Submit(1, needs)
	for range blockers {
		<-runner.running
	}
	regB.Submit(1, []Need{{Target: census, Cond: testCond, Class: ClassMaintain, Rank: 0, Source: SourceCensus, NotBefore: time.Now()}})
	regA.Submit(2, append(needs, Need{Target: pool, Cond: testCond, Class: ClassMaintain, Rank: 99, Source: SourcePool}))
	runner.mutex.Lock()
	close(runner.hold[blockers[0]])
	delete(runner.hold, blockers[0])
	runner.mutex.Unlock()
	require.Equal(t, pool, <-runner.running, "the free slot goes to the standby")
}

func TestASuccessEndsTheWaitOfABackoff(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	runner := newFakeRunner()
	c := New(func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, runner.run)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	c.Manual(context.Background(), x, testCond, "proxies")
	clock.Lock()
	now = now.Add(time.Second)
	clock.Unlock()
	t0 := now.Add(time.Second)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassSearch, Source: SourceSearch, NotBefore: t0}})
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, runner.count(x), "the failure backs the need off")
	// a later test finds x alive, still before what the need asks for
	runner.set(x, true)
	c.Manual(context.Background(), x, testCond, "proxies")
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, runner.count(x), "a test now could not do for a need asking for a later one")
	clock.Lock()
	now = t0.Add(time.Millisecond)
	clock.Unlock()
	c.kick()
	require.Eventually(t, func() bool { return runner.count(x) == 3 }, time.Second, time.Millisecond, "no backoff is left to wait for")
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 3, runner.count(x), "a test that started after what the need asks for does for it")
}

func TestTheCensusTestsAgainEveryInterval(t *testing.T) {
	now := time.Now()
	var clock sync.Mutex
	runner := newFakeRunner()
	c := New(func() time.Time { clock.Lock(); defer clock.Unlock(); return now }, runner.run)
	x := node("x")
	runner.set(x, true)
	activeRegistration(c, newRecorder("g", x))
	c.census.mutex.Lock()
	c.census.interval = MinCensusInterval
	c.census.reg = c.registerPersistent(censusGroup{})
	c.census.due = map[Key]time.Time{{x, testCond}: now.Add(-time.Second)}
	c.census.mutex.Unlock()
	for round := 1; round <= 3; round++ {
		c.census.tick()
		require.Eventually(t, func() bool { return runner.count(x) == round }, time.Second, time.Millisecond)
		require.Eventually(t, func() bool { return !c.Evidence(Key{x, testCond}).Started.IsZero() }, time.Second, time.Millisecond)
		c.census.tick()
		clock.Lock()
		now = now.Add(MinCensusInterval + time.Second)
		clock.Unlock()
	}
	c.census.mutex.Lock()
	c.census.timer.Stop()
	c.census.mutex.Unlock()
}

func TestProbedDirectsAreTheDirectMembersAheadOfOthers(t *testing.T) {
	direct := adapter.NewProxy(outbound.NewDirect())
	named := adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "home"}))
	reject := adapter.NewProxy(outbound.NewReject())
	a := node("a")
	require.True(t, ProbedDirects([]C.Proxy{direct, a, reject})[direct])
	require.True(t, ProbedDirects([]C.Proxy{named, a})[named], "a named direct node is one as well")
	require.False(t, ProbedDirects([]C.Proxy{a, direct})[direct], "a direct member at the end is the last resort")
	require.False(t, ProbedDirects([]C.Proxy{a, direct, reject})[direct], "only built-in members come after it")
	require.Empty(t, ProbedDirects([]C.Proxy{a, reject}))
}

func TestAProbedDirectMemberKeepsItsGroupFromBeingExcluded(t *testing.T) {
	direct := adapter.NewProxy(outbound.NewDirect())
	jk1 := pooledNode("jk1", "jinkela", "")
	reject := adapter.NewProxy(outbound.NewReject())
	ahead := group("Ahead", C.Fallback, "", true, direct, jk1, reject)
	behind := group("Behind", C.Fallback, "", true, jk1, direct)
	table := newExclusions(time.Now, func(_ []ExclusionEntry, _ uint64, publish func()) { publish() }, func(Table) {})
	table.Replace([]ExclusionEntry{{Pool: "jinkela"}})
	ev := table.NewEvaluator(func(string) C.Proxy { return nil })
	excluded, _ := ev.Excluded(ahead)
	require.False(t, excluded, "the direct member it tests is left")
	excluded, _ = ev.Excluded(behind)
	require.True(t, excluded, "a direct member at the end is not one it tests")
}
