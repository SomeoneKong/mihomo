package outboundgroup

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// routeAdapter is a node that dials directly but is not a built-in outbound.
type routeAdapter struct {
	*outbound.Direct
}

func (a *routeAdapter) Type() C.AdapterType { return C.Vless }

// pooledProxy puts a proxy in a traffic pool.
type pooledProxy struct {
	C.Proxy
	pool string
}

func (p *pooledProxy) TrafficPool() string { return p.pool }

type testRunner struct {
	mutex   sync.Mutex
	alive   map[string]bool
	hold    map[string]chan struct{}
	counts  map[string]int
	started chan string
	elapsed map[string]time.Duration // what a test reports it took
}

func (r *testRunner) run(ctx context.Context, key probe.Key) C.ProbeResult {
	name := key.Target.Name()
	r.mutex.Lock()
	r.counts[name]++
	hold := r.hold[name]
	r.mutex.Unlock()
	select {
	case r.started <- name:
	default:
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
		}
	}
	r.mutex.Lock()
	alive := r.alive[name]
	elapsed := r.elapsed[name]
	r.mutex.Unlock()
	if alive {
		return C.ProbeResult{Satisfied: true, Delay: 10, Status: 204, Elapsed: elapsed}
	}
	return C.ProbeResult{ErrorClass: C.ProbeErrorDial, Error: "down", Elapsed: elapsed}
}

func (r *testRunner) set(alive bool, names ...string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, name := range names {
		r.alive[name] = alive
	}
}

func (r *testRunner) count(name string) int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.counts[name]
}

func (r *testRunner) holdOn(name string) chan struct{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	ch := make(chan struct{})
	r.hold[name] = ch
	return ch
}

type odTest struct {
	t       *testing.T
	coord   *probe.Coordinator
	runner  *testRunner
	proxies map[string]C.Proxy
	groups  map[string]*Fallback
}

func newODTest(t *testing.T) *odTest {
	return newODTestAt(t, time.Now)
}

// newODTestAt is a test whose coordinator, and so the groups, keep the time of now.
func newODTestAt(t *testing.T, now func() time.Time) *odTest {
	runner := &testRunner{alive: map[string]bool{}, hold: map[string]chan struct{}{}, counts: map[string]int{}, started: make(chan string, 1000), elapsed: map[string]time.Duration{}}
	ot := &odTest{t: t, runner: runner, proxies: map[string]C.Proxy{}, groups: map[string]*Fallback{}}
	ot.coord = probe.New(now, runner.run)
	ot.coord.SetLookup(func(name string) C.Proxy { return ot.proxies[name] })
	ot.proxies["REJECT"] = adapter.NewProxy(outbound.NewReject())
	return ot
}

func (ot *odTest) node(name, pool string) C.Proxy {
	if p, ok := ot.proxies[name]; ok {
		return p
	}
	var p C.Proxy = adapter.NewProxy(&routeAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name})})
	if pool != "" {
		p = &pooledProxy{Proxy: p, pool: pool}
	}
	ot.proxies[name] = p
	return p
}

// group builds and activates an on-demand group of the named members.
func (ot *odTest) group(name string, members ...string) *Fallback {
	f := ot.build(name, members...)
	ot.activate(f)
	return f
}

func (ot *odTest) build(name string, members ...string) *Fallback {
	return ot.buildOption(GroupCommonOption{Name: name}, members...)
}

// strictGroup builds and activates an on-demand group of the named members in strict order.
func (ot *odTest) strictGroup(name string, members ...string) *Fallback {
	f := ot.buildOption(GroupCommonOption{Name: name, StrictOrder: true}, members...)
	ot.activate(f)
	return f
}

func (ot *odTest) buildOption(option GroupCommonOption, members ...string) *Fallback {
	name := option.Name
	var list []C.Proxy
	for _, member := range members {
		list = append(list, ot.node(member, ""))
	}
	pd, err := provider.NewCompatibleProvider(name, list, provider.NewHealthCheck(list, "http://test/", 1000, 0, true, nil))
	require.NoError(ot.t, err)
	option.URL, option.TestTimeout, option.ExpectedStatus, option.Probe = "http://test/", 1000, "*", ProbeOnDemand
	f, err := NewFallback(option, FallbackOption{}, nil, []P.ProxyProvider{pd})
	require.NoError(ot.t, err)
	f.od.coord = ot.coord
	ot.t.Cleanup(f.od.close)
	ot.proxies[name] = adapter.NewProxy(f)
	f.od.publish()
	return f
}

func (ot *odTest) activate(groups ...*Fallback) {
	var keys []probe.Key
	for _, f := range groups {
		for _, p := range f.GetProxies(false) {
			keys = append(keys, probe.Key{Target: p, Cond: f.od.cond})
		}
		ot.groups[f.Name()] = f
	}
	ot.coord.Activate(keys, nil)
	for _, f := range groups {
		f.od.post(activateNote{})
	}
}

// replace activates fresh in place of old as a reload does: the old group hears of no test
// from then on, handles what it heard before and hands its state over.
func (ot *odTest) replace(old, fresh *Fallback) {
	var keys []probe.Key
	for _, p := range fresh.GetProxies(false) {
		keys = append(keys, probe.Key{Target: p, Cond: fresh.od.cond})
	}
	fresh.od.updateMemberSet(fresh.GetProxies(false), topologyVersion.Add(1))
	ot.coord.Activate(keys, nil)
	state := old.od.retire(fresh.od)
	migrateBindings(old.od, fresh.od)
	fresh.od.post(inheritNote{state: state})
	fresh.od.post(activateNote{})
	ot.groups[fresh.Name()] = fresh
}

func (ot *odTest) current(f *Fallback) string {
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	if f.od.current == nil {
		return ""
	}
	return f.od.current.Name()
}

func (ot *odTest) eventually(cond func() bool, msg string) {
	ot.t.Helper()
	require.Eventually(ot.t, cond, 3*time.Second, time.Millisecond, msg)
}

func (ot *odTest) signal(f *Fallback, member string) {
	now := f.od.now()
	f.od.post(signalNote{member: ot.proxies[member], cond: f.od.cond, network: "tcp", strong: true, class: "eof", at: now, dialed: now})
}

func (ot *odTest) groupEvents(event string) []map[string]any {
	var list []map[string]any
	for _, raw := range ot.coord.Records().Events(0, 100000).Events {
		var e map[string]any
		require.NoError(ot.t, json.Unmarshal(raw, &e))
		if e["kind"] == "group" && e["event"] == event {
			list = append(list, e)
		}
	}
	return list
}

func (ot *odTest) records(kind string) []map[string]any {
	var list []map[string]any
	for _, raw := range ot.coord.Records().Events(0, 100000).Events {
		var e map[string]any
		require.NoError(ot.t, json.Unmarshal(raw, &e))
		if e["kind"] == kind {
			list = append(list, e)
		}
	}
	return list
}

func (ot *odTest) idle(f *Fallback) {
	ot.t.Helper()
	// Let the group handle what it was told, and the tests it asked for run.
	time.Sleep(50 * time.Millisecond)
}

func TestOnDemandRoutesThroughTheFirstMemberWithoutTesting(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects the first member")
	require.Equal(t, "A", f.Now())
	ot.idle(f)
	require.Zero(t, ot.runner.count("A")+ot.runner.count("B"), "nothing is tested while the group is not in use")
}

func TestOnDemandVerifiesASignalAndSearchesInBatches(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "E", "F", "G", "H")
	f := ot.group("g", "A", "B", "C", "D", "E", "F", "G", "H", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.signal(f, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("searchEnd")) == 1 }, "the search ends")
	end := ot.groupEvents("searchEnd")[0]
	require.Equal(t, "found", end["result"])
	require.Equal(t, "E", ot.current(f))
	require.Equal(t, 1, ot.runner.count("A"), "one verification")
	batches := ot.groupEvents("searchBatch")
	require.Len(t, batches, 2)
	require.Len(t, batches[0]["members"], probe.BatchSize, "the first batch tests four members at once")
	for _, name := range []string{"B", "C", "D", "E", "F", "G"} {
		require.Equal(t, 1, ot.runner.count(name), name)
	}
	start := ot.groupEvents("searchStart")[0]
	require.Equal(t, "verify", start["cause"].(map[string]any)["reason"])
	pool := f.od.state()["pool"].([]any)
	require.Len(t, pool, 2)
}

func TestOnDemandSearchEndsExhaustedOnTheBuiltinMember(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.signal(f, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("searchEnd")) == 1 }, "the search ends")
	require.Equal(t, "exhausted", ot.groupEvents("searchEnd")[0]["result"])
	require.Equal(t, "REJECT", ot.current(f))
	require.Equal(t, "REJECT", f.Now())
}

func TestOnDemandStartsASearchWhenAnotherGroupFindsItsCurrentMemberDead(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "B", "C", "D")
	f := ot.group("g", "A", "B", "C", "D", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	result := ot.coord.Manual(context.Background(), ot.proxies["A"], f.od.cond, "proxies")
	require.False(t, result.Satisfied)
	ot.eventually(func() bool { return len(ot.groupEvents("searchEnd")) == 1 }, "the search ends")
	cause := ot.groupEvents("searchStart")[0]["cause"].(map[string]any)
	require.Equal(t, "external", cause["reason"])
	require.Equal(t, "proxies", cause["sourceApi"])
	require.Equal(t, "B", ot.current(f))
}

func TestOnDemandSearchTestsAgainWhatAnEarlierTestFound(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "B", "C", "D", "E")
	f := ot.group("g", "A", "B", "C", "D", "E", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	// A test of B starts before A fails, and is still running when the search wants B.
	release := ot.runner.holdOn("B")
	go ot.coord.Manual(context.Background(), ot.proxies["B"], f.od.cond, "proxies")
	require.Equal(t, "B", <-ot.runner.started)
	time.Sleep(5 * time.Millisecond)
	ot.signal(f, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("searchStart")) == 1 }, "the search starts")
	ot.runner.mutex.Lock()
	delete(ot.runner.hold, "B")
	ot.runner.mutex.Unlock()
	close(release)
	ot.eventually(func() bool { return len(ot.groupEvents("searchEnd")) == 1 }, "the search ends after B is tested again")
	require.Equal(t, 2, ot.runner.count("B"), "the earlier success does not count")
	require.Equal(t, "found", ot.groupEvents("searchEnd")[0]["result"])
}

func TestOnDemandRefillsItsPoolInBatchesWhileInUse(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "D", "E", "F")
	f := ot.group("g", "A", "B", "C", "D", "E", "F", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.noteUse("dial")
	ot.eventually(func() bool {
		pool := f.od.state()["pool"].([]any)
		return len(pool) == 2 && pool[0].(map[string]any)["name"] == "D" && pool[1].(map[string]any)["name"] == "E"
	}, "the pool is D and E")
	require.Equal(t, 1, ot.runner.count("B"))
	require.Equal(t, 1, ot.runner.count("C"))
	ot.eventually(func() bool { return ot.runner.count("A") == 1 }, "the route never tested is verified once the group comes into use")
	require.NotEmpty(t, ot.groupEvents("refillBatch"))
}

func TestOnDemandCoolsDownAfterAFalseAlarm(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.signal(f, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("cooldown")) == 1 }, "the verification finds A alive")
	time.Sleep(20 * time.Millisecond) // a signal later than the verification, as the clock tells
	ot.signal(f, "A")
	ot.idle(f)
	require.Equal(t, 1, ot.runner.count("A"), "the cooldown defers the next verification")
	signals := ot.records("signal")
	require.Equal(t, "verify", signals[0]["disposition"])
	require.Equal(t, "obligation", signals[1]["disposition"])
	state := f.od.state()
	require.NotNil(t, state["members"].([]any)[0].(map[string]any)["obligation"])
}

func TestOnDemandSuppressesSignalsOfConnectionsDialedBeforeASuccess(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	dialed := time.Now()
	f.od.post(successNote{member: ot.proxies["A"], cond: f.od.cond, dialed: dialed.Add(time.Second)})
	f.od.post(signalNote{member: ot.proxies["A"], cond: f.od.cond, network: "tcp", strong: true, class: "eof", at: time.Now(), dialed: dialed})
	ot.eventually(func() bool { return len(ot.records("signal")) == 1 }, "the signal is recorded")
	require.Equal(t, "suppressed", ot.records("signal")[0]["disposition"])
	ot.idle(f)
	require.Zero(t, ot.runner.count("A"))
}

func TestOnDemandAvoidsExcludedPools(t *testing.T) {
	ot := newODTest(t)
	ot.node("A", "jinkela")
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "jinkela"}})
	require.Equal(t, "B", f.Now(), "the dial path avoids the excluded member at once")
	ot.eventually(func() bool { return ot.current(f) == "B" }, "the selection moves to B")
	ot.eventually(func() bool { return ot.runner.count("B") == 1 }, "the new member is verified")
	eligibility := ot.groupEvents("eligibility")
	require.Len(t, eligibility, 1)
	require.Equal(t, true, eligibility[0]["excluded"])
	ot.coord.Exclusions().Replace(nil)
	ot.eventually(func() bool { return ot.current(f) == "A" }, "back to A")
}

func TestOnDemandPinsAMember(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	require.NoError(t, f.Set("B"))
	require.Equal(t, "B", f.Now())
	ot.eventually(func() bool { return ot.current(f) == "B" }, "the pinned member comes first")
	ot.eventually(func() bool { return ot.runner.count("B") == 1 }, "the pinned member is verified")
	selection := ot.groupEvents("selection")
	require.Equal(t, "pin", selection[len(selection)-1]["cause"].(map[string]any)["reason"])
	f.ForceSet("")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "unpinned")
}

func TestOnDemandHandsItsSearchToTheGroupReplacingIt(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "D")
	old := ot.group("g", "A", "B", "C", "D", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	release := ot.runner.holdOn("C")
	ot.signal(old, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("searchStart")) == 1 }, "the search starts")
	searchID := ot.groupEvents("searchStart")[0]["search"]

	ot.coord.BeginPublish()
	fresh := ot.build("g", "A", "B", "C", "D", "REJECT")
	ot.replace(old, fresh)
	close(release)
	ot.eventually(func() bool { return len(ot.groupEvents("searchEnd")) == 1 }, "the new group ends the search")
	end := ot.groupEvents("searchEnd")[0]
	require.Equal(t, searchID, end["search"])
	require.Equal(t, "g", end["group"])
	inherit := ot.groupEvents("inherit")
	require.Len(t, inherit, 1)
	require.Equal(t, searchID, inherit[0]["search"])
}

func TestOnDemandObservesConnections(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 1)
			if _, err := conn.Read(buf); err == nil && buf[0] == 'y' {
				_, _ = conn.Write([]byte("ok"))
			}
			_ = conn.Close()
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	metadata := &C.Metadata{NetWork: C.TCP, DstPort: uint16(addr.Port)}
	require.NoError(t, metadata.SetRemoteAddress(addr.String()))

	// A connection that ends before any data is a strong failure signal.
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	_, err = conn.Write([]byte("n"))
	require.NoError(t, err)
	_, err = conn.Read(make([]byte, 2))
	require.Error(t, err)
	require.NoError(t, conn.Close())
	ot.eventually(func() bool { return len(ot.records("signal")) == 1 }, "the signal is recorded")
	signal := ot.records("signal")[0]
	require.Equal(t, "eof", signal["class"])
	require.Equal(t, "verify", signal["disposition"])

	// A connection that receives data is a success, which renews the use of the group.
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	binder, ok := conn.(C.ActivityBinder)
	require.True(t, ok)
	require.NotNil(t, binder.ActivityBinding())
	_, err = conn.Write([]byte("y"))
	require.NoError(t, err)
	n, err := conn.Read(make([]byte, 2))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, conn.Close())
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return !f.od.member(ot.proxies["A"]).lastSuccessDial.IsZero()
	}, "the success is recorded")
	require.True(t, f.od.state()["inUse"].(bool))

	bindings.Lock()
	live := len(bindings.byGroup[f.od])
	bindings.Unlock()
	require.Zero(t, live, "closed connections release their bindings")
}

func TestOnDemandRecordsSwitchesOfRealDials(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.beforeDial(context.Background(), f.od.selectProxy(nil, true), false)
	ot.signal(f, "A")
	ot.eventually(func() bool { return ot.current(f) == "B" }, "fails over to B")
	ot.eventually(func() bool { return f.Now() == "B" }, "the dial path follows")
	f.od.beforeDial(context.Background(), f.od.selectProxy(nil, true), false)
	ot.eventually(func() bool { return len(ot.records("switch")) == 1 }, "the switch is recorded")
	sw := ot.records("switch")[0]
	require.Equal(t, "A", sw["from"].(map[string]any)["name"])
	require.Equal(t, "B", sw["to"].(map[string]any)["name"])
	// the verification of the route once the group came into use, or of the signal
	require.Contains(t, []string{"verify", "continue"}, sw["cause"].(map[string]any)["reason"])
}

func TestOnDemandRequiresTheProbeOptionOnAFallbackGroup(t *testing.T) {
	option := GroupCommonOption{Name: "s", Type: "select", Probe: ProbeOnDemand}
	require.False(t, checkProbeOption(&option))
	require.Empty(t, option.Probe)
	option = GroupCommonOption{Name: "f", Type: "fallback", Probe: ProbeOnDemand, Interval: 300}
	require.True(t, checkProbeOption(&option))
	require.Zero(t, option.Interval)
}

func (ot *odTest) failed(f *Fallback, member string, started time.Time) probe.Completion {
	p := ot.proxies[member]
	before := ot.coord.Evidence(probe.Key{Target: p, Cond: f.od.cond})
	after := before
	after.State = probe.Dead
	after.Started = started
	after.Version = before.Version + 1000
	return probe.Completion{Exec: 999, Key: probe.Key{Target: p, Cond: f.od.cond}, Started: started, Adopted: true, Before: before, After: after, API: "proxies"}
}

// plainReference refers to one proxy under the test condition of the groups of these tests.
type plainReference struct{ target C.Proxy }

func (r *plainReference) Name() string { return "ref" }
func (r *plainReference) References(target C.Proxy, _ probe.Condition) bool {
	return target == r.target
}
func (r *plainReference) Deliver(probe.Notice) {}

var testCondition = probe.Condition{URL: "http://test/", Expected: "*", Timeout: time.Second}

func TestOnDemandRoutesByKnownEvidenceRightAfterPublishing(t *testing.T) {
	ot := newODTest(t)
	a := ot.node("A", "")
	ot.coord.Register(&plainReference{target: a})
	ot.coord.Activate(nil, nil)
	ot.coord.Manual(context.Background(), a, testCondition, "proxies")
	require.Equal(t, probe.Dead, ot.coord.Evidence(probe.Key{Target: a, Cond: testCondition}).State)
	f := ot.build("g", "A", "B", "REJECT")
	require.Equal(t, "B", f.Now(), "the first dial avoids the member known dead")
	ot.idle(f)
	require.Zero(t, ot.runner.count("B"), "nothing is tested before activation")
}

func TestOnDemandHandlesAFailureBeforeALaterExclusion(t *testing.T) {
	ot := newODTest(t)
	ot.node("A", "jinkela")
	f := ot.build("g", "A", "B", "C", "REJECT")
	require.Equal(t, "A", ot.current(f))
	// The failure of A came before A was excluded: it is a failure of the current member.
	f.od.halt()
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "jinkela"}})
	f.od.drain()
	f.od.round([]any{ot.failed(f, "A", time.Now()), probe.ExclusionsChanged{Table: ot.coord.Exclusions().Current()}}, false)
	require.Len(t, ot.groupEvents("searchStart"), 1)
	require.Equal(t, "B", ot.current(f))
}

func TestOnDemandIgnoresAFailureAfterTheMemberWasExcluded(t *testing.T) {
	ot := newODTest(t)
	ot.node("A", "jinkela")
	f := ot.build("g", "A", "B", "C", "REJECT")
	f.od.halt()
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "jinkela"}})
	f.od.drain()
	f.od.round([]any{probe.ExclusionsChanged{Table: ot.coord.Exclusions().Current()}}, false)
	f.od.round([]any{ot.failed(f, "A", time.Now())}, false)
	require.Empty(t, ot.groupEvents("searchStart"), "A was no longer the current member")
}

func TestOnDemandExplainsALateSignalByALaterTest(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	signalled := time.Now()
	time.Sleep(5 * time.Millisecond)
	ot.coord.Manual(context.Background(), ot.proxies["A"], f.od.cond, "proxies")
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return f.od.view[ot.proxies["A"]].State == probe.Alive
	}, "the test is heard")
	// the signal is handled only now, after the test that started later than it
	f.od.post(signalNote{member: ot.proxies["A"], cond: f.od.cond, network: "tcp", strong: true, class: "eof", at: signalled, dialed: signalled})
	ot.eventually(func() bool { return len(ot.records("obligation")) == 1 }, "the obligation is explained")
	require.Equal(t, "satisfied", ot.records("obligation")[0]["end"])
	ot.idle(f)
	require.Equal(t, 1, ot.runner.count("A"), "no verification for an explained signal")
}

func TestOnDemandRetriesItsPoolOnceTheBackoffEnds(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.noteUse("dial")
	ot.eventually(func() bool { return ot.runner.count("B") == 1 && ot.runner.count("C") == 1 }, "the pool is filled")
	ot.idle(f)
	f.od.mutex.Lock()
	gap, timerAt := f.od.poolGap(f.od.pool), f.od.timerAt
	f.od.mutex.Unlock()
	require.True(t, gap, "the pool still wants members while they back off")
	require.False(t, timerAt.IsZero())
	require.True(t, timerAt.Before(time.Now().Add(probe.BackoffFirst+time.Second)), "woken when the backoff ends")
}

func TestOnDemandVerifiesTheMemberAPinMovesToDuringASearch(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "X")
	f := ot.group("g", "A", "B", "C", "D", "X", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	for _, name := range []string{"B", "C", "D"} {
		ot.runner.holdOn(name)
	}
	ot.signal(f, "A")
	ot.eventually(func() bool { return len(ot.groupEvents("searchBatch")) == 1 }, "the search tests a batch")
	require.NoError(t, f.Set("X"))
	// the test of X in the batch may have started before the pin, and then X is tested again
	ot.eventually(func() bool { return ot.runner.count("X") >= 1 }, "the pinned member is verified while the batch runs")
}

func TestOnDemandRetiringGroupHandlesWhatItHeard(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	ot.coord.BeginPublish()
	fresh := ot.build("g", "B", "C", "REJECT")
	// A failed before the new config, which drops A, was activated; the old group did not
	// get to it yet.
	old.od.halt()
	old.od.post(ot.failed(old, "A", time.Now()))
	ot.replace(old, fresh)
	ot.eventually(func() bool { return len(ot.groupEvents("inherit")) == 1 }, "the new group takes over")
	require.NotNil(t, ot.groupEvents("inherit")[0]["search"], "the search the failure started is inherited")
}

func TestOnDemandBindsASlowDialToTheGroupReplacingItsOwn(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "REJECT")
	fresh := ot.build("g", "A", "REJECT")
	ot.replace(old, fresh)
	b := old.od.bind(ot.proxies["A"], false)
	require.Equal(t, fresh.od, b.target.Load())
	gone := ot.build("h", "A", "REJECT")
	migrateBindings(gone.od, nil)
	require.Nil(t, gone.od.bind(ot.proxies["A"], false).target.Load())
}

func TestOnDemandNormalizesTheExpectedStatus(t *testing.T) {
	require.Equal(t, normalizeExpected("204"), normalizeExpected(" 204 "))
	ot := newODTest(t)
	f := ot.build("g", "A", "REJECT")
	cond := ManualCondition(WithManualTimeout(context.Background(), time.Second), "http://test/", nil)
	require.Equal(t, f.od.cond, cond, "a manual test under the group's condition updates its evidence")
}

func TestGroupDelayTestsThroughTheCoordinator(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	selector, err := NewSelector(GroupCommonOption{Name: "s"}, SelectorOption{}, nil, f.Providers())
	require.NoError(t, err)
	saved := probe.Default
	probe.Default = ot.coord
	defer func() { probe.Default = saved }()
	ctx := WithManualTimeout(context.Background(), time.Second)
	delays, err := GroupManualTest(ctx, selector, "http://test/", nil)
	require.NoError(t, err)
	require.Contains(t, delays, "A")
	require.NotEmpty(t, ot.records("probe"))
	require.Equal(t, "group", ot.records("probe")[0]["manual"])
}

func TestOnDemandForwardsWhatARetiredGroupStillObserves(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	fresh := ot.build("g", "A", "B", "REJECT")
	ot.replace(old, fresh)
	ot.eventually(func() bool { return ot.current(fresh) == "A" }, "the new group selects A")
	// a connection of the old group fails after the handover
	ot.signal(old, "A")
	ot.eventually(func() bool { return len(ot.records("signal")) == 1 }, "the new group hears of it")
	require.Equal(t, "g", ot.records("signal")[0]["group"])
	require.Equal(t, "verify", ot.records("signal")[0]["disposition"])
}

func TestOnDemandCountsOneFalseAlarmPerTest(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	a := ot.proxies["A"]
	early := time.Now()
	time.Sleep(5 * time.Millisecond)
	ot.coord.Manual(context.Background(), a, f.od.cond, "proxies")
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return f.od.view[a].State == probe.Alive
	}, "the test is heard")
	for i := 0; i < 3; i++ {
		f.od.post(signalNote{member: a, cond: f.od.cond, network: "tcp", strong: true, class: "eof", at: early, dialed: early})
		ot.idle(f)
	}
	f.od.mutex.Lock()
	cooldown := f.od.member(a).cooldown
	f.od.mutex.Unlock()
	require.Equal(t, probe.CooldownFirst, cooldown, "the late signals one test explains are one false alarm")
}

func TestOnDemandHandlesAFailureBeforeANestedSelectionChange(t *testing.T) {
	ot := newODTest(t)
	x, y := ot.node("X", ""), ot.node("Y", "jinkela")
	pd, err := provider.NewCompatibleProvider("S", []C.Proxy{x, y}, provider.NewHealthCheck([]C.Proxy{x, y}, "http://test/", 1000, 0, true, nil))
	require.NoError(t, err)
	selector, err := NewSelector(GroupCommonOption{Name: "S"}, SelectorOption{}, nil, []P.ProxyProvider{pd})
	require.NoError(t, err)
	ot.proxies["S"] = adapter.NewProxy(selector)
	f := ot.build("g", "S", "B", "C", "REJECT")
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "jinkela"}})
	ot.eventually(func() bool { return f.od.state()["members"].([]any)[0].(map[string]any)["excluded"] == false }, "S selects X, not excluded")
	require.Equal(t, "S", ot.current(f))
	f.od.halt()
	f.od.drain()
	// S failed, and only after that, it selected an excluded node
	failure := ot.failed(f, "S", time.Now())
	selector.ForceSet("Y")
	f.od.round([]any{failure}, false)
	require.Len(t, ot.groupEvents("searchStart"), 1, "the failure was of the current member")
	require.Equal(t, "B", ot.current(f))
}

func TestOnDemandKeepsSharedEvidenceOfAMemberLeavingAndComingBack(t *testing.T) {
	ot := newODTest(t)
	a := ot.node("A", "")
	ot.coord.Register(&plainReference{target: a})
	ot.coord.Activate(nil, nil)
	f := ot.build("g", "A", "B", "REJECT")
	ot.coord.Manual(context.Background(), a, f.od.cond, "proxies")
	ot.eventually(func() bool { return ot.current(f) == "B" }, "A is known dead")
	f.od.halt()
	f.od.drain()
	f.od.round([]any{topologyNote{members: []C.Proxy{ot.proxies["B"]}}, topologyNote{members: []C.Proxy{a, ot.proxies["B"]}}}, false)
	require.Equal(t, "B", ot.current(f), "the evidence of A is read again when A comes back")
	require.Equal(t, "B", f.Now())
}

func TestOnDemandIgnoresAMemberSetReadBeforeTheLatestTopology(t *testing.T) {
	ot := newODTest(t)
	f := ot.build("g", "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	old := topologyVersion.Load()
	newer := topologyVersion.Add(1)
	f.od.updateMemberSet([]C.Proxy{b}, newer)
	f.od.updateMemberSet([]C.Proxy{a, b}, old)
	require.False(t, f.od.References(a, f.od.cond), "a set read earlier does not bring A back")
}

func TestOnDemandPassesUseOnToTheGroupReplacingIt(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "REJECT")
	fresh := ot.build("g", "A", "REJECT")
	ot.replace(old, fresh)
	before := fresh.od.lastData.Load()
	old.od.noteUse("data")
	require.Greater(t, fresh.od.lastData.Load(), before, "data a connection of the retired group received renews its successor")
}

func TestOnDemandKeepsTheSameSelectionWithConcurrentPins(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		name := []string{"B", "C"}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.ForceSet(name)
		}()
	}
	wg.Wait()
	ot.eventually(func() bool { return ot.current(f) == f.Now() }, "the dial path and the strategy agree")
}

func TestOnDemandKeepsNoMemberOfAnEarlierConfigForTheSwitchRecord(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.beforeDial(context.Background(), f.od.selectProxy(nil, true), false)
	dialed := f.od.dialed.Load()
	require.NotNil(t, dialed)
	require.Equal(t, "A", dialed.name)
}

func TestOnDemandRecordsASwitchAcrossAReloadOnce(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	old.od.beforeDial(context.Background(), ot.proxies["A"], false)
	// the group of the new config starts from what the old one dialed, as a publish does
	fresh := ot.build("g", "B", "REJECT")
	fresh.od.dialed.CompareAndSwap(nil, old.od.dialed.Load())
	b := ot.proxies["B"]
	fresh.od.noteDialedAs(dialedMember{name: "B"}, selectionCause{Reason: causeReload})
	ot.replace(old, fresh)
	ot.eventually(func() bool { return len(ot.groupEvents("inherit")) == 1 }, "the new group takes over")
	fresh.od.beforeDial(context.Background(), b, false)
	old.od.beforeDial(context.Background(), b, false)
	ot.eventually(func() bool { return len(ot.records("switch")) == 1 }, "A to B is recorded")
	time.Sleep(20 * time.Millisecond)
	require.Len(t, ot.records("switch"), 1, "the number B got, and a dial of the retired group, are no switch")
	require.Equal(t, "A", ot.records("switch")[0]["from"].(map[string]any)["name"])
}

func TestOnDemandRecordsAFailureOfAConnectionWhoseMemberLeftAsStale(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	o := f.od.observe(ot.proxies["A"], time.Now(), false)
	o.binding.target.Store(nil) // as when a reload drops A
	o.signal(true, "eof", nil)
	signals := ot.records("signal")
	require.Len(t, signals, 1)
	require.Equal(t, "stale", signals[0]["disposition"])
	require.Equal(t, "g", signals[0]["group"])
}

func TestOnDemandEndsAConnectionInOneObservation(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	o := f.od.observe(ot.proxies["A"], time.Now().Add(-time.Minute), false)
	o.sent.Store(true)
	o.readFailed(io.EOF)
	o.close()
	ot.eventually(func() bool { return len(ot.records("signal")) == 1 }, "the failed read is signaled")
	time.Sleep(20 * time.Millisecond)
	require.Len(t, ot.records("signal"), 1, "closing after it adds no silent signal")
}

func TestOnDemandRevokesTheObligationOfARemovedMember(t *testing.T) {
	ot := newODTest(t)
	ot.runner.holdOn("A")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.signal(f, "A")
	ot.eventually(func() bool { return ot.runner.count("A") == 1 }, "A is verified")
	f.od.post(topologyNote{members: []C.Proxy{ot.proxies["B"], ot.proxies["REJECT"]}})
	ot.eventually(func() bool {
		for _, e := range ot.records("obligation") {
			if e["end"] == "revoked" && e["reason"] == "memberRemoved" {
				return true
			}
		}
		return false
	}, "the obligation ends with A")
}
