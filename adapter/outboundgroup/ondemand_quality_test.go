package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// gateAdapter is a node whose dials take a set time, or fail.
type gateAdapter struct {
	*routeAdapter
	mutex  sync.Mutex
	delay  time.Duration
	fail   bool
	dials  int
	linger time.Duration // how long it takes to give up once canceled
}

func (a *gateAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	a.mutex.Lock()
	delay, fail, linger := a.delay, a.fail, a.linger
	a.dials++
	a.mutex.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			time.Sleep(linger)
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errors.New("refused")
	}
	return a.routeAdapter.DialContext(ctx, metadata)
}

func (a *gateAdapter) set(delay time.Duration, fail bool) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.delay, a.fail = delay, fail
}

func (a *gateAdapter) count() int {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.dials
}

func (ot *odTest) gateNode(name string) *gateAdapter {
	a := &gateAdapter{routeAdapter: &routeAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name})}}
	ot.proxies[name] = adapter.NewProxy(a)
	return a
}

// echo listens on the loopback and answers what it reads.
func echo(t *testing.T) *C.Metadata {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 16)
				if n, err := conn.Read(buf); err == nil {
					_, _ = conn.Write(buf[:n])
				}
				_ = conn.Close()
			}()
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	metadata := &C.Metadata{NetWork: C.TCP, DstPort: uint16(addr.Port)}
	require.NoError(t, metadata.SetRemoteAddress(addr.String()))
	return metadata
}

// use exchanges data over a connection, as real traffic does.
func use(t *testing.T, conn C.Conn) {
	t.Helper()
	_, err := conn.Write([]byte("hi"))
	require.NoError(t, err)
	_, err = conn.Read(make([]byte, 2))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func (ot *odTest) route(f *Fallback) string {
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	if f.od.route == nil {
		return ""
	}
	return f.od.route.Name()
}

func (ot *odTest) standing(f *Fallback, member string) standing {
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	return f.od.standingOf(ot.proxies[member])
}

func (ot *odTest) recheck(f *Fallback) {
	f.od.post(recheck{})
	ot.idle(f)
}

func TestOnDemandDialsBothMembersWhileTheRouteIsNotCertain(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	require.Equal(t, 1, a.count())
	require.Equal(t, 1, b.count(), "the first candidate is dialed alongside an uncertain route")
	ot.eventually(func() bool { return len(ot.groupEvents("hedge")) == 1 }, "the dial is recorded")
	hedge := ot.groupEvents("hedge")[0]
	require.Equal(t, "parallel", hedge["mode"])
	require.Equal(t, "A", hedge["carrier"].(map[string]any)["name"], "the route quick enough carries the dial")
	require.Equal(t, false, hedge["lost"])
}

func TestOnDemandDialsOnlyACertainRouteThatIsQuick(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	a, b := ot.gateNode("A"), ot.gateNode("B")
	f := ot.group("g", "A", "B", "REJECT")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn) // a new connection that received data makes the route certain
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return !f.od.certainUntil(f.od.now()).IsZero()
	}, "the route is certain")
	ot.recheck(f)
	before := b.count()
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	require.Equal(t, 2, a.count())
	require.Equal(t, before, b.count(), "a certain route that is quick dials alone")
}

func TestOnDemandCallsOnACandidateWhenTheRouteHangs(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	a, b := ot.gateNode("A"), ot.gateNode("B")
	f := ot.group("g", "A", "B", "REJECT")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return !f.od.certainUntil(f.od.now()).IsZero()
	}, "the route is certain")
	ot.recheck(f)

	// A hangs: past the fast tier B is dialed and carries the dial.
	hold := ot.runner.holdOn("A")
	defer close(hold)
	a.set(10*time.Second, false)
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err = f.DialContext(ctx, metadata)
	require.NoError(t, err)
	elapsed := time.Since(started)
	require.Less(t, elapsed, 3*time.Second)
	require.GreaterOrEqual(t, elapsed, probe.FastDial-100*time.Millisecond)
	use(t, conn)
	require.GreaterOrEqual(t, b.count(), 2)
	carried := f.od.carried.Load()
	require.Equal(t, "B", carried.name, "the view tells the member that carried the dial")
	require.True(t, carried.hedged)

	// A lost: a slow signal verifies it, and it is set aside until the verification ends.
	ot.eventually(func() bool {
		for _, s := range ot.records("signal") {
			if s["class"] == "slow" {
				return true
			}
		}
		return false
	}, "a slow signal")
	ot.eventually(func() bool { return ot.route(f) == "B" }, "A is set aside, new dials start from B")
	require.Equal(t, "A", ot.current(f), "A stays the preferred member")
	require.NotEmpty(t, ot.groupEvents("suspend"))
	before := a.count()
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	require.Equal(t, before, a.count(), "the suspended member is not dialed")
}

func TestOnDemandDeliversTheFirstToSucceedWhenAllAreSlow(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	a.set(1800*time.Millisecond, false)
	b.set(2500*time.Millisecond, false)
	f := ot.group("g", "A", "B", "REJECT")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool { return len(ot.groupEvents("hedge")) == 1 }, "the dial is recorded")
	hedge := ot.groupEvents("hedge")[0]
	require.Equal(t, "A", hedge["carrier"].(map[string]any)["name"], "past the fast tier the first to succeed carries the dial")
	require.Equal(t, false, hedge["lost"])
	for _, s := range ot.records("signal") {
		require.NotEqual(t, "slow", s["class"], "no member lost")
	}
}

func TestOnDemandMovesOnWhenTheRouteFailsQuickly(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	a.set(0, true)
	f := ot.group("g", "A", "B", "REJECT")
	metadata := echo(t)
	started := time.Now()
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	require.Less(t, time.Since(started), probe.FastDial)
	use(t, conn)
	require.Equal(t, 1, b.count())
	ot.eventually(func() bool {
		for _, s := range ot.records("signal") {
			if s["class"] == "dial" && s["member"].(map[string]any)["name"] == "A" {
				return true
			}
		}
		return false
	}, "the failure of A is a dial signal")
}

// failOnce makes the coordinator find member dead and then alive again by manual tests,
// started at the time of the clock.
func (ot *odTest) failOnce(f *Fallback, member string, clock *fakeClock) {
	ot.t.Helper()
	ot.runner.set(false, member)
	ot.coord.Manual(context.Background(), ot.proxies[member], f.od.cond, "test")
	clock.Advance(time.Minute)
	ot.runner.set(true, member)
	ot.coord.Manual(context.Background(), ot.proxies[member], f.od.cond, "test")
	clock.Advance(time.Minute)
}

type fakeClock struct {
	mutex sync.Mutex
	now   time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.now = c.now.Add(d)
}

func newClockedODTest(t *testing.T) (*odTest, *fakeClock) {
	clock := &fakeClock{now: time.Now()}
	return newODTestAt(t, clock.Now), clock
}

// attribute lets the attribution delay pass and the failures count.
func (ot *odTest) attribute(f *Fallback, clock *fakeClock) {
	clock.Advance(probe.AttributionDelay)
	ot.coord.AttributeDue()
	ot.recheck(f)
}

func TestOnDemandAMemberYieldsAfterRepeatedFailuresAndComesBackOnTrial(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	for i := 0; i < probe.YieldRecent; i++ {
		ot.failOnce(f, "A", clock)
		ot.attribute(f, clock)
		// a second failure within an hour puts it on trial, which the third one fails
	}
	ot.eventually(func() bool { return ot.standing(f, "A") == standingYield }, "A yields")
	ot.eventually(func() bool { return ot.current(f) == "B" }, "B comes first")
	require.Equal(t, []string{"B", "A", "REJECT"}, names(f.od))

	// Once its counts fall and its readmission time comes, it is back on trial, in place.
	clock.Advance(probe.RecentWindow)
	ot.recheck(f)
	ot.eventually(func() bool { return ot.standing(f, "A") == standingTrial }, "A is on trial")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "A is back in place")
	f.od.mutex.Lock()
	require.True(t, f.od.certainUntil(f.od.now()).IsZero(), "a route on trial is not certain")
	f.od.mutex.Unlock()

	// A success after the trial began, and the trial time, restore it.
	ot.coord.Manual(context.Background(), ot.proxies["A"], f.od.cond, "test")
	clock.Advance(probe.TrialLength)
	ot.recheck(f)
	ot.recheck(f)
	ot.eventually(func() bool { return ot.standing(f, "A") == standingNormal }, "A passed its trial")
}

func names(od *onDemand) []string {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	var list []string
	for _, p := range od.order {
		list = append(list, p.Name())
	}
	return list
}

func TestOnDemandAFailedTrialWaitsLongerEachTime(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	// two failures within an hour: on trial when it is back
	ot.failOnce(f, "A", clock)
	ot.attribute(f, clock)
	clock.Advance(20 * time.Minute)
	ot.failOnce(f, "A", clock)
	ot.attribute(f, clock)
	ot.eventually(func() bool { return ot.standing(f, "A") == standingTrial }, "A is on trial after a second failure within an hour")

	// It fails on trial: it yields, and may try again only after the readmission time.
	ot.failOnce(f, "A", clock)
	ot.attribute(f, clock)
	ot.eventually(func() bool { return ot.standing(f, "A") == standingYield }, "the failed trial yields")
	var ended map[string]any
	for _, e := range ot.groupEvents("standing") {
		if e["reason"] == "trialFailed" {
			ended = e
		}
	}
	require.NotNil(t, ended["round"], "the end of a trial tells its round")
	f.od.mutex.Lock()
	q := f.od.quality[ot.proxies["A"]]
	readmit, since, failed := q.readmitAt, q.since, q.failedTrials
	f.od.mutex.Unlock()
	require.Equal(t, 1, failed)
	require.Equal(t, probe.ReadmitFirst, readmit.Sub(since), "the first failed trial waits an hour")

	clock.Advance(probe.ReadmitFirst / 2)
	ot.recheck(f)
	require.Equal(t, standingYield, ot.standing(f, "A"), "not before the readmission time")
}

func TestOnDemandTheStandbysGrowWhileTheyAreNotTrusted(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B", "C", "D", "E")
	f := ot.group("g", "A", "B", "C", "D", "E", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	// B failed once lately: alive, but not a trusted standby
	ot.failOnce(f, "B", clock)
	ot.attribute(f, clock)
	f.od.noteUse("dial")
	ot.recheck(f)
	ot.eventually(func() bool {
		pool := f.od.state()["pool"].([]any)
		return len(pool) == 3
	}, "the pool takes C and D besides B")
	pool := f.od.state()["pool"].([]any)
	require.Equal(t, "B", pool[0].(map[string]any)["name"])
	require.Equal(t, "C", pool[1].(map[string]any)["name"])
	require.Equal(t, "D", pool[2].(map[string]any)["name"])
}

func TestOnDemandAPairOfTestsFindsADegradation(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	ot.runner.mutex.Lock()
	ot.runner.elapsed["A"] = 900 * time.Millisecond
	ot.runner.elapsed["B"] = 100 * time.Millisecond
	ot.runner.mutex.Unlock()
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.noteUse("dial")
	ot.signal(f, "A")
	ot.eventually(func() bool {
		for _, e := range ot.records("performance") {
			if e["event"] == "pair" {
				return true
			}
		}
		return false
	}, "the verification of A is paired with a test of B")
	var pair map[string]any
	for _, e := range ot.records("performance") {
		if e["event"] == "pair" {
			pair = e
		}
	}
	require.Equal(t, true, pair["degraded"])
	clock.Advance(probe.AttributionDelay)
	ot.recheck(f)
	f.od.mutex.Lock()
	counts := f.od.countsOf(ot.proxies["A"], f.od.now())
	f.od.mutex.Unlock()
	require.Equal(t, probe.DegradeWeight, counts.Recent)
}

func TestOnDemandAVerificationSlowerThanItsStandbySetsTheMemberAside(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.noteUse("dial")
	ot.idle(f)
	hold := ot.runner.holdOn("A")
	defer close(hold)
	ot.signal(f, "A")
	ot.eventually(func() bool { return ot.route(f) == "B" }, "A is set aside once its verification runs past the fast tier")
	suspends := ot.groupEvents("suspend")
	require.NotEmpty(t, suspends)
	require.Equal(t, "slowTest", suspends[len(suspends)-1]["reason"])
	require.Equal(t, "A", ot.current(f))
}

func TestOnDemandViewExplainsTheStrategy(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	for i := 0; i < probe.YieldRecent; i++ {
		ot.failOnce(f, "A", clock)
		ot.attribute(f, clock)
	}
	ot.eventually(func() bool { return ot.standing(f, "A") == standingYield }, "A yields")
	view := f.od.strategyView(clock.Now())
	data, err := json.Marshal(view)
	require.NoError(t, err)
	require.Contains(t, string(data), `"type":"badge"`)
	members := view["members"].([]any)
	require.Len(t, members, 3)
	first := members[0].(map[string]any)
	require.Equal(t, "B", first["name"])
	require.Equal(t, 1, first["index"], "the index is the place in the members as listed by the group")
	require.Equal(t, "preferred", first["role"].(map[string]any)["id"])
	yielding := members[1].(map[string]any)
	require.Equal(t, "A", yielding["name"])
	require.Contains(t, string(data), "readmitAt")
	require.Contains(t, string(data), "让位")

	whole, err := json.Marshal(View())
	require.NoError(t, err)
	require.Contains(t, string(whole), `"view":1`)
}

func TestOnDemandACanceledDialDeliversNothing(t *testing.T) {
	ot := newODTest(t)
	a, _ := ot.gateNode("A"), ot.gateNode("B")
	a.set(time.Second, false)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	// B succeeds at once and waits for the route until the fast tier: the caller cancels
	// meanwhile.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	conn, err := f.DialContext(ctx, metadata)
	require.Nil(t, conn)
	require.ErrorIs(t, err, context.Canceled)
	ot.eventually(func() bool { return len(ot.groupEvents("hedge")) == 1 }, "the dial is recorded")
	for _, s := range ot.records("signal") {
		require.NotEqual(t, "slow", s["class"], "a canceled dial decides no loss")
	}
}

func TestOnDemandTheRouteAMemberYieldsToIsVerified(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.failOnce(f, "A", clock)
	ot.attribute(f, clock)
	clock.Advance(20 * time.Minute)
	ot.failOnce(f, "A", clock)
	ot.attribute(f, clock)
	ot.eventually(func() bool { return ot.standing(f, "A") == standingTrial }, "A is on trial")
	ot.failOnce(f, "A", clock)
	from := len(ot.records("need"))
	ot.attribute(f, clock)
	ot.eventually(func() bool { return ot.current(f) == "B" }, "A yields to B")
	ot.eventually(func() bool {
		for _, e := range ot.records("need")[from:] {
			if e["state"] == "created" && e["source"] == probe.SourceContinue && e["target"].(map[string]any)["name"] == "B" {
				return true
			}
		}
		return false
	}, "B is verified once the dials move to it")
}

func TestOnDemandAReplacingGroupMergesWhatBothFound(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	fresh := ot.build("g", "A", "B", "REJECT")
	a := ot.proxies["A"]
	now := old.od.now()
	minute := summaryKey{member: a, minute: now.Unix()/60 + 1}

	old.od.mutex.Lock()
	q := old.od.qualityOf(a)
	q.failedTrials, q.readmitAt = 2, now.Add(time.Hour)
	q.standing = standingYield
	old.od.degradations = []*degradation{{member: a, exec: 7, started: now, commitAt: now.Add(probe.AttributionDelay)}}
	old.od.summaries[minute] = &summary{dialed: 2, delivered: 2, failed: map[string]int{}}
	old.od.mutex.Unlock()
	state := old.od.export()

	fresh.od.mutex.Lock()
	defer fresh.od.mutex.Unlock()
	// what the new group found itself since it was published
	own := fresh.od.qualityOf(a)
	own.standing = standingYield
	own.degradations = []probe.QualityEvent{{At: now.Add(-time.Hour), Weight: probe.DegradeWeight, Exec: 5}}
	fresh.od.degradations = []*degradation{{member: a, exec: 7, started: now, commitAt: now.Add(probe.AttributionDelay)}}
	fresh.od.summaries[minute] = &summary{dialed: 1, failed: map[string]int{"silent": 1}}
	fresh.od.inherit(state, now)

	merged := fresh.od.quality[a]
	require.Equal(t, 2, merged.failedTrials, "the standing handed over stands")
	require.Equal(t, now.Add(time.Hour), merged.readmitAt)
	require.Len(t, merged.degradations, 1, "the degradation the new group found is kept")
	require.Len(t, fresh.od.degradations, 1, "a degradation both groups found counts once")
	s := fresh.od.summaries[minute]
	require.Equal(t, 3, s.dialed)
	require.Equal(t, 2, s.delivered)
	require.Equal(t, 1, s.failed["silent"])
}

func TestOnDemandTheRunsOfPairedTestsAreForgottenInTime(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	now := f.od.now()
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	f.od.cond.Timeout = 2 * time.Minute
	// a verification that runs past the fast tier, and its standby that ends quickly
	f.od.notePairRun(&pairRun{member: a, exec: 1, started: now}, now)
	f.od.notePairRun(&pairRun{member: b, exec: 2, started: now, elapsed: time.Second, ok: true, done: true, standby: true}, now.Add(time.Second))
	// the verification ends ninety seconds later: the standby is still there to pair with
	f.od.notePairRun(&pairRun{member: a, exec: 1, started: now, elapsed: 90 * time.Second, ok: true, done: true}, now.Add(90*time.Second))
	require.True(t, f.od.pairRuns[1].paired)
	// long after, none is left
	f.od.notePairRun(&pairRun{member: b, exec: 3, started: now.Add(time.Hour)}, now.Add(time.Hour))
	require.Len(t, f.od.pairRuns, 1)
}

func TestOnDemandARouteOutOfTimeIsNoLoss(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	a.set(10*time.Second, false)
	b.set(1200*time.Millisecond, false)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	// B carries the dial; A would have to beat twice its time, past the deadline
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := f.DialContext(ctx, metadata)
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool { return len(ot.groupEvents("hedge")) == 1 }, "the dial is recorded")
	require.Equal(t, false, ot.groupEvents("hedge")[0]["lost"])
	for _, s := range ot.records("signal") {
		if s["member"].(map[string]any)["name"] == "A" {
			require.NotContains(t, []any{"slow", "timeout"}, s["class"], "the deadline decides nothing of A")
		}
	}
}

func TestOnDemandAReloadSumsUpTheMembersItDrops(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	fresh := ot.build("g", "B", "REJECT")
	a := ot.proxies["A"]
	old.od.mutex.Lock()
	old.od.summaries[summaryKey{member: a, minute: old.od.now().Unix() / 60}] = &summary{dialed: 1, delivered: 1, firstData: 1, failed: map[string]int{}}
	old.od.mutex.Unlock()
	ot.replace(old, fresh)
	ot.idle(fresh)
	var found bool
	for _, e := range ot.groupEvents("connections") {
		if e["member"].(map[string]any)["name"] == "A" {
			found = true
			require.EqualValues(t, 1, e["dialed"])
		}
	}
	require.True(t, found, "the minute of a member the new group does not have is summed up")
}

func TestOnDemandAReloadKeepsTheObligationOfAStandby(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	fresh := ot.build("g", "A", "B", "REJECT")
	b := ot.proxies["B"]
	now := old.od.now()
	old.od.mutex.Lock()
	ms := old.od.member(b)
	ms.signals, ms.obligationDial = []time.Time{now}, now
	old.od.mutex.Unlock()
	ot.replace(old, fresh)
	// the new group verifies B for it rather than revoking it
	ot.eventually(func() bool {
		for _, e := range ot.records("obligation") {
			if e["member"].(map[string]any)["name"] == "B" && e["end"] == "satisfied" {
				return true
			}
		}
		return false
	}, "the obligation of a standby in range goes on")
	for _, e := range ot.records("obligation") {
		require.NotEqual(t, "revoked", e["end"], e)
	}
}

func TestOnDemandATestHeardBeforeTakingOverCountsForTheTrial(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	fresh := ot.build("g", "A", "B", "REJECT")
	a := ot.proxies["A"]
	now := old.od.now()
	old.od.mutex.Lock()
	q := old.od.qualityOf(a)
	q.standing = standingTrial
	q.trial = &trialState{round: 1, started: now.Add(-time.Minute), pending: map[uint64]time.Time{}}
	old.od.mutex.Unlock()
	// the new group hears of a failure of A before it takes over
	fresh.od.mutex.Lock()
	fresh.od.onCompletion(probe.Completion{
		Exec: 99, Key: probe.Key{Target: a, Cond: fresh.od.cond}, Started: now, Adopted: true,
		Before: probe.Evidence{State: probe.Alive}, After: probe.Evidence{State: probe.Dead},
	}, now)
	fresh.od.mutex.Unlock()
	ot.replace(old, fresh)
	ot.eventually(func() bool {
		fresh.od.mutex.Lock()
		defer fresh.od.mutex.Unlock()
		q := fresh.od.quality[a]
		if q == nil || q.trial == nil {
			return false
		}
		_, pending := q.trial.pending[99]
		return pending
	}, "the failure waits for its attribution in the trial taken over")
}

func TestOnDemandEachDegradationOfATrialIsAttributed(t *testing.T) {
	ot, clock := newClockedODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	now := f.od.now()
	f.od.mutex.Lock()
	run := func(member C.Proxy, exec uint64, started time.Time, elapsed time.Duration, standby bool) *pairRun {
		return &pairRun{member: member, exec: exec, started: started, elapsed: elapsed, ok: true, done: true, standby: standby}
	}
	f.od.comparePair(run(a, 1, now, 5*time.Second, false), run(b, 2, now, 100*time.Millisecond, true), now)
	q := f.od.qualityOf(a)
	q.standing = standingTrial
	q.trial = &trialState{round: 1, started: now.Add(time.Minute), pending: map[uint64]time.Time{}}
	later := now.Add(2 * time.Minute)
	f.od.comparePair(run(a, 3, later, 5*time.Second, false), run(b, 4, later, 100*time.Millisecond, true), later)
	require.Len(t, f.od.degradations, 2, "each is attributed")
	_, pending := q.trial.pending[3]
	require.True(t, pending, "the one within the trial waits")
	f.od.mutex.Unlock()

	clock.Advance(probe.AttributionDelay + 3*time.Minute)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	f.od.commitDegradations(f.od.now())
	require.Len(t, q.degradations, 1, "within DegradeMerge they count once")
	require.Empty(t, q.trial.pending)
	require.True(t, q.trial.failed)
}

func TestOnDemandARouteThatGivesUpLateAfterTheDeadlineIsNoLoss(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	a.set(10*time.Second, false)
	a.mutex.Lock()
	a.linger = 700 * time.Millisecond
	a.mutex.Unlock()
	b.set(1200*time.Millisecond, false)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := f.DialContext(ctx, metadata)
	require.NoError(t, err)
	use(t, conn)
	require.Eventually(t, func() bool { return len(ot.groupEvents("hedge")) == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, false, ot.groupEvents("hedge")[0]["lost"])
	for _, s := range ot.records("signal") {
		if s["member"].(map[string]any)["name"] == "A" {
			require.NotContains(t, []any{"slow", "timeout"}, s["class"])
		}
	}
}

func TestOnDemandAStandbyWithAFailureOfItsOwnIsVerifiedForIt(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	a, b := ot.proxies["A"], ot.proxies["B"]
	now := f.od.now()
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	// A is to be verified, and B failed carrying a dial of the fast tier: B is the standby
	// tested alongside, and has an obligation of its own
	for _, p := range []C.Proxy{a, b} {
		ms := f.od.member(p)
		ms.signals, ms.obligationDial, ms.cooldownUntil = []time.Time{now}, now, time.Time{}
	}
	var need *probe.Need
	for _, n := range f.od.deriveLocked(now) {
		if n.Target == b {
			n := n
			need = &n
		}
	}
	require.NotNil(t, need)
	require.Equal(t, probe.SourceVerify, need.Source, "the need of B itself stands")
	require.True(t, f.od.askedAsStandby(b, now), "and the test serves as the standby")
	// A ends first and the next derivation asks for no standby: B is still paired
	f.od.states[a].signals = nil
	f.od.deriveLocked(now)
	require.True(t, f.od.askedAsStandby(b, now))
}

func TestOnDemandATrialBegunInTheHandoverGoesOn(t *testing.T) {
	ot := newODTest(t)
	old := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	fresh := ot.build("g", "A", "B", "REJECT")
	a := ot.proxies["A"]
	now := fresh.od.now()
	fresh.od.mutex.Lock()
	q := fresh.od.qualityOf(a)
	q.standing, q.since = standingTrial, now
	q.trial = &trialState{round: 7, started: now, pending: map[uint64]time.Time{}}
	fresh.od.mutex.Unlock()
	ot.replace(old, fresh)
	ot.idle(fresh)
	require.Equal(t, standingTrial, ot.standing(fresh, "A"), "the trial it began on what only it heard stands")
}

func TestOnDemandTheTrialClockStopsWhenTheNetworkSeemedDown(t *testing.T) {
	ot, clock := newClockedODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	a := ot.proxies["A"]
	t0 := f.od.now()
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	q := f.od.qualityOf(a)
	q.standing, q.since = standingTrial, t0
	q.trial = &trialState{round: 1, started: t0, resumed: t0, pending: map[uint64]time.Time{}}
	// both notices handled in one round, long after the network seemed down
	clock.Advance(29 * time.Minute)
	now := f.od.now()
	f.od.handle(probe.NetworkChanged{State: probe.NetworkDown, Since: t0.Add(time.Minute)}, now)
	f.od.handle(probe.NetworkChanged{State: probe.NetworkNormal, Since: t0.Add(28 * time.Minute)}, now)
	f.od.advanceTrial(a, q.trial, now)
	require.Equal(t, 2*time.Minute, q.trial.effective)
}

func TestOnDemandAHeldCandidateCarriesTheDialAtTheFastTier(t *testing.T) {
	ot := newODTest(t)
	a, b, c := ot.gateNode("A"), ot.gateNode("B"), ot.gateNode("C")
	a.set(10*time.Second, false)
	b.set(100*time.Millisecond, false)
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	require.Equal(t, 0, c.count(), "no third member while one that succeeded waits")
}

func TestOnDemandTheDeadlineBeforeTheFastTierDecidesNoLoss(t *testing.T) {
	ot := newODTest(t)
	a, b := ot.gateNode("A"), ot.gateNode("B")
	a.set(10*time.Second, false)
	b.set(100*time.Millisecond, false)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := f.DialContext(ctx, metadata)
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool { return len(ot.groupEvents("hedge")) == 1 }, "the dial is recorded")
	require.Equal(t, false, ot.groupEvents("hedge")[0]["lost"])
	for _, s := range ot.records("signal") {
		if s["member"].(map[string]any)["name"] == "A" {
			require.NotContains(t, []any{"slow", "timeout"}, s["class"])
		}
	}
}

func TestOnDemandTheCountsOfATrialAreNoFailedTrial(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	a := ot.proxies["A"]
	now := f.od.now()
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	q := f.od.qualityOf(a)
	q.standing, q.since = standingTrial, now
	q.trial = &trialState{round: 1, started: now, resumed: now, pending: map[uint64]time.Time{}}
	// anomalies from before the trial reach the threshold; the trial itself failed on none
	for i := 0; i < 2*probe.YieldRecent; i++ {
		q.degradations = append(q.degradations, probe.QualityEvent{At: now.Add(-time.Duration(i+1) * probe.DegradeMerge), Weight: probe.DegradeWeight, Exec: uint64(100 + i)})
	}
	f.od.updateStandings(now)
	require.Equal(t, standingYield, q.standing)
	require.Equal(t, 0, q.failedTrials)
	events := ot.groupEvents("standing")
	require.EqualValues(t, 1, events[len(events)-1]["round"], "the end of the trial tells its round")
}

func TestOnDemandASuspensionNothingWillEndIsLifted(t *testing.T) {
	ot := newODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	a := ot.proxies["A"]
	now := f.od.now()
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	f.od.suspend(&suspendState{member: a, since: now, reason: "loss", after: now}, now)
	require.NotNil(t, f.od.suspended)
	// the obligation it waited for went: a newer connection through A succeeded
	f.od.updateSuspend(now)
	require.Nil(t, f.od.suspended)
}

func TestOnDemandADeadlineCallsOnNoMoreCandidates(t *testing.T) {
	ot := newODTest(t)
	a, b, c := ot.gateNode("A"), ot.gateNode("B"), ot.gateNode("C")
	a.set(10*time.Second, false)
	b.set(10*time.Second, false)
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := f.DialContext(ctx, metadata)
	require.Error(t, err)
	ot.idle(f)
	require.Equal(t, 0, c.count(), "nothing new is dialed once the dial is out of time")
}
