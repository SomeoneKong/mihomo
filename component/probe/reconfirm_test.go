package probe

import (
	"context"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// lastCompletion is the latest completion a group heard of a target.
func lastCompletion(t *testing.T, g *recorder, p C.Proxy) Completion {
	t.Helper()
	list := g.completions()
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Key.Target == p {
			return list[i]
		}
	}
	t.Fatalf("no completion of %s", p.Name())
	return Completion{}
}

// endPoor ends the poor network the test made, as clean checks in a row do: the checks find
// the network clean from then on.
func endPoor(t *testing.T, c *Coordinator, hops *switchHop, at time.Time) {
	t.Helper()
	hops.set("clean")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.NotEqual(t, NetworkNormal, c.network.state)
	c.endNetworkLocked("recovered", at)
}

// failedInPoor sets up a poor network with the members failing within it: x makes it poor, and
// each of failing fails while it lasts.
func failedInPoor(t *testing.T, failing ...C.Proxy) (*Coordinator, *fakeRunner, *fakeClock, *switchHop, *Registration, *recorder) {
	t.Helper()
	c, runner, clock := newClockedCoordinator(t)
	hops := &switchHop{mode: "lossy"}
	setHops(c, hops.dial)
	x := node("x")
	reg, g, _, _ := twoEntries(t, c, runner, append([]C.Proxy{x}, failing...)...)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x)
	settle(t, c)
	require.Equal(t, NetworkPoor, networkState(c))
	for i, p := range failing {
		clock.Advance(time.Minute)
		testOnce(t, reg, g, uint64(3+i), p)
		require.False(t, lastCompletion(t, g, p).After.State == Alive)
	}
	return c, runner, clock, hops, reg, g
}

func TestAPoolOutLongerThanThePoorNetworkCountsOnceForThePool(t *testing.T) {
	a, b := pooledNode("pa", "jk", ""), pooledNode("pb", "jk", "")
	c, runner, clock, hops, reg, g := failedInPoor(t, a, b)
	inPoor := []uint64{lastCompletion(t, g, a).Exec, lastCompletion(t, g, b).Exec}
	clock.Advance(time.Minute)
	endPoor(t, c, hops, clock.Now())

	// both are still out: the first failure of each found since counts anew
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 10, a)
	first := lastCompletion(t, g, a)
	require.True(t, first.Charged)
	require.Equal(t, first.Before.DeadSince, first.After.DeadSince, "the same time dead goes on")
	require.Empty(t, c.Outages(), "one failure suggests no outage")
	clock.Advance(3 * time.Minute)
	testOnce(t, reg, g, 11, b)
	require.True(t, lastCompletion(t, g, b).Charged)
	second := lastCompletion(t, g, b)
	since := []uint64{first.Exec, second.Exec}
	require.Len(t, c.Outages(), 1, "the two failures found since suggest it")
	// failing again counts no more
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 12, a)
	require.False(t, lastCompletion(t, g, a).Charged)

	// a comes back: the outage is established, and counted once for the pool
	runner.set(a, true)
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 13, a)
	settle(t, c)
	require.Empty(t, c.Outages())
	// counted once its latest witness is due, not its first
	clock.Advance(first.Started.Add(AttributionDelay + time.Second).Sub(clock.Now()))
	c.attributeNow()
	require.Zero(t, c.PoolCounts("jk", testCond).Long)
	clock.Advance(second.Started.Add(AttributionDelay).Sub(clock.Now()))
	c.attributeNow()
	require.Equal(t, 1.0, c.PoolCounts("jk", testCond).Long)
	require.Equal(t, Counts{}, c.MemberCounts(Key{a, testCond}))
	require.Equal(t, Counts{}, c.MemberCounts(Key{b, testCond}))
	outcomes := map[uint64]string{}
	for _, at := range g.attributed() {
		outcomes[at.Exec] = at.Outcome
	}
	for _, exec := range inPoor {
		require.Equal(t, OutcomeLocal, outcomes[exec], "the failures in the poor network stay the local network's")
	}
	for _, exec := range since {
		require.Equal(t, OutcomePool, outcomes[exec], "those found since are explained by the outage")
	}
}

func TestAMemberOutLongerThanThePoorNetworkCountsOnce(t *testing.T) {
	m := node("m")
	c, _, clock, hops, reg, g := failedInPoor(t, m)
	clock.Advance(time.Minute)
	endPoor(t, c, hops, clock.Now())
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 10, m)
	require.True(t, lastCompletion(t, g, m).Charged)
	for i := 0; i < 3; i++ {
		clock.Advance(time.Hour)
		testOnce(t, reg, g, uint64(11+i), m)
		require.False(t, lastCompletion(t, g, m).Charged, "dead since, it counts once")
	}
	c.attributeNow()
	require.Equal(t, 1.0, c.MemberCounts(Key{m, testCond}).Long)
}

func TestAMemberBackAfterThePoorNetworkCountsNothing(t *testing.T) {
	m := node("m")
	c, runner, clock, hops, reg, g := failedInPoor(t, m)
	clock.Advance(time.Minute)
	endPoor(t, c, hops, clock.Now())
	runner.set(m, true)
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 10, m)
	require.Equal(t, Alive, lastCompletion(t, g, m).After.State)
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Equal(t, Counts{}, c.MemberCounts(Key{m, testCond}))
}

func TestATestStartedInThePoorNetworkConfirmsNothing(t *testing.T) {
	m := node("m")
	c, runner, clock, hops, reg, g := failedInPoor(t, m)
	hold := runner.holdOn(m)
	clock.Advance(time.Minute)
	before := len(g.completions())
	reg.Submit(10, []Need{{Target: m, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()}})
	require.Eventually(t, func() bool { return runner.count(m) == 2 }, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	endPoor(t, c, hops, clock.Now())
	close(hold)
	require.Eventually(t, func() bool { return len(g.completions()) > before }, time.Second, time.Millisecond)
	require.False(t, lastCompletion(t, g, m).Charged, "it started while the network was poor")
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 11, m)
	require.True(t, lastCompletion(t, g, m).Charged)
}

func TestReconfirmingFollowsTheLatestFailureTakenToAttribution(t *testing.T) {
	c, _ := newTestCoordinator(t)
	start := time.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.network.past = []interval{{start: start, end: start.Add(10 * time.Minute)}}
	inside, after := start.Add(time.Minute), start.Add(20*time.Minute)
	ev := &Evidence{State: Dead, charged: inside, chargedExec: 1}
	require.True(t, c.reconfirmsLocked(ev, after))
	require.False(t, c.reconfirmsLocked(ev, start.Add(5*time.Minute)), "started within the coverage")
	require.False(t, c.reconfirmsLocked(&Evidence{State: Dead, charged: start.Add(-time.Minute)}, after), "the failure before counted")
	require.False(t, c.reconfirmsLocked(&Evidence{State: Dead}, after), "nothing taken to attribution")
	// the coverage forgotten, what the attribution found still tells
	c.network.past = nil
	require.False(t, c.reconfirmsLocked(ev, after))
	ev.chargedLocal = true
	require.True(t, c.reconfirmsLocked(ev, after))
}

// failedInPastCoverage makes a fail under cond while the local network did not seem normal,
// which it then ended.
func failedInPastCoverage(c *Coordinator, clock *fakeClock, a C.Proxy, cond Condition) {
	e := startManual(c, a, cond)
	clock.Advance(5 * time.Millisecond)
	c.mutex.Lock()
	c.network.past = []interval{{start: e.started.Add(-time.Minute), end: clock.Now()}}
	c.mutex.Unlock()
	clock.Advance(time.Millisecond)
	c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: 6 * time.Millisecond}, nil)
	clock.Advance(2 * time.Minute)
}

func startManual(c *Coordinator, a C.Proxy, cond Condition) *execution {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.newExecutionLocked(Key{a, cond}, ClassVerify, c.now(), "proxies")
}

func poorFrom(c *Coordinator, at time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.network.state = NetworkPoor
	c.network.start, c.network.since, c.network.confirmed, c.network.activity = at, at, at, at
}

func TestAFailureConfirmedAgainCountsForThePoolUnderAnotherTimeout(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
	short, long := testCond, testCond
	long.Timeout = 2 * short.Timeout
	g1 := &conditionedRecorder{newRecorder("short", a), short}
	g2 := &conditionedRecorder{newRecorder("long", b), long}
	c.Register(g1)
	c.Register(g2)
	c.Activate([]Key{{a, short}, {b, long}}, nil)
	failedInPastCoverage(c, clock, b, long)
	c.Manual(context.Background(), b, long, "proxies")
	require.True(t, lastCompletion(t, g2.recorder, b).Charged)
	clock.Advance(time.Second)
	// a timeout under the shorter condition tells nothing under the longer one
	e := startManual(c, a, short)
	c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: short.Timeout}, nil)
	found := false
	for _, o := range c.Outages() {
		found = found || o.Cond == short
	}
	require.True(t, found, "B failing again under the longer condition counts under the shorter one too")
}

func TestOneTestConfirmsAgainUnderEachConditionAlike(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	first, other := testCond, testCond
	other.Timeout = 20 * time.Second
	g1 := &conditionedRecorder{newRecorder("own", a), first}
	g2 := &conditionedRecorder{newRecorder("other", a), other}
	c.Register(g1)
	c.Register(g2)
	c.Activate([]Key{{a, first}, {a, other}}, nil)
	failedInPastCoverage(c, clock, a, first)
	e := startManual(c, a, first)
	clock.Advance(100 * time.Millisecond)
	// the network seems poor again while the test runs: what it tells may move the coverage
	// back to it, which decides nothing for the other key
	poorFrom(c, clock.Now())
	c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: 200 * time.Millisecond}, nil)
	require.True(t, lastCompletion(t, g1.recorder, a).Charged)
	require.True(t, lastCompletion(t, g2.recorder, a).Charged)
}

func TestAFailureOfAKeyReadAgainAfterAReloadCountsNothing(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	cond, other := testCond, testCond
	other.URL = "http://different/"
	reg := func(co Condition) *conditionedRecorder {
		g := &conditionedRecorder{newRecorder("g", a), co}
		c.Register(g)
		c.Activate([]Key{{a, co}}, []C.Proxy{a})
		return g
	}
	g := reg(cond)
	failedInPastCoverage(c, clock, a, cond)
	c.Manual(context.Background(), a, cond, "proxies")
	require.True(t, lastCompletion(t, g.recorder, a).Charged)
	c.BeginPublish()
	reg(other)
	c.BeginPublish()
	reg(cond)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	require.Zero(t, c.MemberCounts(Key{a, cond}).Long, "the key read again starts afresh")
}

func TestATimeNotNormalLongAgoStillExplainsItsFailures(t *testing.T) {
	a := node("A")
	c, _, clock, _, reg, g := failedInPoor(t, a)
	old := lastCompletion(t, g, a)
	// paused past the long window: the time not normal ends as it expired
	clock.Advance(LongWindow + LocalConfirm + time.Minute)
	c.AttributeDue()
	outcomes := map[uint64]string{}
	for _, at := range g.attributed() {
		outcomes[at.Exec] = at.Outcome
	}
	require.Equal(t, OutcomeLocal, outcomes[old.Exec])
	testOnce(t, reg, g, 15, a)
	require.True(t, lastCompletion(t, g, a).Charged)
}

func TestATestStartedInATimeNotNormalLongAgoConfirmsNothing(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	cond := testCond
	cond.Timeout = 20 * time.Minute
	g := &conditionedRecorder{newRecorder("g", a), cond}
	c.Register(g)
	c.Activate([]Key{{a, cond}}, nil)
	failedInPastCoverage(c, clock, a, cond)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	require.True(t, c.Evidence(Key{a, cond}).chargedLocal)
	poorFrom(c, clock.Now())
	clock.Advance(time.Second)
	e := startManual(c, a, cond)
	clock.Advance(LongWindow + LocalConfirm + time.Minute)
	c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: cond.Timeout}, nil)
	require.False(t, lastCompletion(t, g.recorder, a).Charged, "it started while the network was poor")
	clock.Advance(time.Second)
	c.Manual(context.Background(), a, cond, "proxies")
	require.True(t, lastCompletion(t, g.recorder, a).Charged)
}

func TestEarlierNetworkCoverageSurvivesUntilFailureIsAttributed(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	key := Key{a, testCond}
	g := &conditionedRecorder{newRecorder("g", a), testCond}
	c.Register(g)
	c.Activate([]Key{key}, nil)

	failedInPastCoverage(c, clock, a, testCond)
	first := lastCompletion(t, g.recorder, a)
	poorFrom(c, clock.Now())
	clock.Advance(LongWindow + time.Hour)
	c.AttributeDue()
	require.Equal(t, []Attributed{{Key: key, Exec: first.Exec, Started: first.Started, Outcome: OutcomeLocal}}, g.attributed())
	ev := c.Evidence(key)
	require.Equal(t, first.Started, ev.charged)
	require.True(t, ev.chargedLocal)

	// Once the attribution is in, the next prune can forget the interval.
	poorFrom(c, clock.Now())
	clock.Advance(time.Second)
	c.mutex.Lock()
	c.endNetworkLocked("recovered", clock.Now())
	retained := c.network.covers(first.Started)
	c.mutex.Unlock()
	require.False(t, retained)
	c.Manual(context.Background(), a, testCond, "proxies")
	retry := lastCompletion(t, g.recorder, a)
	require.True(t, retry.Charged)
	ev = c.Evidence(key)
	require.Equal(t, retry.Started, ev.charged)
	require.False(t, ev.chargedLocal)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	require.Equal(t, 1.0, c.MemberCounts(key).Long)
}

func TestEarlierNetworkCoverageSurvivesUntilRunningRetryFinishes(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	key := Key{a, testCond}
	g := &conditionedRecorder{newRecorder("g", a), testCond}
	c.Register(g)
	c.Activate([]Key{key}, nil)

	failedInPastCoverage(c, clock, a, testCond)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	first := c.Evidence(key)
	require.True(t, first.chargedLocal)
	poorFrom(c, clock.Now())
	clock.Advance(time.Second)
	inside := startManual(c, a, testCond)
	clock.Advance(time.Second)
	c.mutex.Lock()
	c.endNetworkLocked("recovered", clock.Now())
	c.mutex.Unlock()
	clock.Advance(time.Minute)
	poorFrom(c, clock.Now())
	clock.Advance(LongWindow + time.Hour)

	c.finish(inside, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: testCond.Timeout}, nil)
	retry := lastCompletion(t, g.recorder, a)
	require.Equal(t, inside.id, retry.Exec)
	require.True(t, retry.Adopted)
	require.False(t, retry.Charged)
	ev := c.Evidence(key)
	require.Equal(t, first.chargedExec, ev.chargedExec)
	require.True(t, ev.chargedLocal)

	clock.Advance(time.Second)
	c.Manual(context.Background(), a, testCond, "proxies")
	outside := lastCompletion(t, g.recorder, a)
	require.True(t, outside.Charged)
	ev = c.Evidence(key)
	require.Equal(t, outside.Exec, ev.chargedExec)
	require.False(t, ev.chargedLocal)
	clock.Advance(time.Second)
	c.Manual(context.Background(), a, testCond, "proxies")
	require.False(t, lastCompletion(t, g.recorder, a).Charged)
}

func TestFailuresConfirmedAgainTooFarApartCountForTheMembers(t *testing.T) {
	a, b := pooledNode("pa", "jk", ""), pooledNode("pb", "jk", "")
	c, _, clock, hops, reg, g := failedInPoor(t, a, b)
	clock.Advance(time.Minute)
	endPoor(t, c, hops, clock.Now())
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 10, a)
	require.True(t, lastCompletion(t, g, a).Charged)
	clock.Advance(11 * time.Minute)
	testOnce(t, reg, g, 11, a)
	require.False(t, lastCompletion(t, g, a).Charged, "failing again is no new failure")
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 12, b)
	require.True(t, lastCompletion(t, g, b).Charged)
	require.Empty(t, c.Outages(), "the failures found since are 12 minutes apart")
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Zero(t, c.PoolCounts("jk", testCond).Long)
	require.Equal(t, 1.0, c.MemberCounts(Key{a, testCond}).Long)
	require.Equal(t, 1.0, c.MemberCounts(Key{b, testCond}).Long)
}
