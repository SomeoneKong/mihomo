package probe

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

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

func newClockedCoordinator(t *testing.T) (*Coordinator, *fakeRunner, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Now()}
	runner := newFakeRunner()
	return New(clock.Now, runner.run), runner, clock
}

func (r *recorder) attributed() []Attributed {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var list []Attributed
	for _, n := range r.notices {
		if a, ok := n.(Attributed); ok {
			list = append(list, a)
		}
	}
	return list
}

func (r *recorder) has(match func(Notice) bool) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, n := range r.notices {
		if match(n) {
			return true
		}
	}
	return false
}

// testOnce runs one verification of each target and waits for their results.
func testOnce(t *testing.T, reg *Registration, g *recorder, version uint64, targets ...C.Proxy) {
	t.Helper()
	before := len(g.completions())
	var needs []Need
	for _, p := range targets {
		needs = append(needs, Need{Target: p, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: reg.coordinator.now()})
	}
	reg.Submit(version, needs)
	require.Eventually(t, func() bool { return len(g.completions()) >= before+len(targets) }, 2*time.Second, time.Millisecond)
}

func (c *Coordinator) attributeNow() {
	c.AttributeDue()
}

func TestAFailureCountsForTheMemberAfterTheAttributionDelay(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, x)
	key := Key{x, testCond}
	c.attributeNow()
	require.Empty(t, g.attributed(), "it waits for the attribution delay")
	require.Equal(t, Counts{}, c.MemberCounts(key))
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Len(t, g.attributed(), 1)
	require.Equal(t, OutcomeMember, g.attributed()[0].Outcome)
	require.Equal(t, Counts{Recent: 1, Long: 1}, c.MemberCounts(key))

	// Tests that find it dead again do not count again.
	clock.Advance(BackoffMax)
	testOnce(t, reg, g, 2, x)
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Len(t, g.attributed(), 1)

	clock.Advance(RecentWindow)
	require.Equal(t, Counts{Recent: 0, Long: 1}, c.MemberCounts(key))
}

func TestFailuresWhileTheNetworkSeemsDownDoNotCount(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	alive, x, y, z := node("alive"), node("x"), node("y"), node("z")
	var checks sync.Mutex
	checked := 0
	c.firstHop = func(context.Context, C.Proxy) (time.Duration, error) {
		checks.Lock()
		defer checks.Unlock()
		checked++
		return 0, errors.New("unreachable")
	}
	g := newRecorder("g", alive, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { state, _ := c.Network(); return state == NetworkDown }, time.Second, time.Millisecond)
	require.True(t, g.has(func(n Notice) bool { nc, ok := n.(NetworkChanged); return ok && nc.State == NetworkDown }))

	// A failure while it seems down does not lengthen the backoff.
	ev := c.Evidence(Key{x, testCond})
	require.Equal(t, Dead, ev.State)

	clock.Advance(AttributionDelay)
	c.attributeNow()
	for _, a := range g.attributed() {
		require.Equal(t, OutcomeLocal, a.Outcome, a.Key.Target.Name())
	}
	require.Equal(t, Counts{}, c.MemberCounts(Key{x, testCond}))

	// The tests are throttled: one at a time, one each interval.
	holdX, holdY := runner.holdOn(x), runner.holdOn(y)
	defer close(holdX)
	defer close(holdY)
	before := runner.count(x) + runner.count(y) + runner.count(z)
	reg.Submit(3, []Need{
		{Target: x, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover},
		{Target: y, Cond: testCond, Class: ClassMaintain, Rank: 41, Source: SourceRecover},
		{Target: z, Cond: testCond, Class: ClassMaintain, Rank: 42, Source: SourceRecover},
	})
	require.Eventually(t, func() bool { return runner.count(x)+runner.count(y)+runner.count(z) == before+1 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	c.kick()
	require.Equal(t, before+1, runner.count(x)+runner.count(y)+runner.count(z), "one at a time")

	// A success ends it, and the members found dead meanwhile are tested again at once.
	runner.set(alive, true)
	c.NoteNewSuccess(alive, clock.Now())
	state, _ := c.Network()
	require.Equal(t, NetworkNormal, state)
	ev = c.Evidence(Key{z, testCond})
	require.False(t, clock.Now().Before(ev.NextAllowed))
}

func TestAnOutageOfAPoolMergesTheRecoveryOfItsNodes(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, healthy := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	g := newRecorder("g", a, b, healthy)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a)
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 2, b)
	outages := c.Outages()
	require.Len(t, outages, 1)
	require.Equal(t, OutageSuspected, outages[0].State)
	require.Equal(t, "jk", outages[0].Pool)

	// The failures wait for the outage to be concluded, however long it takes.
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Empty(t, g.attributed())

	// Only one of the dead nodes is tested again at a time.
	holdA, holdB := runner.holdOn(a), runner.holdOn(b)
	before := runner.count(a) + runner.count(b)
	reg.Submit(3, []Need{
		{Target: a, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover},
		{Target: b, Cond: testCond, Class: ClassMaintain, Rank: 41, Source: SourceRecover},
	})
	require.Eventually(t, func() bool { return runner.count(a)+runner.count(b) == before+1 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, before+1, runner.count(a)+runner.count(b))

	// The node tested comes back: the outage is established, and counts once for the pool.
	runner.set(a, true)
	runner.set(b, true)
	close(holdA)
	close(holdB)
	require.Eventually(t, func() bool { return len(c.Outages()) == 0 }, time.Second, time.Millisecond)
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageEstablished }))
	require.Equal(t, Counts{Recent: 1, Long: 1}, c.PoolCounts("jk", testCond))
	require.Len(t, g.attributed(), 2)
	for _, attributed := range g.attributed() {
		require.Equal(t, OutcomePool, attributed.Outcome)
	}
	require.Equal(t, Counts{}, c.MemberCounts(Key{a, testCond}))
}

func TestAHealthyNodeDisprovesAnOutage(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, healthy := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	g := newRecorder("g", a, b, healthy)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	runner.set(healthy, true)
	testOnce(t, reg, g, 2, healthy)
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageDisproved }))
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Len(t, g.attributed(), 2)
	for _, attributed := range g.attributed() {
		require.Equal(t, OutcomeMember, attributed.Outcome)
	}
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
}

func TestANewConnectionThroughADeadNodeEstablishesAnOutage(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	clock.Advance(time.Minute)
	c.NoteNewSuccess(b, clock.Now())
	require.Empty(t, c.Outages())
	require.Equal(t, Counts{Recent: 1, Long: 1}, c.PoolCounts("jk", testCond))
}

func TestTheBackoffOfALongOutageGrowsToTwoHours(t *testing.T) {
	require.Equal(t, BackoffMaxLong, backoffUpTo(100, BackoffMaxLong))
	c, _, clock := newClockedCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, x)
	since := c.Evidence(Key{x, testCond}).DeadSince
	for i := uint64(2); i < 14; i++ {
		clock.Advance(BackoffMaxLong)
		testOnce(t, reg, g, i, x)
	}
	ev := c.Evidence(Key{x, testCond})
	require.Equal(t, since, ev.DeadSince)
	require.True(t, ev.Outage(clock.Now()) > LongOutageAfter)
	require.Equal(t, clock.Now().Add(BackoffMaxLong), ev.NextAllowed)
	require.True(t, c.Persistent(Key{x, testCond}))
}

func TestATestStillRunningAtTheFastTierIsAnnounced(t *testing.T) {
	c, runner := newTestCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	hold := runner.holdOn(x)
	defer close(hold)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: time.Now()}})
	require.Eventually(t, func() bool {
		return g.has(func(n Notice) bool { r, ok := n.(Running); return ok && r.Key.Target == x && len(r.Requests) == 1 })
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 500*time.Millisecond, FastProbeFor(time.Second))
	require.Equal(t, FastProbe, FastProbeFor(10*time.Second))
}

func TestAnEpisodeTellsHowLongTheMemberWasDead(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, x)
	clock.Advance(2 * time.Hour)
	runner.set(x, true)
	testOnce(t, reg, g, 2, x)
	completions := g.completions()
	episode := completions[len(completions)-1].Episode
	require.NotNil(t, episode)
	require.Equal(t, 2*time.Hour, episode.Duration)
	require.True(t, episode.Persistent())
}

func failingFirstHop(context.Context, C.Proxy) (time.Duration, error) {
	return 0, errors.New("unreachable")
}

func networkState(c *Coordinator) NetworkState {
	state, _ := c.Network()
	return state
}

func TestTheNetworkIsCheckedThroughProxiesThatWorkedLately(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	c.firstHop = failingFirstHop
	x, y, z := node("x"), node("y"), node("z")
	g := newRecorder("g", x, y, z)
	reg := activeRegistration(c, g)
	for _, p := range []C.Proxy{x, y, z} {
		runner.set(p, true)
	}
	testOnce(t, reg, g, 1, x, y, z)
	clock.Advance(time.Second)
	for _, p := range []C.Proxy{x, y, z} {
		runner.set(p, false)
	}
	// All of them are dead once the third fails: they still worked lately.
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
}

func TestACheckOutrunByASuccessChangesNothing(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	release := make(chan struct{})
	c.firstHop = func(context.Context, C.Proxy) (time.Duration, error) {
		<-release
		return 0, errors.New("unreachable")
	}
	alive, x, y, z := node("alive"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	checking := func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.network.checking
	}
	require.Eventually(t, checking, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	c.NoteNewSuccess(alive, clock.Now())
	close(release)
	require.Eventually(t, func() bool { return !checking() }, 5*time.Second, time.Millisecond)
	require.Equal(t, NetworkNormal, networkState(c))
}

func TestAFailureWaitingForAnOutageSetsNoTimer(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Empty(t, g.attributed())
	c.mutex.Lock()
	timer := c.quality.timer
	c.mutex.Unlock()
	require.Nil(t, timer, "the failures wait for the outage to be concluded, not for a timer")
}

func TestAnOutageLeftWithoutNodesEndsUndecided(t *testing.T) {
	c, _, _ := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	c.Prune(nil, nil)
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageUndecided }))
}

func TestFailuresTheLocalNetworkExplainsAreNoOutage(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	c.firstHop = failingFirstHop
	alive, z := node("alive"), node("z")
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", alive, a, b, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, a, b)
	require.Len(t, c.Outages(), 1)
	testOnce(t, reg, g, 3, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	require.Empty(t, c.Outages())

	clock.Advance(time.Minute)
	c.NoteNewSuccess(a, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c))
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
	require.Equal(t, Counts{}, c.MemberCounts(Key{a, testCond}))
}

func TestOneRepresentativeOfAnOutageRunsAtATime(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	holdA, holdB := runner.holdOn(a), runner.holdOn(b)
	defer close(holdA)
	defer close(holdB)
	before := runner.count(a) + runner.count(b)
	reg.Submit(2, []Need{
		{Target: a, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover},
		{Target: b, Cond: testCond, Class: ClassMaintain, Rank: 41, Source: SourceRecover},
	})
	require.Eventually(t, func() bool { return runner.count(a)+runner.count(b) == before+1 }, time.Second, time.Millisecond)
	// Past the backoff of the outage, while the first still runs.
	clock.Advance(BackoffMax)
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, before+1, runner.count(a)+runner.count(b))
}

func TestAnEpisodeLeavesOutTheTimeTheNetworkSeemedDown(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	c.firstHop = failingFirstHop
	alive, x, y, z := node("alive"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	clock.Advance(2 * time.Hour)
	require.False(t, c.Persistent(Key{x, testCond}), "the time the network seems down does not count")
	runner.set(x, true)
	testOnce(t, reg, g, 3, x)
	completions := g.completions()
	episode := completions[len(completions)-1].Episode
	require.NotNil(t, episode)
	require.Equal(t, time.Duration(0), episode.Duration)
	require.False(t, episode.Persistent())
}

func TestOneSlowTargetDoesNotMakeTheNetworkPoor(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	fast, slow := node("fast"), node("slow")
	c.firstHop = func(_ context.Context, p C.Proxy) (time.Duration, error) {
		if p == slow {
			return FastDial + time.Millisecond, nil
		}
		return 20 * time.Millisecond, nil
	}
	x, y, z := node("x"), node("y"), node("z")
	g := newRecorder("g", fast, slow, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(fast, true)
	runner.set(slow, true)
	testOnce(t, reg, g, 1, fast, slow)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	checked := func() bool {
		for _, raw := range c.Records().Events(0, 1000).Events {
			if strings.Contains(string(raw), `"networkCheck"`) && strings.Contains(string(raw), `"checks"`) {
				return true
			}
		}
		return false
	}
	require.Eventually(t, checked, time.Second, time.Millisecond)
	require.Equal(t, NetworkNormal, networkState(c))
}

func TestASuccessHeardDuringACheckSupersedesIt(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	release := make(chan struct{})
	c.firstHop = func(context.Context, C.Proxy) (time.Duration, error) {
		<-release
		return 0, errors.New("unreachable")
	}
	alive, x, y, z := node("alive"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	dialed := clock.Now()
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	checking := func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.network.checking
	}
	require.Eventually(t, checking, time.Second, time.Millisecond)
	// a connection dialed before the check receives data while it runs
	clock.Advance(time.Second)
	c.NoteNewSuccess(alive, dialed)
	close(release)
	require.Eventually(t, func() bool { return !checking() }, 5*time.Second, time.Millisecond)
	require.Equal(t, NetworkNormal, networkState(c))
}

func TestTheTestThatStartedFirstConcludesAnOutage(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, healthy := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	g := newRecorder("g", a, b, healthy)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	// the healthy node is tested first and takes long; a dead one comes back meanwhile
	runner.set(healthy, true)
	hold := runner.holdOn(healthy)
	reg.Submit(2, []Need{{Target: healthy, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()}})
	require.Eventually(t, func() bool { return runner.count(healthy) == 1 }, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	runner.set(a, true)
	c.Manual(context.Background(), a, testCond, "test")
	require.Len(t, c.Outages(), 1, "it waits for the test that started earlier")
	close(hold)
	require.Eventually(t, func() bool { return len(c.Outages()) == 0 }, time.Second, time.Millisecond)
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageDisproved }))
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
}

func TestTheTimeTheNetworkSeemedDownCountsOnce(t *testing.T) {
	at := func(minute int) time.Time { return time.Unix(0, 0).Add(time.Duration(minute) * time.Minute) }
	n := &network{past: []interval{{start: at(10), end: at(20)}, {start: at(15), end: at(25)}}}
	require.Equal(t, 15*time.Minute, n.excused(at(0), at(60)), "overlapping times count once")
	require.Equal(t, 10*time.Minute, n.excused(at(15), at(60)))
	n.state, n.start = NetworkDown, at(30)
	require.Equal(t, 15*time.Minute+10*time.Minute, n.excused(at(0), at(40)), "the current time too")
}

func TestAFailureDueWhenItEndsIsAttributedAtOnce(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	hold := runner.holdOn(x)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()}})
	require.Eventually(t, func() bool { return runner.count(x) == 1 }, time.Second, time.Millisecond)
	clock.Advance(AttributionDelay + time.Minute)
	close(hold)
	require.Eventually(t, func() bool { return len(g.attributed()) == 1 }, time.Second, time.Millisecond)
}

func TestARouteNoLongerKnownIsAnUnknownPath(t *testing.T) {
	c, _ := newTestCoordinator(t)
	c.SetLookup(func(string) C.Proxy { return nil })
	x := pooledNode("x", "jk", "hop")
	pools, unknown := c.poolsOfRoute(x, C.ProbeResult{}, [][]string{{"gone"}})
	require.Equal(t, []string{"jk"}, pools)
	require.True(t, unknown)
}

func TestTheFirstOfTestsStartedTogetherConcludesAnOutage(t *testing.T) {
	conclude := func(first, second string) string {
		c, runner, clock := newClockedCoordinator(t)
		a, b, healthy := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
		nodes := map[string]C.Proxy{"a": a, "c": healthy}
		g := newRecorder("g", a, b, healthy)
		reg := activeRegistration(c, g)
		testOnce(t, reg, g, 1, a, b)
		runner.set(a, true)
		runner.set(healthy, true)
		holds := map[string]chan struct{}{"a": runner.holdOn(a), "c": runner.holdOn(healthy)}
		reg.Submit(2, []Need{
			{Target: healthy, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()},
			{Target: a, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover},
		})
		require.Eventually(t, func() bool { return runner.count(a) == 2 && runner.count(healthy) == 1 }, time.Second, time.Millisecond)
		close(holds[first])
		require.Eventually(t, func() bool {
			for _, done := range g.completions() {
				if done.Key.Target == nodes[first] && done.Result.Satisfied {
					return true
				}
			}
			return false
		}, time.Second, time.Millisecond)
		close(holds[second])
		require.Eventually(t, func() bool { return len(c.Outages()) == 0 }, time.Second, time.Millisecond)
		state := ""
		g.has(func(n Notice) bool {
			if o, ok := n.(OutageChanged); ok && o.State != OutageSuspected {
				state = o.State
				return true
			}
			return false
		})
		return state
	}
	require.Equal(t, conclude("a", "c"), conclude("c", "a"), "the order the results end in decides nothing")
}

func TestARecoveryRunningBeforeAnOutageIsItsRepresentative(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a)
	hold := runner.holdOn(a)
	defer close(hold)
	clock.Advance(BackoffFirst)
	reg.Submit(2, []Need{{Target: a, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover}})
	require.Eventually(t, func() bool { return runner.count(a) == 2 }, time.Second, time.Millisecond)
	// b fails meanwhile: the pool seems out, and a is already being tested again
	testOnce(t, reg, g, 3, b)
	require.Len(t, c.Outages(), 1)
	before := runner.count(b)
	reg.Submit(4, []Need{
		{Target: a, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover},
		{Target: b, Cond: testCond, Class: ClassMaintain, Rank: 41, Source: SourceRecover},
	})
	time.Sleep(20 * time.Millisecond)
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, before, runner.count(b), "no second recovery while the first runs")
}

func TestAnOldConnectionReceivingLateDoesNotEndTheNetworkDown(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	c.firstHop = failingFirstHop
	alive, x, y, z := node("alive"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	testOnce(t, reg, g, 1, alive)
	dialed := clock.Now()
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	clock.Advance(time.Minute)
	c.NoteNewSuccess(alive, dialed)
	require.Equal(t, NetworkDown, networkState(c), "dialed before it seemed down")
	c.NoteNewSuccess(alive, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c))
}

func TestACompletionNotAdoptedTellsItsRoute(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	x := pooledNode("x", "jk", "")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	runner.set(x, true)
	hold := runner.holdOn(x)
	reg.Submit(1, []Need{{Target: x, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()}})
	require.Eventually(t, func() bool { return runner.count(x) == 1 }, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	runner.mutex.Lock()
	delete(runner.hold, x)
	runner.mutex.Unlock()
	c.Manual(context.Background(), x, testCond, "test") // starts later, ends first
	close(hold)
	notAdopted := func() []Completion {
		var list []Completion
		for _, done := range g.completions() {
			if !done.Adopted && done.API == "" {
				list = append(list, done)
			}
		}
		return list
	}
	require.Eventually(t, func() bool { return len(notAdopted()) == 1 }, time.Second, time.Millisecond)
	require.Equal(t, []string{"jk"}, notAdopted()[0].Pools)
}

func TestTheEventsOutOfTheWindowsAreNotListed(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	x := node("x")
	g := newRecorder("g", x)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, x)
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Len(t, c.Failures(Key{x, testCond}), 1)
	clock.Advance(LongWindow)
	require.Empty(t, c.Failures(Key{x, testCond}))
}

func TestTheTransportUnderTheWrappersDecidesTheFirstHop(t *testing.T) {
	parse := func(network string) C.Proxy {
		p, err := adapter.ParseProxy(map[string]any{
			"name": "v-" + network, "type": "vmess", "server": "127.0.0.1", "port": 443,
			"uuid": "b831381d-6324-4d53-ad4f-8cda48b30811", "alterId": 0, "cipher": "auto", "network": network,
		})
		require.NoError(t, err)
		return p
	}
	require.True(t, tcpFirstHop(parse("tcp")))
	require.False(t, tcpFirstHop(parse("kcp")), "kcp runs over UDP, under the wrappers too")
}

func TestAConnectionBeforeANodeFailedDisprovesTheOutage(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, other := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	g := newRecorder("g", a, b, other)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	require.Len(t, c.Outages(), 1)
	clock.Advance(time.Second)
	dialed := clock.Now()
	// the third node fails later, before the connection dialed through it is heard of
	clock.Advance(time.Second)
	runner.set(other, false)
	testOnce(t, reg, g, 2, other)
	c.NoteNewSuccess(other, dialed)
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageDisproved }))
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
}

func TestAnEarlierSuccessNotAdoptedStillDisprovesTheOutage(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, other := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	g := newRecorder("g", a, b, other)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a, b)
	runner.set(other, true)
	hold := runner.holdOn(other)
	reg.Submit(2, []Need{{Target: other, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: clock.Now()}})
	require.Eventually(t, func() bool { return runner.count(other) == 1 }, time.Second, time.Millisecond)
	// a later test finds it dead first: its earlier success decides the evidence no more
	clock.Advance(time.Second)
	runner.mutex.Lock()
	delete(runner.hold, other)
	runner.alive[other] = false
	runner.mutex.Unlock()
	c.Manual(context.Background(), other, testCond, "test")
	runner.set(other, true)
	close(hold)
	require.Eventually(t, func() bool { return len(c.Outages()) == 0 }, time.Second, time.Millisecond)
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageDisproved }))
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
}
