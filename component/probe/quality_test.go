package probe

import (
	"context"
	"errors"
	"hash/fnv"
	"net/netip"
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
	setHops(c, unreachableHop)
	alive, other, x, y, z := node("alive"), node("other"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, other, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	runner.set(other, true)
	testOnce(t, reg, g, 1, alive, other)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { state, _ := c.Network(); return state == NetworkDown }, time.Second, time.Millisecond)
	require.True(t, g.has(func(n Notice) bool { nc, ok := n.(NetworkChanged); return ok && nc.State == NetworkDown }))
	settle(t, c)

	// A failure while it seems down does not lengthen the backoff.
	ev := c.Evidence(Key{x, testCond})
	require.Equal(t, Dead, ev.State)

	// The tests are throttled: one at a time, one each interval.
	clock.Advance(time.Minute) // past the backoffs of the failures that led to it
	recheck(t, c)
	require.Equal(t, NetworkDown, networkState(c))
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

	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.NotEmpty(t, g.attributed())
	for _, a := range g.attributed() {
		require.Equal(t, OutcomeLocal, a.Outcome, a.Key.Target.Name())
	}
	require.Equal(t, Counts{}, c.MemberCounts(Key{x, testCond}))
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
	// it counts once its witnesses are attributed
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Equal(t, Counts{Recent: 1, Long: 1}, c.PoolCounts("jk", testCond))
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.Committed && o.Counted }))
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

// entryOf is an address of its own for each node, for the checks of the local network.
func entryOf(p C.Proxy) netip.AddrPort {
	h := fnv.New32a()
	_, _ = h.Write([]byte(p.Name()))
	v := h.Sum32()
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, byte(v >> 16), byte(v >> 8), byte(v)}), 443)
}

// setHops makes the checks of the local network connect to each node at an address of its
// own, as dial tells, with the connections to it a millisecond apart.
func setHops(c *Coordinator, dial func(p C.Proxy) (time.Duration, error)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.spacing = time.Millisecond
	c.resolveHop = func(_ context.Context, p C.Proxy) (netip.AddrPort, error) { return entryOf(p), nil }
	c.dialHop = func(_ context.Context, p C.Proxy, _ netip.AddrPort) (time.Duration, error) { return dial(p) }
}

func unreachableHop(C.Proxy) (time.Duration, error) {
	return 0, errors.New("connect: network is unreachable")
}

func fastHop(C.Proxy) (time.Duration, error) {
	return 20 * time.Millisecond, nil
}

func networkState(c *Coordinator) NetworkState {
	state, _ := c.Network()
	return state
}

// settle waits for the check of the local network running, if any, to end.
func settle(t *testing.T, c *Coordinator) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return !c.network.checking
	}, 2*time.Second, time.Millisecond)
}

// recheck lets the check of the local network due now run, and waits for it to end.
func recheck(t *testing.T, c *Coordinator) {
	t.Helper()
	c.kick()
	settle(t, c)
}

func TestTheNetworkIsCheckedThroughProxiesThatWorkedLately(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	setHops(c, unreachableHop)
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

func TestEntriesAllOutWhileASuccessIsHeardMakeTheNetworkPoor(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	release := make(chan struct{})
	setHops(c, func(C.Proxy) (time.Duration, error) {
		<-release
		return LocalCheckTimeout, errors.New("i/o timeout")
	})
	alive, other, x, y, z := node("alive"), node("other"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, other, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	runner.set(other, true)
	testOnce(t, reg, g, 1, alive, other)
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
	settle(t, c)
	require.Equal(t, NetworkPoor, networkState(c), "something works: not down, but poor")
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
	release := make(chan struct{})
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		<-release
		return unreachableHop(p)
	})
	alive, other := node("alive"), node("other")
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", alive, other, a, b)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	runner.set(other, true)
	testOnce(t, reg, g, 1, alive, other)
	clock.Advance(time.Second)
	// the check the first failure calls for runs while the pool seems out
	testOnce(t, reg, g, 2, a, b)
	require.Len(t, c.Outages(), 1)
	close(release)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	require.Empty(t, c.Outages())
	settle(t, c)

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
	setHops(c, unreachableHop)
	alive, other, x, y, z := node("alive"), node("other"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, other, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	runner.set(other, true)
	testOnce(t, reg, g, 1, alive, other)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	settle(t, c)
	// the checks keep confirming it for two hours while the network is used
	for i := 0; i < 24; i++ {
		clock.Advance(5 * time.Minute)
		c.NoteDialFailure(y, clock.Now())
		recheck(t, c)
		require.Equal(t, NetworkDown, networkState(c))
	}
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
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		if p == slow {
			return FastDial + time.Millisecond, nil
		}
		return 20 * time.Millisecond, nil
	})
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

func TestOnlyAConnectionDialedDuringACheckTellsTheNetworkIsNotDown(t *testing.T) {
	check := func(dialedDuring bool) NetworkState {
		c, runner, clock := newClockedCoordinator(t)
		release := make(chan struct{})
		setHops(c, func(p C.Proxy) (time.Duration, error) {
			<-release
			return unreachableHop(p)
		})
		alive, other, x, y, z := node("alive"), node("other"), node("x"), node("y"), node("z")
		g := newRecorder("g", alive, other, x, y, z)
		reg := activeRegistration(c, g)
		runner.set(alive, true)
		runner.set(other, true)
		testOnce(t, reg, g, 1, alive, other)
		clock.Advance(time.Second)
		dialed := clock.Now() // before the check
		clock.Advance(time.Second)
		testOnce(t, reg, g, 2, x, y, z)
		checking := func() bool {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			return c.network.checking
		}
		require.Eventually(t, checking, time.Second, time.Millisecond)
		clock.Advance(time.Second)
		if dialedDuring {
			dialed = clock.Now()
		}
		clock.Advance(time.Second)
		c.NoteNewSuccess(alive, dialed)
		close(release)
		settle(t, c)
		return networkState(c)
	}
	require.Equal(t, NetworkDown, check(false), "a connection dialed before the check")
	require.Equal(t, NetworkPoor, check(true), "a connection dialed while it ran")
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
	n := &network{past: []interval{{start: at(10), end: at(20)}, {start: at(15), end: at(25)}}, confirmed: at(40), activity: at(40)}
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
	setHops(c, unreachableHop)
	alive, other, x, y, z := node("alive"), node("other"), node("x"), node("y"), node("z")
	g := newRecorder("g", alive, other, x, y, z)
	reg := activeRegistration(c, g)
	runner.set(alive, true)
	runner.set(other, true)
	testOnce(t, reg, g, 1, alive, other)
	dialed := clock.Now()
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool { return networkState(c) == NetworkDown }, time.Second, time.Millisecond)
	settle(t, c)
	clock.Advance(time.Minute)
	c.NoteNewSuccess(alive, dialed)
	settle(t, c)
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
