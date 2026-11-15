package probe

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func compareNeed(p C.Proxy, notBefore time.Time) Need {
	return Need{Target: p, Cond: testCond, Class: ClassMaintain, Source: SourceCompare, NotBefore: notBefore, Notify: true}
}

func (r *recorder) began() []Began {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var list []Began
	for _, n := range r.notices {
		if b, ok := n.(Began); ok {
			list = append(list, b)
		}
	}
	return list
}

func TestAFailureOnlyComparisonsAskedForIsNotTakenIn(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
	g := &conditionedRecorder{newRecorder("g", a, b), testCond}
	// another group reads A under a longer timeout: the failure decides nothing there either
	long := testCond
	long.Timeout = 2 * testCond.Timeout
	other := &conditionedRecorder{newRecorder("long", a), long}
	reg := c.Register(g)
	c.Register(other)
	c.Activate(nil, nil)
	runner.set(a, true)
	testOnce(t, reg, g.recorder, 1, a)
	before := c.Evidence(Key{a, testCond})
	require.Equal(t, Alive, before.State)
	related := c.Evidence(Key{a, long})
	require.Equal(t, Alive, related.State, "the success decided the longer timeout")

	clock.Advance(time.Minute)
	runner.set(a, false)
	runner.set(b, false)
	reg.Submit(2, []Need{compareNeed(a, clock.Now()), compareNeed(b, clock.Now())})
	require.Eventually(t, func() bool { return len(g.completions()) == 3 }, 2*time.Second, time.Millisecond)
	for _, completion := range g.completions()[1:] {
		require.True(t, completion.Compared, completion.Key.Target.Name())
		require.False(t, completion.Ended.IsZero())
		require.False(t, completion.Adopted)
		require.False(t, completion.Charged)
	}
	require.Equal(t, before, c.Evidence(Key{a, testCond}), "the evidence stays")
	require.Equal(t, related, c.Evidence(Key{a, long}), "so does that of the longer timeout")
	require.Len(t, other.completions(), 1, "nothing to tell under the longer timeout")
	require.Equal(t, Unknown, c.Evidence(Key{b, testCond}).State)
	require.Empty(t, c.Outages(), "two nodes of a pool failing suggest no outage")
	c.mutex.Lock()
	require.Equal(t, NetworkNormal, c.network.state)
	require.Empty(t, c.network.failures, "no check of the local network is asked for")
	require.Empty(t, c.quality.pending, "nothing to attribute")
	for _, entry := range reg.needs {
		require.Equal(t, needDone, entry.status, "the comparisons have their result")
	}
	c.mutex.Unlock()
	require.Len(t, g.began(), 2, "told of the start of each")
}

func TestASuccessOfAComparisonIsTakenIn(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	reg.Submit(1, []Need{compareNeed(a, clock.Now())})
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, 2*time.Second, time.Millisecond)
	completion := g.completions()[0]
	require.True(t, completion.Adopted)
	require.False(t, completion.Compared)
	require.Equal(t, Alive, c.Evidence(Key{a, testCond}).State)
}

func TestANeedJoinsNoTestOnlyComparisonsStarted(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	other := newRecorder("other", a)
	reg := activeRegistration(c, g)
	otherReg := c.Register(other)
	c.Activate(nil, nil)
	release := runner.holdOn(a)
	reg.Submit(1, []Need{compareNeed(a, clock.Now())})
	<-runner.running
	// another group asks for a test of the member while the comparison runs: it gets one of
	// its own, after
	otherReg.Submit(1, []Need{{Target: a, Cond: testCond, Class: ClassMaintain, Source: SourcePool}})
	runner.mutex.Lock()
	delete(runner.hold, a)
	runner.mutex.Unlock()
	close(release)
	require.Eventually(t, func() bool { return len(other.completions()) == 2 }, 2*time.Second, time.Millisecond)
	first, second := other.completions()[0], other.completions()[1]
	require.True(t, first.Compared)
	require.False(t, first.Adopted)
	require.False(t, second.Compared)
	require.True(t, second.Adopted)
	require.Equal(t, Dead, c.Evidence(Key{a, testCond}).State)
	require.Equal(t, 2, runner.count(a))
}

func TestAComparisonBecomingAnotherNeedGetsATestOfItsOwn(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	release := runner.holdOn(a)
	start := clock.Now()
	reg.Submit(1, []Need{compareNeed(a, start)})
	<-runner.running
	// the group now verifies the member as well: the need is no comparison any more
	verify := compareNeed(a, start)
	verify.Class, verify.Source = ClassVerify, SourceVerify
	reg.Submit(2, []Need{verify})
	runner.mutex.Lock()
	delete(runner.hold, a)
	runner.mutex.Unlock()
	close(release)
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, 2*time.Second, time.Millisecond)
	require.True(t, g.completions()[0].Compared)
	require.True(t, g.completions()[1].Adopted)
	require.Equal(t, Dead, c.Evidence(Key{a, testCond}).State)
}

func TestAComparisonRunsOnce(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	start := clock.Now()
	reg.Submit(1, []Need{compareNeed(a, start)})
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, 2*time.Second, time.Millisecond)
	// submitted again for the same round, it does not run again
	reg.Submit(2, []Need{compareNeed(a, start)})
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, runner.count(a))
	// a new round asks for a new test
	clock.Advance(time.Minute)
	reg.Submit(3, []Need{compareNeed(a, clock.Now())})
	require.Eventually(t, func() bool { return runner.count(a) == 2 }, 2*time.Second, time.Millisecond)
}

func TestAComparisonTakesNoEvidenceItDidNotRun(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	start := clock.Now()
	clock.Advance(time.Second)
	c.Manual(context.Background(), a, testCond, "proxies")
	require.Equal(t, 1, runner.count(a))
	reg.Submit(1, []Need{compareNeed(a, start)})
	require.Eventually(t, func() bool { return runner.count(a) == 2 }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return len(g.began()) == 1 }, 2*time.Second, time.Millisecond)
}

func TestAComparisonOfADeadMemberRecoversItAsAnyTest(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, a)
	require.Equal(t, Dead, c.Evidence(Key{a, testCond}).State)
	clock.Advance(BackoffFirst)
	reg.Submit(2, []Need{compareNeed(a, clock.Now())})
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, 2*time.Second, time.Millisecond)
	completion := g.completions()[1]
	require.False(t, completion.Compared, "a test of a dead member is a recovery")
	require.True(t, completion.Adopted)
	require.Equal(t, 2, c.Evidence(Key{a, testCond}).Failures)
}

func TestAGroupIsToldOfATestJoinedWhileRunning(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	other := newRecorder("other", a)
	reg := activeRegistration(c, g)
	otherReg := c.Register(other)
	c.Activate(nil, nil)
	runner.set(a, true)
	release := runner.holdOn(a)
	start := clock.Now()
	otherReg.Submit(1, []Need{{Target: a, Cond: testCond, Class: ClassMaintain, Source: SourcePool, NotBefore: start}})
	<-runner.running
	reg.Submit(1, []Need{compareNeed(a, start)})
	require.Len(t, g.began(), 1)
	require.Empty(t, other.began(), "it did not ask")
	// a need asking to be told once it is served already is told too
	pool := Need{Target: a, Cond: testCond, Class: ClassMaintain, Source: SourcePool, NotBefore: start}
	otherReg.Submit(2, []Need{pool})
	pool.Notify = true
	otherReg.Submit(3, []Need{pool})
	require.Len(t, other.began(), 1)
	close(release)
	require.Eventually(t, func() bool { return len(g.completions()) == 1 }, 2*time.Second, time.Millisecond)
	require.Equal(t, g.began()[0].Exec, g.completions()[0].Exec)
}

func TestComparisonsRunMarked(t *testing.T) {
	type mark struct {
		name   string
		marked bool
	}
	marked := make(chan mark, 4)
	clock := &fakeClock{now: time.Now()}
	c := New(clock.Now, func(ctx context.Context, key Key) C.ProbeResult {
		marked <- mark{key.Target.Name(), C.IsComparison(ctx)}
		return C.ProbeResult{ErrorClass: C.ProbeErrorDial}
	})
	a, b := node("A"), node("B")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	reg.Submit(1, []Need{compareNeed(a, clock.Now()), {Target: b, Cond: testCond, Class: ClassMaintain, Source: SourcePool}})
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case m := <-marked:
			got[m.name] = m.marked
		case <-time.After(2 * time.Second):
			t.Fatal("the tests did not run")
		}
	}
	require.Equal(t, map[string]bool{"A": true, "B": false}, got)
}

func TestARecordTellsTheFirstResponseAndAComparison(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c := New(clock.Now, func(ctx context.Context, key Key) C.ProbeResult {
		if key.Target.Name() == "A" {
			return C.ProbeResult{Satisfied: true, Delay: 100, FirstSatisfied: true, FirstElapsed: 1200 * time.Millisecond}
		}
		return C.ProbeResult{ErrorClass: C.ProbeErrorTimeout}
	})
	a, b := node("A"), node("B")
	g := newRecorder("g", a, b)
	reg := activeRegistration(c, g)
	reg.Submit(1, []Need{compareNeed(a, clock.Now()), compareNeed(b, clock.Now())})
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, 2*time.Second, time.Millisecond)
	probes := map[string]map[string]any{}
	for _, raw := range c.Records().Events(0, 1000).Events {
		var e map[string]any
		require.NoError(t, json.Unmarshal(raw, &e))
		if e["kind"] == "probe" {
			probes[e["target"].(map[string]any)["name"].(string)] = e
		}
	}
	require.Equal(t, map[string]any{"ok": true, "elapsed": float64(1200)}, probes["A"]["first"])
	require.Nil(t, probes["A"]["compared"])
	require.Equal(t, map[string]any{"ok": false}, probes["B"]["first"])
	require.Equal(t, true, probes["B"]["compared"])
}

func TestCoveredTellsTheLocalNetworkAndTheOutagesOfPools(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	now := clock.Now()
	c.mutex.Lock()
	c.network.past = []interval{{start: now.Add(-time.Hour), end: now.Add(-50 * time.Minute)}}
	c.outages.past = append(c.outages.past,
		&outage{key: poolKey{"p", testCond}, first: now.Add(-30 * time.Minute), end: now.Add(-20 * time.Minute), state: OutageEstablished},
		&outage{key: poolKey{"q", testCond}, first: now.Add(-30 * time.Minute), end: now.Add(-20 * time.Minute), state: OutageDisproved},
	)
	c.mutex.Unlock()
	require.True(t, c.Covered(now.Add(-55*time.Minute), nil, testCond), "the local network")
	require.False(t, c.Covered(now.Add(-40*time.Minute), []string{"p"}, testCond))
	require.True(t, c.Covered(now.Add(-25*time.Minute), []string{"x", "p"}, testCond), "an outage of a pool along it")
	require.False(t, c.Covered(now.Add(-25*time.Minute), []string{"q"}, testCond), "a disproved one")
	other := testCond
	other.URL = "http://other/"
	require.False(t, c.Covered(now.Add(-25*time.Minute), []string{"p"}, other), "under another condition")
}

func TestANeedARoundIsToldOfTakesNoEvidenceItDidNotRun(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	start := clock.Now()
	clock.Advance(time.Second)
	// a success after the round started, before the group asked
	c.Manual(context.Background(), a, testCond, "proxies")
	verify := Need{Target: a, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: start, Notify: true}
	reg.Submit(1, []Need{verify})
	require.Eventually(t, func() bool { return runner.count(a) == 2 && len(g.began()) == 1 }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, 2*time.Second, time.Millisecond)
	require.Equal(t, g.began()[0].Exec, g.completions()[1].Exec)
	reg.Submit(2, []Need{verify})
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, runner.count(a), "answered once for the round")
}

func TestAComparisonALaterTestOvertookHasItsResult(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	release := runner.holdOn(a)
	start := clock.Now()
	reg.Submit(1, []Need{compareNeed(a, start)})
	<-runner.running
	runner.mutex.Lock()
	delete(runner.hold, a)
	runner.mutex.Unlock()
	clock.Advance(time.Second)
	c.Manual(context.Background(), a, testCond, "proxies")
	manual := c.Evidence(Key{a, testCond}).Exec
	close(release)
	require.Eventually(t, func() bool { return len(g.completions()) == 2 }, 2*time.Second, time.Millisecond)
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, runner.count(a), "no third test for the round")
	require.Equal(t, manual, c.Evidence(Key{a, testCond}).Exec)
}

func TestANeedBecomingAComparisonJoinsTheComparisonRunning(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	other := newRecorder("other", a)
	reg := activeRegistration(c, g)
	otherReg := c.Register(other)
	c.Activate(nil, nil)
	release := runner.holdOn(a)
	start := clock.Now()
	reg.Submit(1, []Need{compareNeed(a, start)})
	<-runner.running
	pool := Need{Target: a, Cond: testCond, Class: ClassMaintain, Source: SourcePool, NotBefore: start}
	otherReg.Submit(1, []Need{pool})
	// it waits for the comparison; once a comparison itself, it joins it
	otherReg.Submit(2, []Need{compareNeed(a, start)})
	require.Len(t, other.began(), 1)
	runner.mutex.Lock()
	delete(runner.hold, a)
	runner.mutex.Unlock()
	close(release)
	require.Eventually(t, func() bool { return len(other.completions()) == 1 }, 2*time.Second, time.Millisecond)
	c.kick()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, runner.count(a))
}

func TestANeedAnsweredByEvidenceIsToldOfATestOnceARoundAsksForIt(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a := node("A")
	g := newRecorder("g", a)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	start := clock.Now()
	clock.Advance(time.Second)
	c.Manual(context.Background(), a, testCond, "proxies")
	verify := Need{Target: a, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: start}
	reg.Submit(1, []Need{verify})
	c.mutex.Lock()
	done := reg.needs[Key{a, testCond}].status == needDone
	c.mutex.Unlock()
	require.True(t, done, "answered by the manual test")
	verify.Notify = true
	reg.Submit(2, []Need{verify})
	require.Eventually(t, func() bool { return len(g.began()) == 1 && runner.count(a) == 2 }, 2*time.Second, time.Millisecond)
}
