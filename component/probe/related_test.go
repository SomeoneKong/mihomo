package probe

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// conditionedRecorder is a recorder that reads evidence under one condition.
type conditionedRecorder struct {
	*recorder
	cond Condition
}

func (r *conditionedRecorder) References(target C.Proxy, cond Condition) bool {
	return cond == r.cond && r.recorder.References(target, cond)
}

func (r *conditionedRecorder) Conditions() []Condition { return []Condition{r.cond} }

func testTimeoutCondition(seconds int) Condition {
	cond := testCond
	cond.Timeout = time.Duration(seconds) * time.Second
	return cond
}

// scriptedRunner ends every test with the result it is set to.
type scriptedRunner struct {
	mutex  sync.Mutex
	result C.ProbeResult
}

func (r *scriptedRunner) set(result C.ProbeResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.result = result
}

func (r *scriptedRunner) run(context.Context, Key) C.ProbeResult {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.result
}

func TestATestUnderAnotherTimeoutTellsAGroupWhatItDecides(t *testing.T) {
	runner := &scriptedRunner{}
	c := New(time.Now, runner.run)
	x := node("x")
	cond := Condition{URL: "http://test/", Expected: "*", Timeout: 5 * time.Second}
	g := &conditionedRecorder{recorder: newRecorder("g", x), cond: cond}
	activeRegistration(c, g)
	key := Key{Target: x, Cond: cond}
	longer := Condition{URL: cond.URL, Expected: cond.Expected, Timeout: 10 * time.Second}
	shorter := Condition{URL: cond.URL, Expected: cond.Expected, Timeout: 2 * time.Second}
	ctx := context.Background()

	// a success within the timeout of the group
	runner.set(C.ProbeResult{Satisfied: true, Delay: 80, Status: 204, Elapsed: 300 * time.Millisecond})
	c.Manual(ctx, x, longer, "proxies")
	require.Equal(t, Alive, c.Evidence(key).State)
	require.True(t, g.has(func(n Notice) bool {
		completion, ok := n.(Completion)
		return ok && completion.Key == key && completion.Adopted && completion.Result.Satisfied
	}), "the group hears of it as of any test under its condition")

	// a failure within the timeout of the group is one under it; a slower success decides nothing
	runner.set(C.ProbeResult{ErrorClass: C.ProbeErrorDial, Error: "down", Elapsed: time.Second})
	c.Manual(ctx, x, longer, "proxies")
	require.Equal(t, Dead, c.Evidence(key).State)
	runner.set(C.ProbeResult{Satisfied: true, Delay: 7000, Status: 204, Elapsed: 7 * time.Second})
	c.Manual(ctx, x, longer, "proxies")
	require.Equal(t, Dead, c.Evidence(key).State, "a slow success decides nothing")

	// a timeout no shorter than the timeout of the group fails it, a shorter one decides nothing
	runner.set(C.ProbeResult{Satisfied: true, Delay: 80, Status: 204, Elapsed: 100 * time.Millisecond})
	c.Manual(ctx, x, shorter, "proxies")
	require.Equal(t, Alive, c.Evidence(key).State, "a success under a shorter timeout is one under a longer")
	runner.set(C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: 2 * time.Second})
	c.Manual(ctx, x, shorter, "proxies")
	require.Equal(t, Alive, c.Evidence(key).State)
	runner.set(C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: 10 * time.Second})
	c.Manual(ctx, x, longer, "proxies")
	ev := c.Evidence(key)
	require.Equal(t, Dead, ev.State)

	// another URL tells nothing
	runner.set(C.ProbeResult{Satisfied: true, Delay: 80, Status: 204, Elapsed: 100 * time.Millisecond})
	c.Manual(ctx, x, Condition{URL: "http://other/", Expected: "*", Timeout: 5 * time.Second}, "proxies")
	require.Equal(t, ev.Started, c.Evidence(key).Started)
}

func TestPruneKeepsManualResultForContinuouslyReferencedTimeout(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
	short, long := testCond, testCond
	short.Timeout, long.Timeout = 5*time.Second, 10*time.Second
	g := &conditionedRecorder{newRecorder("short", a), short}
	c.Register(g)
	c.Register(&conditionedRecorder{newRecorder("pool", b), testCond})
	keys := []Key{{a, short}, {b, testCond}}
	c.Activate(keys, nil)
	e := startManual(c, a, long)
	c.Prune(keys, []C.Proxy{a})
	clock.Advance(time.Second)
	c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Second}, nil)

	got := lastCompletion(t, g.recorder, a)
	require.Equal(t, e.id, got.Exec)
	require.True(t, got.Adopted && got.Charged)
	require.Equal(t, Dead, c.Evidence(Key{a, short}).State)
	require.Equal(t, Unknown, c.Evidence(Key{a, long}).State)
	var record map[string]any
	page := c.Records().Events(got.Seq-1, 1)
	require.Len(t, page.Events, 1)
	require.NoError(t, json.Unmarshal(page.Events[0], &record))
	require.Equal(t, false, record["adopted"])
	require.NotContains(t, record, "void")
	require.Len(t, record["related"], 1)

	// The newly dead key also supplies a witness under B's timeout.
	clock.Advance(time.Second)
	c.finish(startManual(c, b, testCond), C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: testCond.Timeout}, nil)
	_, suspected := c.OutageOf("p", testCond)
	require.True(t, suspected)
}

func TestRunningTestCannotDecideANewOrRecreatedTimeout(t *testing.T) {
	for _, when := range []string{"added", "recreated", "addedThenPruned"} {
		t.Run(when, func(t *testing.T) {
			c, _, clock := newClockedCoordinator(t)
			a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
			short, long := testCond, testCond
			short.Timeout, long.Timeout = 5*time.Second, 10*time.Second
			g := &conditionedRecorder{newRecorder("long", a), long}
			c.Register(g)
			kept := []Key{{a, long}, {b, short}}
			c.Register(&conditionedRecorder{newRecorder("pool", b), short})
			keys := append([]Key{{a, short}}, kept...)
			var old *Registration
			initial := kept
			if when == "recreated" {
				old = c.Register(&conditionedRecorder{newRecorder("short", a), short})
				initial = keys
			}
			c.Activate(initial, nil)
			failedInPastCoverage(c, clock, a, long)
			e := startManual(c, a, long)
			if old != nil {
				old.Unregister()
				c.Prune(kept, nil)
			}
			fresh := &conditionedRecorder{newRecorder("fresh", a), short}
			added := c.Register(fresh)
			c.Activate(keys, nil)
			if when == "addedThenPruned" {
				added.Unregister()
				c.Prune(kept, nil)
			}
			clock.Advance(time.Second)
			c.finish(e, C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Second}, nil)
			require.True(t, lastCompletion(t, g.recorder, a).Charged)
			require.Empty(t, fresh.completions())
			require.Equal(t, Unknown, c.Evidence(Key{a, short}).State)

			clock.Advance(time.Second)
			c.finish(startManual(c, b, short), C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: short.Timeout}, nil)
			_, suspected := c.OutageOf("p", short)
			require.False(t, suspected, "the stale result cannot witness an outage under the new key")
		})
	}
}

func TestRecreatedPrimaryNeedWaitsForItsOwnExecution(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	answers := make(chan C.ProbeResult, 2)
	c := New(clock.Now, func(context.Context, Key) C.ProbeResult { return <-answers })
	a := pooledNode("A", "p", "")
	short, long := testCond, testCond
	short.Timeout, long.Timeout = 5*time.Second, 10*time.Second
	related := &conditionedRecorder{newRecorder("short", a), short}
	primary := &conditionedRecorder{newRecorder("long", a), long}
	c.Register(related)
	old := c.Register(primary)
	keys := []Key{{a, short}, {a, long}}
	c.Activate(keys, nil)
	need := Need{Target: a, Cond: long, Class: ClassVerify, Source: SourceVerify, Notify: true, NotBefore: clock.Now()}
	old.Submit(1, []Need{need})
	first := primary.began()[0].Exec
	old.Unregister()
	c.Prune(keys[:1], nil)

	fresh := &conditionedRecorder{newRecorder("new", a), long}
	reg := c.Register(fresh)
	c.Activate(keys, nil)
	reg.Submit(1, []Need{need})
	beganBeforeRelease := fresh.began()
	answers <- C.ProbeResult{Satisfied: true}
	answers <- C.ProbeResult{ErrorClass: C.ProbeErrorDial}
	require.Empty(t, beganBeforeRelease, "the new need cannot join the old test, even at the same start time")
	require.Eventually(t, func() bool { return len(fresh.completions()) == 2 }, time.Second, time.Millisecond)
	results := fresh.completions()
	require.Equal(t, first, results[0].Exec)
	require.False(t, results[0].Adopted)
	require.Equal(t, Unknown, results[0].After.State)
	require.True(t, results[1].Adopted)
	require.NotEqual(t, first, results[1].Exec)
	require.Len(t, fresh.began(), 1)
	require.Equal(t, results[1].Exec, fresh.began()[0].Exec)
	projected := related.completions()
	require.Len(t, projected, 2)
	require.Equal(t, first, projected[0].Exec)
	require.True(t, projected[0].Adopted)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.Equal(t, projected[0].Started, c.outages.alive[poolKey{"p", short}])
	require.NotContains(t, c.outages.alive, poolKey{"p", long}, "the old success only tells the surviving pool condition")
}

func TestResultUnderAnotherTimeout(t *testing.T) {
	failed := C.ProbeResult{ErrorClass: C.ProbeErrorStatus, Status: 500, Elapsed: 4 * time.Second}
	r, ok := resultUnder(failed, 10*time.Second, 2*time.Second)
	require.True(t, ok)
	require.Equal(t, C.ProbeErrorTimeout, r.ErrorClass, "a test under the shorter timeout gives up before the answer")
	require.Equal(t, 2*time.Second, r.Elapsed)
	r, ok = resultUnder(failed, 10*time.Second, 5*time.Second)
	require.True(t, ok)
	require.Equal(t, failed, r)
	_, ok = resultUnder(C.ProbeResult{Canceled: true}, time.Second, time.Second)
	require.False(t, ok)
}

func TestManualProbeOfAnUnreadPrimary(t *testing.T) {
	for _, phase := range []string{"beforePrune", "pruned", "readAgain", "readAgainBeforePrune"} {
		t.Run(phase, func(t *testing.T) {
			c, runner, clock := newClockedCoordinator(t)
			a := node("A")
			key := Key{a, testCond}
			reader := &conditionedRecorder{newRecorder("g", a), testCond}
			c.Register(reader)
			c.Activate([]Key{key}, []C.Proxy{a})
			ctx := context.Background()
			c.Manual(ctx, a, testCond, "proxies")
			before := c.Evidence(key)
			require.Equal(t, Dead, before.State)

			// The provider publishes membership before the coordinator prunes its evidence.
			delete(reader.targets, a)
			clock.Advance(time.Second)
			runner.set(a, true)
			if phase == "beforePrune" {
				require.True(t, c.Manual(ctx, a, testCond, "proxies").Satisfied)
				require.Equal(t, Alive, c.Evidence(key).State)
				c.mutex.Lock()
				lastSuccess := c.network.success
				c.mutex.Unlock()
				require.Equal(t, clock.Now(), lastSuccess)
				return
			}
			old := startManual(c, a, testCond)
			if phase != "readAgainBeforePrune" {
				c.Prune(nil, []C.Proxy{a})
			}
			if phase != "pruned" {
				reader.targets[a] = true
				if phase == "readAgain" {
					c.Prune([]Key{key}, []C.Proxy{a})
				}
			}
			c.finish(old, C.ProbeResult{Satisfied: true}, nil)
			if phase == "readAgainBeforePrune" {
				require.Equal(t, before, c.Evidence(key))
			} else {
				require.Equal(t, Unknown, c.Evidence(key).State)
			}
			if phase != "pruned" {
				c.Manual(ctx, a, testCond, "proxies")
				require.Equal(t, Alive, c.Evidence(key).State)
			}
		})
	}
}

func TestStaleTimeoutCannotWitnessAnOutageUnderTheNewerEvidence(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, third := pooledNode("A", "p", ""), pooledNode("B", "p", ""), pooledNode("C", "p", "")
	short, long, panel := testTimeoutCondition(5), testTimeoutCondition(10), testTimeoutCondition(15)
	c.Register(&conditionedRecorder{newRecorder("long", a, b, third), long})
	c.Register(&conditionedRecorder{newRecorder("short", a), short})
	c.Activate([]Key{{a, short}, {a, long}, {b, long}, {third, long}}, nil)
	for _, p := range []C.Proxy{b, third} {
		c.finish(startManual(c, p, long), C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Millisecond}, nil)
		clock.Advance(time.Second)
	}
	outage := c.outages.active[poolKey{"p", long}]
	require.NotNil(t, outage)
	old := startManual(c, a, panel)
	clock.Advance(time.Second)
	fresh := startManual(c, a, long)
	clock.Advance(8 * time.Second)
	c.finish(fresh, C.ProbeResult{Satisfied: true, Elapsed: 8 * time.Second}, nil)
	require.Equal(t, OutageSuspected, outage.state)
	clock.Advance(6 * time.Second)
	c.finish(old, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: panel.Timeout}, nil)
	require.Equal(t, Alive, c.Evidence(Key{a, long}).State)
	require.Equal(t, Dead, c.Evidence(Key{a, short}).State)
	require.Equal(t, OutageDisproved, outage.state)
	require.NotContains(t, outage.witness, Key{a, long})
}

func TestOneWitnessFailurePerExecutionAndPoolCondition(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
	short, long := testTimeoutCondition(5), testTimeoutCondition(10)
	c.Register(&conditionedRecorder{newRecorder("short", a, b), short})
	c.Register(&conditionedRecorder{newRecorder("long", a), long})
	c.Activate([]Key{{a, short}, {a, long}, {b, short}}, nil)
	fail := C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Millisecond}
	c.finish(startManual(c, b, short), fail, nil)
	clock.Advance(time.Second)
	panel := startManual(c, a, testTimeoutCondition(15))
	c.finish(panel, fail, nil)
	outage := c.outages.active[poolKey{"p", short}]
	require.NotNil(t, outage)
	w := outage.witness[Key{a, short}]
	require.NotNil(t, w)
	require.Equal(t, []time.Time{panel.started}, w.failures)
}

func TestPoolWitnessAtTheManualTimeout(t *testing.T) {
	for _, seconds := range []int{10, 15} {
		panel := testTimeoutCondition(seconds)
		for _, kind := range []string{"firstFailure", "reconfirmed", "retry"} {
			t.Run(panel.Timeout.String()+"/"+kind, func(t *testing.T) {
				c, _, clock := newClockedCoordinator(t)
				a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
				short, long := testTimeoutCondition(5), testTimeoutCondition(10)
				g := &conditionedRecorder{newRecorder("short", a), short}
				c.Register(g)
				c.Register(&conditionedRecorder{newRecorder("long", b), long})
				keys := []Key{{a, short}, {b, long}}
				c.Activate(keys, nil)
				fail := C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Second}
				switch kind {
				case "reconfirmed":
					failedInPastCoverage(c, clock, a, short)
				case "retry":
					c.finish(startManual(c, a, short), fail, nil)
					clock.Advance(OutageWindow + time.Second)
				}
				panelRun := startManual(c, a, panel)
				c.Prune(keys, nil)
				clock.Advance(time.Second)
				c.finish(panelRun, fail, nil)
				require.Equal(t, Dead, c.Evidence(Key{a, short}).State)
				require.Equal(t, Unknown, c.Evidence(Key{a, long}).State)
				require.Equal(t, kind != "retry", lastCompletion(t, g.recorder, a).Charged)
				clock.Advance(time.Second)
				c.finish(startManual(c, b, long), fail, nil)
				outage := c.outages.active[poolKey{"p", long}]
				if kind == "retry" {
					require.Nil(t, outage)
					return
				}
				require.NotNil(t, outage)
				require.Equal(t, OutageSuspected, outage.state)
				w := outage.witness[Key{a, long}]
				require.NotNil(t, w)
				require.Equal(t, []time.Time{panelRun.started}, w.failures)
			})
		}
	}
}

func TestAFailureUnderAnotherTimeoutTellsThePoolUnderTheConditionOfAGroup(t *testing.T) {
	runner := &scriptedRunner{}
	c := New(time.Now, runner.run)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	short := Condition{URL: "http://test/", Expected: "*", Timeout: 5 * time.Second}
	long := Condition{URL: short.URL, Expected: short.Expected, Timeout: 10 * time.Second}
	c.Register(&conditionedRecorder{recorder: newRecorder("short", a), cond: short})
	c.Register(&conditionedRecorder{recorder: newRecorder("long", b), cond: long})
	c.Activate([]Key{{a, short}, {b, long}}, nil)
	ctx := context.Background()
	// b, read under the long timeout only, fails at once: under the short one too
	runner.set(C.ProbeResult{ErrorClass: C.ProbeErrorDial, Error: "down", Elapsed: time.Millisecond})
	c.Manual(ctx, b, long, "proxies")
	// a times out under the short one
	runner.set(C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Error: "timeout", Elapsed: short.Timeout})
	c.Manual(ctx, a, short, "proxies")
	outages := c.Outages()
	require.Len(t, outages, 1)
	require.Equal(t, short, outages[0].Cond)
}

func condTimeout(sec int) Condition {
	cond := testCond
	cond.Timeout = time.Duration(sec) * time.Second
	return cond
}

func TestAFailureSuggestsNoOutageUnderATimeoutNoNodeOfThePoolIsReadUnder(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, q := pooledNode("A", "P", ""), pooledNode("B", "P", ""), pooledNode("Q", "Q", "")
	short, long := condTimeout(5), condTimeout(10)
	c.Register(&conditionedRecorder{newRecorder("P-only", a, b), short})
	c.Register(&conditionedRecorder{newRecorder("Q-only", q), long})
	c.Activate([]Key{{a, short}, {b, short}, {q, long}}, nil)
	fail := C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Millisecond}
	for _, p := range []C.Proxy{a, b} {
		c.finish(startManual(c, p, long), fail, nil)
		clock.Advance(time.Second)
	}
	_, underShort := c.OutageOf("P", short)
	_, underLong := c.OutageOf("P", long)
	require.True(t, underShort)
	require.False(t, underLong, "only Q is read under the longer timeout")
	c.finish(startManual(c, a, long), C.ProbeResult{Satisfied: true, Elapsed: time.Millisecond}, nil)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	require.Equal(t, 1.0, c.PoolCounts("P", short).Long)
	require.Zero(t, c.PoolCounts("P", long).Long)
}

func TestALateFailureLeavesTheLaterFailureOfTheNode(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("A", "P", ""), pooledNode("B", "P", "")
	small, mid, long := condTimeout(3), condTimeout(5), condTimeout(15)
	c.Register(&conditionedRecorder{newRecorder("mid", a), mid})
	c.Register(&conditionedRecorder{newRecorder("long", a), long})
	c.Register(&conditionedRecorder{newRecorder("small", b), small})
	c.Activate([]Key{{a, mid}, {a, long}, {b, small}}, nil)
	first := startManual(c, a, condTimeout(20))
	clock.Advance(time.Second)
	// a new connection through A carried data: failures that started before tell nothing
	c.NoteNewSuccess(a, clock.Now())
	clock.Advance(time.Second)
	next := startManual(c, a, mid)
	clock.Advance(mid.Timeout)
	c.finish(next, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: mid.Timeout}, nil)
	clock.Advance(13 * time.Second)
	c.finish(first, C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, Elapsed: 20 * time.Second}, nil)
	recent := c.outages.recent[poolKey{"P", small}]
	require.Len(t, recent, 1)
	require.Equal(t, next.started, recent[0].started, "the later failure stands")
	clock.Advance(time.Second)
	c.finish(startManual(c, b, small), C.ProbeResult{ErrorClass: C.ProbeErrorDial, Elapsed: time.Millisecond}, nil)
	_, suspected := c.OutageOf("P", small)
	require.True(t, suspected)
}
