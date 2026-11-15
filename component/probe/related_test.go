package probe

import (
	"context"
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
