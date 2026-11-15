package probe

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func (r *recorder) forwarded() []Forwarded {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var list []Forwarded
	for _, n := range r.notices {
		if f, ok := n.(Forwarded); ok {
			list = append(list, f)
		}
	}
	return list
}

// delegation sets up a group judging its own availability under key, and a group listing it.
func delegation(t *testing.T) (c *Coordinator, runner *fakeRunner, key Key, child, parent *recorder, childReg, parentReg *Registration) {
	c, runner = newTestCoordinator(t)
	exit := node("exit")
	key = Key{exit, testCond}
	child = newRecorder("exit")
	parent = newRecorder("top", exit)
	childReg = activeRegistration(c, child)
	parentReg = activeRegistration(c, parent)
	childReg.Delegate(key, true)
	return
}

func TestAPublishedAvailabilityAnswersWhatItRestsOn(t *testing.T) {
	c, runner, key, child, parent, childReg, parentReg := delegation(t)
	require.True(t, c.Delegated(key))
	require.True(t, c.Evidence(key).Delegated)
	t0 := c.now()
	parentReg.Submit(1, []Need{{Target: key.Target, Cond: testCond, Class: ClassSearch, Source: SourceSearch, NotBefore: t0}})
	forwarded := child.forwarded()
	require.NotEmpty(t, forwarded)
	last := forwarded[len(forwarded)-1]
	require.Len(t, last.Needs, 1, "the group hears what is needed of it")
	require.Equal(t, "top", last.Needs[0].Group)
	require.True(t, last.Needs[0].NotBefore.Equal(t0))

	// An availability resting on an earlier test does not answer it.
	childReg.Publish(key, Availability{State: Alive, Proof: t0.Add(-time.Second), Exec: 7})
	require.Equal(t, needPending, parentReg.needs[key].status)
	childReg.Publish(key, Availability{State: Alive, Proof: t0.Add(time.Millisecond), Exec: 8})
	require.Equal(t, needDone, parentReg.needs[key].status)
	last = child.forwarded()[len(child.forwarded())-1]
	require.Empty(t, last.Needs, "nothing is needed of it any more")
	completions := parent.completions()
	require.Equal(t, Alive, completions[len(completions)-1].After.State)
	require.True(t, completions[len(completions)-1].Result.Satisfied)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, runner.count(key.Target), "nothing is tested through the group")
}

func TestATerminalAvailabilityEndsAnyNeed(t *testing.T) {
	c, _, key, _, _, childReg, parentReg := delegation(t)
	childReg.Publish(key, Availability{State: Alive, Terminal: true})
	parentReg.Submit(1, []Need{{Target: key.Target, Cond: testCond, Class: ClassVerify, Source: SourceContinue, NotBefore: c.now()}})
	require.Equal(t, needDone, parentReg.needs[key].status)
}

func TestNoTestWritesADelegatedKey(t *testing.T) {
	c, runner, key, _, _, childReg, _ := delegation(t)
	childReg.Publish(key, Availability{State: Dead})
	runner.set(key.Target, true)
	result := c.Manual(context.Background(), key.Target, testCond, "proxies")
	require.True(t, result.Satisfied, "a manual test runs")
	require.Equal(t, Dead, c.Evidence(key).State, "but only the group writes its evidence")
}

func TestAFailureTheGroupPublishesCountsForTheGroupsListingIt(t *testing.T) {
	c, _, key, _, parent, childReg, _ := delegation(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	childReg.Publish(key, Availability{State: Alive, Proof: now.Add(-time.Minute), Exec: 3})
	childReg.Publish(key, Availability{State: Dead, Failed: true, Proof: now, Exec: 4})
	require.Empty(t, c.network.failures, "the paths tell the check of the local network")
	now = now.Add(AttributionDelay)
	c.AttributeDue()
	require.Equal(t, 1.0, c.MemberCounts(key).Recent)
	var attributed []Attributed
	parent.mutex.Lock()
	for _, n := range parent.notices {
		if a, ok := n.(Attributed); ok {
			attributed = append(attributed, a)
		}
	}
	parent.mutex.Unlock()
	require.Len(t, attributed, 1)
	require.Equal(t, OutcomeMember, attributed[0].Outcome)
}

func TestTurningDelegationOffForgetsWhatWasPublished(t *testing.T) {
	c, _, key, _, parent, childReg, _ := delegation(t)
	childReg.Publish(key, Availability{State: Alive, Proof: c.now(), Exec: 1})
	childReg.Delegate(key, false)
	ev := c.Evidence(key)
	require.False(t, ev.Delegated)
	require.Equal(t, Unknown, ev.State)
	completions := parent.completions()
	require.Equal(t, Unknown, completions[len(completions)-1].After.State)
	childReg.Publish(key, Availability{State: Dead})
	require.Equal(t, Unknown, c.Evidence(key).State, "a group no longer judging it publishes nothing")
}

func TestRunningProbeCannotWriteAcrossADelegateChange(t *testing.T) {
	for _, role := range []string{"primary", "related"} {
		for _, initial := range []string{"delegated", "ordinary", "replacedOwner"} {
			t.Run(role+"/"+initial, func(t *testing.T) {
				c, runner, clock := newClockedCoordinator(t)
				s := group("S", C.Fallback, "", true)
				short, long := testTimeoutCondition(5), testTimeoutCondition(10)
				key := Key{s, short}
				kept := Key{s, long}
				keys := []Key{key, kept}
				c.Register(&conditionedRecorder{newRecorder("short", s), short})
				c.Register(&conditionedRecorder{newRecorder("long", s), long})
				owner := c.Register(&conditionedRecorder{newRecorder("S"), short})
				c.Activate(keys, nil)
				if initial != "ordinary" {
					owner.Delegate(key, true)
					owner.Publish(key, Availability{State: Alive, Terminal: true})
				}
				panel := long
				if role == "primary" {
					panel = short
				}
				// A terminal delegate has no representative, so the panel tests S itself.
				c.mutex.Lock()
				target, _ := c.manualTargetLocked(s, panel)
				c.mutex.Unlock()
				require.Equal(t, s, target)
				old := startManual(c, target, panel)
				switch initial {
				case "ordinary":
					owner.Delegate(key, true)
				case "replacedOwner":
					owner = c.Register(&conditionedRecorder{newRecorder("next"), short})
					owner.Delegate(key, true)
				}
				owner.Delegate(key, false)
				if initial == "delegated" {
					c.Prune(keys, nil)
				}
				c.mutex.Lock()
				delegated := c.delegatedTargetLocked(s)
				c.mutex.Unlock()
				require.False(t, delegated)
				reset := c.Evidence(key)
				require.Equal(t, Unknown, reset.State)
				clock.Advance(time.Second)
				c.finish(old, C.ProbeResult{Satisfied: true, Elapsed: time.Second}, nil)
				require.Equal(t, reset, c.Evidence(key))
				require.Equal(t, Alive, c.Evidence(kept).State)
				runner.set(s, true)
				c.Manual(context.Background(), s, short, "proxies")
				require.Equal(t, Alive, c.Evidence(key).State)
			})
		}
	}
}

func TestTheCensusLeavesADelegatedKeyToItsPaths(t *testing.T) {
	c, _, key, _, _, _, _ := delegation(t)
	reg := c.registerPersistent(newRecorder("census"))
	reg.Submit(1, []Need{{Target: key.Target, Cond: testCond, Class: ClassMaintain, Source: SourceCensus}})
	require.Empty(t, reg.needs)
}

func TestAnUnavailabilityByPolicyIsNoFailure(t *testing.T) {
	c, _, key, _, parent, childReg, _ := delegation(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	childReg.Publish(key, Availability{State: Alive, Proof: now.Add(-time.Minute), Exec: 3})
	childReg.Publish(key, Availability{State: Dead, Proof: now.Add(-time.Hour), Exec: 2})
	completions := parent.completions()
	require.True(t, completions[len(completions)-1].Policy)
	now = now.Add(AttributionDelay)
	c.AttributeDue()
	require.Zero(t, c.MemberCounts(key).Recent, "an old failure of a member does not count again")
}

func TestAFailureDueAlreadyIsAttributedAtOnce(t *testing.T) {
	c, _, key, _, _, childReg, _ := delegation(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	childReg.Publish(key, Availability{State: Alive, Proof: now.Add(-time.Hour), Exec: 3})
	childReg.Publish(key, Availability{State: Dead, Failed: true, Proof: now.Add(-2 * AttributionDelay), Exec: 4})
	require.Equal(t, 1.0, c.MemberCounts(key).Recent)
}

func TestAChanceKeptWhileTheNetworkSeemedDownIsNotTakenForADelegatedKey(t *testing.T) {
	c, runner, key, _, _, childReg, _ := delegation(t)
	childReg.Delegate(key, false)
	c.mutex.Lock()
	c.evidenceLocked(key).State = Dead
	c.network.retained = map[Key]*Registration{key: childReg}
	c.mutex.Unlock()
	childReg.Delegate(key, true)
	c.mutex.Lock()
	_, kept := c.network.retained[key]
	c.mutex.Unlock()
	require.False(t, kept)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, runner.count(key.Target))
}

func TestAManualTestOfADelegatedGroupTestsThePathItPrefers(t *testing.T) {
	c, runner, key, _, _, childReg, _ := delegation(t)
	path := node("path")
	runner.set(path, true)
	childReg.Publish(key, Availability{State: Alive, Probe: path})
	manual := Condition{URL: testCond.URL, Expected: testCond.Expected, Timeout: 10 * time.Second}
	result := c.Manual(context.Background(), key.Target, manual, "proxies")
	require.True(t, result.Satisfied)
	require.Equal(t, 1, runner.count(path))
	require.Zero(t, runner.count(key.Target), "not through the group")
	var via []any
	for _, raw := range c.Records().Events(0, 1000).Events {
		var e map[string]any
		require.NoError(t, json.Unmarshal(raw, &e))
		if e["kind"] == "probe" {
			via, _ = e["via"].([]any)
		}
	}
	require.Len(t, via, 1, "the record names the group the test was taken from")

	// under another test it is tested through as any group
	other := Condition{URL: "http://other/", Expected: "*", Timeout: time.Second}
	runner.set(key.Target, true)
	c.Manual(context.Background(), key.Target, other, "proxies")
	require.Equal(t, 1, runner.count(key.Target))
}
