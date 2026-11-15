package outboundgroup

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

func TestClearMemoryRestoresYieldingMemberAndKeepsCurrentFacts(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	ot.node("C", "excluded")
	f := ot.group("office", "A", "B", "C", "D", "REJECT")
	od := f.od
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "excluded"}})
	od.setPinned("B", "user")
	ot.settle(f)
	for i := 0; i < probe.YieldRecent; i++ {
		ot.failOnce(f, "A", clock)
		clock.Advance(probe.AttributionDelay)
		ot.coord.AttributeDue()
		ot.settle(f)
	}
	require.Equal(t, standingYield, ot.standing(f, "A"))
	od.setPinned("C", "user")
	a := probe.Key{Target: ot.proxies["A"], Cond: od.cond}
	d := probe.Key{Target: ot.proxies["D"], Cond: od.cond}
	ot.coord.Manual(context.Background(), a.Target, od.cond, "test")
	ot.coord.Manual(context.Background(), d.Target, od.cond, "test")
	ot.settle(f)
	require.Equal(t, "B", ot.current(f))
	alive, dead := ot.coord.Evidence(a), ot.coord.Evidence(d)
	require.Equal(t, probe.Alive, alive.State)
	require.Equal(t, probe.Dead, dead.State)
	require.NotZero(t, dead.NextAllowed)
	require.GreaterOrEqual(t, ot.coord.MemberCounts(a).Recent, float64(probe.YieldRecent))
	table := ot.coord.Exclusions().Current()
	probes, needs := len(ot.records("probe")), len(ot.records("need"))
	od.noteDialed(ot.proxies["B"])
	require.Equal(t, probe.MemoryClearResult{Clear: 1, Groups: 1}, ot.coord.ClearMemory())
	require.Equal(t, standingNormal, ot.standing(f, "A"))
	require.Equal(t, "A", ot.current(f))
	require.Equal(t, "A", ot.route(f))
	require.Equal(t, causeMemoryCleared, od.snap.Load().cause.Reason)
	require.Equal(t, probe.Counts{}, ot.coord.MemberCounts(a))
	require.Equal(t, alive, ot.coord.Evidence(a))
	require.Equal(t, dead, ot.coord.Evidence(d))
	require.Equal(t, table, ot.coord.Exclusions().Current())
	require.Equal(t, "C", od.snap.Load().pinned)
	require.True(t, od.snap.Load().excluded[ot.proxies["C"]])
	require.Len(t, ot.records("probe"), probes)
	require.Len(t, ot.records("need"), needs, "the clear asks for no test")
	standings := ot.groupEvents("standing")
	last := standings[len(standings)-1]
	require.Equal(t, "cleared", last["reason"])
	require.Equal(t, "yield", last["previous"])
	require.Equal(t, "normal", last["standing"])
	require.Contains(t, last["display"].(map[string]any)["text"].(map[string]any)["zh"], "策略记忆被清除")
	od.noteDialed(a.Target)
	ot.settle(f)
	switches := ot.records("switch")
	require.Len(t, switches, 1)
	require.Equal(t, causeMemoryCleared, switches[0]["cause"].(map[string]any)["reason"])
}

func TestClearMemoryReleasesCooldownButKeepsObligation(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "Normal", true: "Strict"}[strict], func(t *testing.T) {
			ot, clock := newClockedODTest(t)
			ot.runner.set(true, "A", "B")
			f := ot.buildOption(GroupCommonOption{Name: "g", StrictOrder: strict}, "A", "B")
			ot.activate(f)
			ot.settle(f)
			a := ot.proxies["A"]
			od := f.od
			hold := ot.runner.holdOn("A")
			od.mutex.Lock()
			ms := od.member(a)
			ms.cooldown, ms.cooldownUntil, ms.cooldownExec = probe.CooldownMax, clock.Now().Add(time.Hour), 42
			ms.cooldownReset = clock.Now()
			ms.signals, ms.obligationDial = []time.Time{clock.Now()}, clock.Now()
			od.mutex.Unlock()
			clock.Advance(time.Second)
			result := ot.coord.ClearMemory()
			od.mutex.Lock()
			cooldown, until, exec, reset, signals := ms.cooldown, ms.cooldownUntil, ms.cooldownExec, ms.cooldownReset, len(ms.signals)
			od.mutex.Unlock()
			close(hold)
			require.Equal(t, 1, result.Groups)
			require.Zero(t, cooldown)
			require.Zero(t, until)
			require.Zero(t, exec)
			require.Zero(t, reset)
			require.Equal(t, 1, signals)
			ot.eventually(func() bool {
				od.mutex.Lock()
				defer od.mutex.Unlock()
				return len(ms.signals) == 0 && ms.cooldown == probe.CooldownFirst && ms.cooldownExec != 42
			}, "the new verification satisfies the existing obligation and starts a fresh cooldown")
		})
	}
}

func TestClearMemoryLateCompletionsUpdateFactsOnly(t *testing.T) {
	ot, clock := newClockedODTest(t)
	f := ot.group("g", "A", "B", "REJECT")
	ot.settle(f)
	before := clock.Now()
	clock.Advance(time.Millisecond)
	ot.coord.ClearMemory()
	od := f.od
	od.mutex.Lock()
	defer od.mutex.Unlock()
	a, b := ot.proxies["A"], ot.proxies["B"]
	now := clock.Now()
	key := probe.Key{Target: a, Cond: od.cond}
	ms := od.member(a)
	ms.signals = []time.Time{before}
	od.suspended = &suspendState{member: a, after: before, exec: 11}
	od.activated = false
	// Even if an earlier trial were available for replay, old results cannot settle it.
	trial := &trialState{started: before, pending: map[uint64]time.Time{}}
	od.qualityOf(a).trial = trial
	result := probe.Completion{Key: key, Exec: 11, Started: before, Adopted: true,
		After:  probe.Evidence{State: probe.Alive, Started: before, Version: 1},
		Result: C.ProbeResult{Satisfied: true, Elapsed: time.Second}, Requests: []probe.Request{{Group: "g", Source: probe.SourceVerify}}}
	od.handle(result, now)
	require.Empty(t, ms.signals)
	require.Zero(t, ms.cooldown)
	require.Nil(t, od.suspended)
	require.False(t, trial.witness)
	require.Equal(t, probe.Alive, od.view[a].State)
	standby := result
	standby.Exec, standby.Key.Target, standby.Started = 12, b, now
	standby.Result.Elapsed = time.Millisecond
	standby.Requests = []probe.Request{{Group: "g", Source: probe.SourceStandby}}
	od.handle(standby, now)
	require.Empty(t, od.degradations)
	require.False(t, od.pairRuns[executionKey{standby.Key, standby.Exec}].paired)
	result.Exec++
	result.After.State, result.After.Version, result.Result.Satisfied, result.Charged = probe.Dead, 2, false, true
	od.handle(result, now)
	od.handle(probe.Attributed{Key: key, Exec: result.Exec, Started: before, Outcome: probe.OutcomeMember}, now)
	require.Empty(t, trial.pending)
	require.False(t, trial.failed)
	require.Empty(t, od.attributed)
	require.Len(t, od.early, 1, "only the standby that started at the boundary is kept for replay")
	require.NotNil(t, od.search)
	require.Equal(t, before, od.search.t0)
	require.Equal(t, b, od.current)
}

func TestClearMemoryVoidsComparisonAndKeepsConfirmation(t *testing.T) {
	ot, clock := newClockedODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.settle(f)
	od := f.od
	od.mutex.Lock()
	defer od.mutex.Unlock()
	ps := od.prefer
	a, b := ot.proxies["A"], ot.proxies["B"]
	before := clock.Now()
	noteRound(od, before, tiered{a, tierSlow}, tiered{b, tierTimely})
	od.changePreference(b, causePreferred, "", []pairResult{{at: before}}, nil, before)
	ps.round = &compareRound{id: 23, t0: before, c: b, parts: []*participant{{member: b, exec: 11, started: before}}}
	ps.confirms[a] = confirmation{exec: 12, after: before}
	ps.nextAt = before.Add(time.Hour)
	lease := &onDemand{}
	ps.leases[lease] = true
	clock.Advance(time.Second)
	now := clock.Now()
	od.handle(probe.MemoryCleared{MemoryBoundary: probe.MemoryBoundary{Clear: 1, At: now}}, now)
	require.Nil(t, ps.preferred)
	require.Empty(t, ps.basis)
	require.Empty(t, ps.records)
	require.Nil(t, ps.round)
	require.Zero(t, ps.lastChange)
	require.Empty(t, od.gate(&compareRound{c: a}, now))
	require.Equal(t, now.Add(compareRetry), ps.retryAt)
	require.Equal(t, before.Add(time.Hour), ps.nextAt)
	require.True(t, ps.leases[lease])
	require.Equal(t, uint64(12), ps.confirms[a].exec)
	events := ot.groupEvents("preference")
	require.Equal(t, causePreferenceEnd, events[len(events)-1]["reason"])
	require.Equal(t, "cleared", events[len(events)-1]["why"])
	rounds := ot.groupEvents("compare")
	require.Len(t, rounds, 1)
	require.Equal(t, "void", rounds[0]["end"])
	require.Equal(t, "cleared", rounds[0]["reason"])
	// a test started after the failure and before the clear, back after it, still confirms
	key := probe.Key{Target: a, Cond: od.cond}
	started := before.Add(500 * time.Millisecond)
	od.handle(probe.Began{Key: key, Exec: 13, Started: started}, now)
	od.handle(probe.Completion{Key: key, Exec: 13, Started: started, Adopted: true,
		After: probe.Evidence{State: probe.Alive, Started: started, Exec: 13, Version: 1}, Result: C.ProbeResult{Satisfied: true}}, now)
	delete(ps.leases, lease)
	od.updatePrefer(now)
	require.Empty(t, ps.confirms, "the old completion still confirms the member")
	require.Empty(t, ps.records)
	confirms := ot.groupEvents("confirm")
	require.NotEmpty(t, confirms)
	ended := confirms[len(confirms)-1]
	require.Equal(t, "tested", ended["end"])
	require.EqualValues(t, 12, ended["probe"])
	require.EqualValues(t, 13, ended["by"])
}

func TestClearMemoryAcrossReload(t *testing.T) {
	for _, when := range []string{"BeforeRetirement", "BetweenRetirementAndHandoff", "AfterHandoff"} {
		t.Run(when, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)}
			ot := newODTestAt(t, clock.Now)
			old := ot.ordered("g", OrderReliable, "A", "B", "C")
			ot.settle(old)
			ot.coord.BeginPublish()
			fresh := ot.buildOption(GroupCommonOption{Name: "g", Order: OrderReliable}, "A", "B", "C")
			ot.settle(fresh)
			a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
			now := clock.Now()
			minute := summaryKey{member: b, minute: now.Unix() / 60}
			old.od.mutex.Lock()
			q := old.od.qualityOf(a)
			q.standing, q.failedTrials, q.readmitAt = standingYield, 2, now.Add(time.Hour)
			q.degradations = []probe.QualityEvent{{At: now, Exec: 7, Weight: probe.DegradeWeight}}
			q.consumed[7] = now
			old.od.degradations = []*degradation{{member: a, exec: 8, started: now, commitAt: now.Add(probe.AttributionDelay)}}
			ms := old.od.member(a)
			ms.cooldown, ms.cooldownUntil, ms.cooldownExec = probe.CooldownMax, now.Add(time.Hour), 7
			ms.signals, ms.obligationDial = []time.Time{now}, now
			noteRound(old.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
			old.od.changePreference(b, causePreferred, "", nil, nil, now)
			old.od.member(c).signals = []time.Time{now}
			old.od.prefer.confirms[c] = confirmation{exec: 10, after: now}
			old.od.summaries[minute] = &summary{dialed: 4, failed: map[string]int{}}
			old.od.search = &searchState{id: 17, t0: now}
			old.od.mutex.Unlock()
			clock.Advance(time.Second)
			if when == "BeforeRetirement" {
				require.Equal(t, probe.MemoryClearResult{Clear: 1, Groups: 1}, ot.coord.ClearMemory())
				require.Len(t, ot.groupEvents("memoryCleared"), 2, "both instances of g processed the notice")
			}
			keys, retain := referencedKeys(ot.proxies, map[string]*onDemand{"g": fresh.od})
			ot.coord.Activate(keys, retain)
			state := old.od.retire(fresh.od)
			if when == "BetweenRetirementAndHandoff" {
				require.Equal(t, 1, ot.coord.ClearMemory().Groups)
				// Only the replacing group observed these results after the clear.
				fresh.od.mutex.Lock()
				fresh.od.qualityOf(a).degradations = []probe.QualityEvent{{At: clock.Now(), Exec: 99, Weight: probe.DegradeWeight}}
				ms := fresh.od.member(a)
				ms.cooldown, ms.cooldownUntil, ms.cooldownExec = probe.CooldownFirst, clock.Now().Add(time.Minute), 99
				noteRound(fresh.od, clock.Now(), tiered{a, tierTimely}, tiered{b, tierSlow})
				fresh.od.changePreference(a, causePreferred, "", nil, nil, clock.Now())
				fresh.od.mutex.Unlock()
			}
			fresh.od.post(inheritNote{state: state})
			ot.settle(fresh)
			if when == "AfterHandoff" {
				require.Equal(t, standingYield, ot.standing(fresh, "A"))
				require.Equal(t, 1, ot.coord.ClearMemory().Groups)
			}
			od := fresh.od
			od.mutex.Lock()
			defer od.mutex.Unlock()
			q = od.qualityOf(a)
			require.Equal(t, standingNormal, q.standing)
			require.Zero(t, q.failedTrials)
			require.Zero(t, q.readmitAt)
			require.Empty(t, q.consumed)
			require.Empty(t, od.degradations)
			if when == "BetweenRetirementAndHandoff" {
				require.Len(t, q.degradations, 1)
				require.Equal(t, uint64(99), q.degradations[0].Exec)
				require.Equal(t, uint64(99), od.member(a).cooldownExec)
				require.Equal(t, a, od.prefer.preferred)
				require.Equal(t, clock.Now(), od.prefer.lastChange)
				require.Len(t, od.prefer.records[[2]C.Proxy{a, b}].results, 1)
			} else {
				require.Empty(t, q.degradations)
				require.Zero(t, od.member(a).cooldown)
				require.Nil(t, od.prefer.preferred)
				require.Empty(t, od.prefer.records)
				require.Zero(t, od.prefer.lastChange)
			}
			require.Equal(t, a, od.current)
			require.Equal(t, uint64(10), od.prefer.confirms[c].exec)
			require.Equal(t, []time.Time{now}, od.member(c).signals)
			require.EqualValues(t, 4, od.summaries[minute].dialed)
			require.NotNil(t, od.search)
			require.Equal(t, uint64(17), od.search.id)
		})
	}
}

func TestClearMemoryNewGroupKeepsPreferenceAcrossTwoReloads(t *testing.T) {
	ot, clock := newClockedODTest(t)
	boundary := ot.coord.ClearMemory()
	require.Equal(t, probe.MemoryClearResult{Clear: 1}, boundary)
	old := ot.ordered("new", OrderReliable, "A", "B")
	ot.settle(old)
	a, b := ot.proxies["A"], ot.proxies["B"]
	old.od.mutex.Lock()
	var id uint64
	for i := 0; i < probe.CompareMinimum; i++ {
		clock.Advance(time.Minute)
		id = noteRound(old.od, clock.Now(), tiered{a, tierSlow}, tiered{b, tierTimely})
	}
	decision := old.od.judgeReliable(&compareRound{id: id, c: a}, []*participant{{member: a}, {member: b}}, clock.Now())
	old.od.mutex.Unlock()
	require.Equal(t, "preferred", decision["change"])
	for i := 0; i < 2; i++ {
		ot.coord.BeginPublish()
		fresh := ot.buildOption(GroupCommonOption{Name: "new", Order: OrderReliable}, "A", "B")
		ot.replace(old, fresh)
		ot.settle(fresh)
		require.Equal(t, "B", ot.preferred(fresh))
		require.Equal(t, "B", ot.current(fresh))
		fresh.od.mutex.Lock()
		clear := fresh.od.memory.Clear
		fresh.od.mutex.Unlock()
		require.Equal(t, boundary.Clear, clear)
		old = fresh
	}
}

func TestClearMemoryChildRepublishesItsNewProof(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	child := ot.build("child", "A", "B", "REJECT")
	parent := ot.build("parent", "X", "child", "REJECT")
	ot.activate(child, parent)
	od := child.od
	a, b := ot.proxies["A"], ot.proxies["B"]
	member := ot.proxies["child"]
	key := probe.Key{Target: member, Cond: parent.od.cond}
	ot.coord.Manual(context.Background(), b, od.cond, "test")
	hold := ot.runner.holdOn("B")
	defer close(hold)
	clock.Advance(time.Hour)
	ot.coord.Manual(context.Background(), a, od.cond, "test")
	ot.settle(child)
	od.mutex.Lock()
	q := od.qualityOf(a)
	q.standing, q.readmitAt = standingYield, clock.Now().Add(time.Hour)
	od.computeOrder()
	od.reselect(clock.Now(), selectionCause{Reason: causeDemoted})
	od.mutex.Unlock()
	ot.settle(child)
	ot.settle(parent)
	before := ot.coord.Evidence(key)
	require.Equal(t, b, od.published.Load().preferred)
	require.Equal(t, ot.coord.Evidence(probe.Key{Target: b, Cond: od.cond}).Started, before.Started)
	parent.od.mutex.Lock()
	trusted := parent.od.trusted(member, clock.Now())
	parent.od.mutex.Unlock()
	require.False(t, trusted, "the previous path has a stale proof")
	require.Equal(t, 2, ot.coord.ClearMemory().Groups)
	ot.settle(parent)
	require.Equal(t, a, od.published.Load().preferred)
	parent.od.mutex.Lock()
	defer parent.od.mutex.Unlock()
	after := parent.od.view[member]
	require.Greater(t, after.Version, before.Version)
	require.Equal(t, ot.coord.Evidence(probe.Key{Target: a, Cond: od.cond}).Started, after.Started)
	require.True(t, parent.od.trusted(member, clock.Now()))
	require.Nil(t, parent.od.search)
	require.Empty(t, ot.coord.Failures(key))
	require.Zero(t, ot.runner.count("child"))
}

func TestClearMemoryPendingGroupReselectsWhenItReachesTheNotice(t *testing.T) {
	ot, clock := newClockedODTest(t)
	f := ot.group("delayed", "A", "B")
	ot.settle(f)
	od := f.od
	od.halt()
	a := ot.proxies["A"]
	q := od.qualityOf(a)
	q.standing, q.readmitAt = standingYield, clock.Now()
	od.computeOrder()
	od.reselect(clock.Now(), selectionCause{Reason: causeDemoted})
	require.Equal(t, ot.proxies["B"], od.current)
	require.Equal(t, probe.MemoryClearResult{Clear: 1, Pending: 1}, ot.coord.ClearMemory())
	// A batch drained before the clear can finish after the shared counts changed.
	od.round([]any{recheck{}}, false)
	require.Equal(t, standingYield, od.standingOf(a))
	od.round(od.drain(), false)
	require.Equal(t, standingNormal, od.standingOf(a))
	require.Equal(t, a, od.current)
	require.Equal(t, causeMemoryCleared, od.snap.Load().cause.Reason)
	require.Len(t, ot.groupEvents("memoryCleared"), 1)
}

func TestClearMemoryMovesASuspendedRouteForItsOwnReason(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B", "C")
	f := ot.group("g", "A", "B", "C", "REJECT")
	ot.settle(f)
	for _, name := range []string{"B", "C"} {
		ot.coord.Manual(context.Background(), ot.proxies[name], f.od.cond, "test")
	}
	ot.settle(f)
	od := f.od
	od.halt()
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	now := od.now()
	od.qualityOf(b).standing = standingYield
	od.computeOrder()
	require.Equal(t, a, od.current)
	od.suspend(&suspendState{member: a, since: now, reason: "slowTest", after: now, exec: 88}, now)
	require.Equal(t, c, od.route)
	require.Equal(t, probe.MemoryClearResult{Clear: 1, Pending: 1}, ot.coord.ClearMemory())
	od.round(od.drain(), false)
	require.Equal(t, a, od.current)
	require.NotNil(t, od.suspended)
	require.Equal(t, b, od.route, "B is back before C")
	require.Equal(t, selectionCause{Reason: causeMemoryCleared}, od.routeCause)
	selections := ot.groupEvents("selection")
	last := selections[len(selections)-1]
	require.Equal(t, causeMemoryCleared, last["cause"].(map[string]any)["reason"])
	require.Nil(t, last["cause"].(map[string]any)["exec"])
}
