package outboundgroup

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func (ot *odTest) strict(name string, members ...string) *Fallback {
	return ot.buildOption(GroupCommonOption{Name: name, StrictOrder: true}, members...)
}

func (ot *odTest) eventsOf(group, event string) []map[string]any {
	var list []map[string]any
	for _, e := range ot.groupEvents(event) {
		if e["group"] == group {
			list = append(list, e)
		}
	}
	return list
}

func (ot *odTest) needsOf(group, target, source string) []map[string]any {
	var list []map[string]any
	for _, e := range ot.records("need") {
		if e["group"] == group && e["target"].(map[string]any)["name"] == target && (source == "" || e["source"] == source) {
			list = append(list, e)
		}
	}
	return list
}

func TestOnDemandAPathFailureStaysInsideTheMemberGroup(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A1", "A2", "B")
	exit := ot.build("exit", "A1", "A2", "REJECT")
	top := ot.strict("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.eventually(func() bool { return ot.current(top) == "exit" && ot.current(exit) == "A1" }, "selects exit through A1")
	require.True(t, ot.coord.Delegated(probe.Key{Target: ot.proxies["exit"], Cond: top.od.cond}))

	ot.runner.set(false, "A1")
	// a connection through both failed: each group heard of it
	ot.signal(top, "exit")
	ot.signal(exit, "A1")
	ot.eventually(func() bool { return ot.current(exit) == "A2" }, "the exit moves to its next path")
	ot.idle(top)
	require.Equal(t, "exit", ot.current(top), "the exit stays")
	require.Zero(t, ot.runner.count("exit"), "nothing is tested through the member group")
	published := ot.eventsOf("exit", "availability")
	require.NotEmpty(t, published)
	last := published[len(published)-1]
	require.Equal(t, "alive", last["state"])
	require.Equal(t, "A2", last["preferred"].(map[string]any)["name"])
}

func TestOnDemandAMemberGroupWithNothingLeftHandsOver(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "B")
	exit := ot.build("exit", "A1", "A2", "REJECT")
	top := ot.strict("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.eventually(func() bool { return ot.current(top) == "exit" }, "selects exit")
	ot.signal(exit, "A1")
	ot.eventually(func() bool { return ot.current(top) == "B" }, "the next exit takes over once this one has nothing left")
	require.Equal(t, "REJECT", ot.current(exit))
	require.Zero(t, ot.runner.count("exit"))
	published := ot.eventsOf("exit", "availability")
	last := published[len(published)-1]
	require.Equal(t, "dead", last["state"])
	starts := ot.eventsOf("top", "searchStart")
	require.Len(t, starts, 1)
	require.Equal(t, last["proof"], starts[0]["t0"], "the search starts from the failure the exit published")

	// In use, the group forwards the recovery of the exit, which tests its paths again
	// without being in use itself.
	conn, err := top.DialContext(context.Background(), echo(t))
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool {
		return len(ot.needsOf("top", "exit", probe.SourceRecover)) > 0 && len(ot.needsOf("exit", "A1", probe.SourceRecover)) > 0
	}, "the recovery is forwarded to the paths")
	ot.runner.set(true, "A1")
	ot.coord.Manual(context.Background(), ot.proxies["A1"], exit.od.cond, "test")
	ot.eventually(func() bool { return ot.current(top) == "exit" && ot.current(exit) == "A1" }, "the exit is back once a path is")
	require.Zero(t, ot.runner.count("exit"))
}

func TestOnDemandAMemberGroupEndingAtDirectEndsTheSearch(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	exit := ot.build("exit", "A1", "DIRECT")
	top := ot.build("top", "X", "exit", "B", "REJECT")
	ot.activate(exit, top)
	require.Equal(t, "A1", ot.current(exit))
	ot.eventually(func() bool { return ot.current(top) == "X" }, "selects X")
	// X fails: the search confirms the exit, whose path fails too and leaves it at direct
	ot.signal(top, "X")
	ot.eventually(func() bool { return ot.current(top) == "exit" && ot.current(exit) == "DIRECT" }, "the exit ends at direct")
	ot.eventually(func() bool { return len(ot.eventsOf("top", "searchEnd")) == 1 }, "the search ends")
	require.Equal(t, "exhausted", ot.eventsOf("top", "searchEnd")[0]["result"])
	require.Zero(t, ot.runner.count("exit"))
	published := ot.eventsOf("exit", "availability")
	require.Equal(t, true, published[len(published)-1]["terminal"])
	for _, need := range ot.needsOf("top", "exit", "") {
		if need["state"] == "reused" {
			return
		}
	}
	t.Fatal("the confirmation of the exit ends by what it published")
}

func TestOnDemandABackupExitConfirmsOnlyThePathItPrefers(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A1", "A2", "C1", "C2")
	exit := ot.build("exit", "A1", "A2", "REJECT")
	backup := ot.build("backup", "C1", "C2", "REJECT")
	top := ot.strict("top", "exit", "backup", "REJECT")
	ot.activate(exit, backup, top)
	ot.eventually(func() bool { return ot.current(top) == "exit" }, "selects exit")
	conn, err := top.DialContext(context.Background(), echo(t))
	require.NoError(t, err)
	use(t, conn)
	ot.eventually(func() bool { return ot.runner.count("C1") > 0 }, "the standby exit confirms its path")
	ot.idle(top)
	require.Zero(t, ot.runner.count("C2"), "a standby exit keeps no standbys of its own")
	require.Zero(t, ot.runner.count("backup"))
	require.NotEmpty(t, ot.needsOf("backup", "C1", probe.SourceDelegate))
}

func TestOnDemandAMemberGroupReachingAPassOutboundIsTestedThrough(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["PASS"] = adapter.NewProxy(outbound.NewPass())
	ot.runner.set(true, "B")
	child := ot.build("child", "A1", "PASS")
	top := ot.build("top", "child", "B", "REJECT")
	ot.activate(child, top)
	require.False(t, ot.coord.Delegated(probe.Key{Target: ot.proxies["child"], Cond: top.od.cond}))
	ot.eventually(func() bool { return ot.current(top) == "child" }, "selects child")
	ot.signal(top, "child")
	ot.eventually(func() bool { return ot.runner.count("child") > 0 }, "it is tested through, as before")
}

func TestOnDemandDelegationLooksThroughEveryGroupOnTheWay(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["PASS"] = adapter.NewProxy(outbound.NewPass())
	inner := ot.build("inner", "A1", "PASS")
	outer := ot.build("outer", "inner", "A2")
	ot.activate(inner, outer)
	outer.od.mutex.Lock()
	defer outer.od.mutex.Unlock()
	require.False(t, outer.od.isDelegable(), "a pass outbound deeper down counts as well")
	require.False(t, ot.coord.Delegated(probe.Key{Target: ot.proxies["inner"], Cond: inner.od.cond}))
}

func TestPublishOrderPutsTheListedGroupsFirst(t *testing.T) {
	ot := newODTest(t)
	exit := ot.build("z-exit", "A1")
	top := ot.build("a-top", "z-exit", "B")
	proxies := map[string]C.Proxy{"z-exit": ot.proxies["z-exit"], "a-top": ot.proxies["a-top"], "A1": ot.proxies["A1"]}
	require.Equal(t, []string{"z-exit", "a-top"}, publishOrder(proxies))
	_, _ = exit, top
}

func TestOnDemandAMemberGroupEndingAtDirectBoundsTheOrder(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	ot.runner.set(true, "A", "C")
	exit := ot.build("exit", "E1", "DIRECT")
	top := ot.build("top", "A", "exit", "C", "REJECT")
	ot.activate(exit, top)
	top.od.mutex.Lock()
	top.od.qualityOf(ot.proxies["A"]).standing = standingYield
	top.od.computeOrder()
	top.od.mutex.Unlock()
	require.Equal(t, []string{"exit", "C", "A", "REJECT"}, names(top.od))
	ot.signal(exit, "E1")
	ot.eventually(func() bool { return ot.current(exit) == "DIRECT" }, "the exit ends at direct")
	ot.eventually(func() bool {
		order := names(top.od)
		return order[0] == "A" && order[1] == "exit"
	}, "a member yielding stays before the member that is always there")
	ot.eventually(func() bool { return ot.current(top) == "A" }, "and is preferred to it")
}

func TestOnDemandAMemberGroupUnavailableByPolicyStartsNoSearch(t *testing.T) {
	ot := newODTest(t)
	ot.node("B1", "jinkela")
	ot.runner.set(true, "B1", "B")
	exit := ot.build("exit", "A1", "B1", "REJECT")
	top := ot.strict("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.signal(exit, "A1")
	ot.eventually(func() bool { return ot.current(exit) == "B1" }, "the exit moves to its second path")
	ot.idle(top)
	require.Empty(t, ot.eventsOf("top", "searchStart"))
	ot.coord.Exclusions().Replace([]probe.ExclusionEntry{{Pool: "jinkela"}})
	ot.eventually(func() bool { return ot.current(top) == "B" }, "the next exit takes over")
	ot.idle(top)
	require.Empty(t, ot.eventsOf("top", "searchStart"), "an exclusion is no failure a search answers")
	published := ot.eventsOf("exit", "availability")
	require.Equal(t, "dead", published[len(published)-1]["state"])
}

func TestOnDemandAStrictGroupDropsTheStandingsItInherits(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	old := ot.group("g", "A", "B", "REJECT")
	old.od.mutex.Lock()
	old.od.qualityOf(ot.proxies["A"]).standing = standingYield
	old.od.computeOrder()
	old.od.mutex.Unlock()
	require.Equal(t, []string{"B", "A", "REJECT"}, names(old.od))
	fresh := ot.strict("g", "A", "B", "REJECT")
	ot.replace(old, fresh)
	ot.eventually(func() bool {
		fresh.od.mutex.Lock()
		defer fresh.od.mutex.Unlock()
		return fresh.od.activated
	}, "takes over")
	ot.idle(fresh)
	require.Equal(t, []string{"A", "B", "REJECT"}, names(fresh.od))
	require.Equal(t, standingNormal, ot.standing(fresh, "A"))
}

func TestARouteIsAsCertainAsTheGroupsItGoesThroughFindItNow(t *testing.T) {
	until := time.Now().Add(time.Minute)
	leaf, middle := &onDemand{}, &onDemand{}
	leaf.snap.Store(&snapshot{certainUntil: until})
	middle.snap.Store(&snapshot{certainUntil: time.Now().Add(-time.Minute), certainChild: leaf})
	top := &snapshot{certainUntil: time.Now().Add(-time.Minute), certainChild: middle}
	require.Equal(t, until, top.certainNow(), "read from the deepest group, not as the middle one last saw it")
	middle.snap.Store(&snapshot{})
	require.True(t, top.certainNow().IsZero(), "a level kept from certainty keeps it")
}

func TestOnDemandSetsAsideToAMemberAfterADirectOne(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	ot.runner.set(true, "A", "DIRECT", "B")
	f := ot.group("g", "A", "DIRECT", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	require.Equal(t, ot.proxies["B"], f.od.suspendCandidate(), "the direct member is no candidate, and no bound")
}

func TestTheCensusTestsTheDirectMembersTheGroupsTest(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	ahead := ot.build("ahead", "DIRECT", "A")
	behind := ot.build("behind", "B", "DIRECT")
	keys := censusKeys(map[string]*onDemand{"ahead": ahead.od, "behind": behind.od})
	var tested []string
	for _, key := range keys {
		if key.Target == ot.proxies["DIRECT"] {
			tested = append(tested, "DIRECT")
		}
	}
	require.Len(t, keys, 3)
	require.Equal(t, []string{"DIRECT"}, tested, "once, for the group testing it")
}

func TestSettlingTheGroupsCarriesARestoredPinUpwards(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A1", "B")
	exit := ot.build("exit", "A1", "REJECT")
	top := ot.strict("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.eventually(func() bool { return ot.current(top) == "exit" }, "selects exit")
	exit.od.setPinned("REJECT", "restore")
	settleGroups([]*onDemand{exit.od, top.od}, time.Second)
	require.Equal(t, "B", ot.current(top), "the group listing it selects by what it published once settled")
}

func TestOnDemandAMemberGroupIsWitnessedOnTrialByWhatItPublishes(t *testing.T) {
	ot := newODTest(t)
	exit := ot.build("exit", "A1", "REJECT")
	top := ot.build("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.idle(top)
	p := ot.proxies["exit"]
	top.od.mutex.Lock()
	defer top.od.mutex.Unlock()
	now := top.od.now()
	q := top.od.qualityOf(p)
	q.trial = &trialState{started: now.Add(-time.Minute), pending: map[uint64]time.Time{7: now.Add(-time.Second)}}
	top.od.handle(successNote{member: p, cond: top.od.cond, dialed: now}, now)
	require.False(t, q.trial.witness, "a connection through it is no witness")
	// it ends at a direct outbound now: what the trial waits for is settled all the same
	ev := top.od.view[p]
	ev.Terminal = true
	top.od.view[p] = ev
	top.od.handle(probe.Attributed{Exec: 7, Key: probe.Key{Target: p, Cond: top.od.cond}, Outcome: probe.OutcomeMember}, now)
	require.Empty(t, q.trial.pending)
	require.True(t, q.trial.failed)
}

func TestOnDemandAGroupTestOfTheUpperGroupTestsThePathsOfTheExits(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A1", "A2", "B")
	exit := ot.build("exit", "A1", "A2", "REJECT")
	top := ot.build("top", "exit", "B", "REJECT")
	ot.activate(exit, top)
	ot.eventually(func() bool { return ot.current(exit) == "A1" }, "selects A1")
	old := probe.Default
	probe.Default = ot.coord
	defer func() { probe.Default = old }()
	manual := probe.Condition{URL: top.od.cond.URL, Expected: top.od.cond.Expected, Timeout: 10 * time.Second}
	_, err := GroupManualTest(context.Background(), top, manual.URL, nil)
	require.NoError(t, err)
	require.Equal(t, 1, ot.runner.count("A1"), "the exit is tested by the path it prefers")
	require.Zero(t, ot.runner.count("A2"))
	require.Zero(t, ot.runner.count("exit"))
	ot.eventually(func() bool {
		published := ot.eventsOf("exit", "availability")
		return len(published) > 0 && published[len(published)-1]["state"] == "alive"
	}, "the result reaches the exit and the group listing it")
	ot.eventually(func() bool {
		top.od.mutex.Lock()
		defer top.od.mutex.Unlock()
		return top.od.view[ot.proxies["exit"]].State == probe.Alive
	}, "the upper group sees the exit alive")
}

func TestOnDemandStandbysAreNoMoreThanTheMembersAfterThePreferredOne(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	ot.runner.set(true, "DIRECT", "X")
	f := ot.group("g", "DIRECT", "X")
	ot.coord.Manual(context.Background(), ot.proxies["DIRECT"], f.od.cond, "test")
	ot.coord.Manual(context.Background(), ot.proxies["X"], f.od.cond, "test")
	ot.eventually(func() bool { return ot.current(f) == "DIRECT" }, "prefers the direct member")
	ot.idle(f)
	view := f.od.strategyView(f.od.now())
	data, err := json.Marshal(view["headline"])
	require.NoError(t, err)
	require.Contains(t, string(data), `"1/1`, "one member after the preferred one: one standby at most")
	require.NotContains(t, string(data), `"1/2`)
}
