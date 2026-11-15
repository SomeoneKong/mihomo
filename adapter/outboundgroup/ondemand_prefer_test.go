package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

// chainAdapter is a node dialing through a dialer proxy.
type chainAdapter struct {
	*outbound.Direct
	dialer string
}

func (a *chainAdapter) Type() C.AdapterType { return C.Vless }
func (a *chainAdapter) ProxyInfo() C.ProxyInfo {
	info := a.Direct.ProxyInfo()
	info.DialerProxy = a.dialer
	return info
}

func (ot *odTest) chainNode(name, dialer string) C.Proxy {
	p := adapter.NewProxy(&chainAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name}), dialer: dialer})
	ot.proxies[name] = p
	return p
}

func (ot *odTest) ordered(name, order string, members ...string) *Fallback {
	f := ot.buildOption(GroupCommonOption{Name: name, Order: order}, members...)
	ot.activate(f)
	return f
}

type tiered struct {
	p C.Proxy
	t tier
}

// noteRound records a round in which the members found what tiers tell, all at once.
func noteRound(od *onDemand, at time.Time, list ...tiered) uint64 {
	id := compareRounds.Add(1)
	for i, x := range list {
		for _, y := range list[i+1:] {
			od.note(x.p, y.p, pairResult{round: id, at: at, sides: [2]side{{tier: x.t, started: at}, {tier: y.t, started: at}}})
		}
	}
	return id
}

func (ot *odTest) preferred(f *Fallback) string {
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	if p := f.od.prefer.preferred; p != nil {
		return p.Name()
	}
	return ""
}

// compareRound runs a round of the group: in use, the next round due, the tests done, and the
// window of the round over.
func (ot *odTest) compareRound(f *Fallback, clock *fakeClock) map[string]any {
	ot.t.Helper()
	before := len(ot.groupEvents("compare"))
	clock.Advance(probe.CompareEvery * 2)
	// the group takes the new time in before it comes into use at it
	ot.recheck(f)
	f.od.noteUse("dial")
	ot.recheck(f)
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		r := f.od.prefer.round
		if r == nil {
			return len(ot.groupEvents("compare")) > before
		}
		for _, p := range r.parts {
			if !p.done && !p.missed {
				return false
			}
		}
		return true
	}, "the tests of the round end")
	clock.Advance(2 * probe.PairWindow)
	ot.recheck(f)
	ot.eventually(func() bool { return len(ot.groupEvents("compare")) > before }, "the round ends")
	events := ot.groupEvents("compare")
	return events[len(events)-1]
}

func TestOnDemandPrefersAMemberThatDoesBetterSideBySide(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B", "C")
	ot.runner.setFirst(1500*time.Millisecond, "A")
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	for i := 0; i < probe.CompareMinimum-1; i++ {
		e := ot.compareRound(f, clock)
		require.Equal(t, "done", e["end"])
		require.Len(t, e["members"], 3)
		require.Empty(t, ot.preferred(f), "not before enough results")
	}
	e := ot.compareRound(f, clock)
	decision := e["decision"].(map[string]any)
	require.Equal(t, "preferred", decision["change"])
	require.Equal(t, "B", ot.preferred(f), "of two members doing as well, the first in config order")
	ot.eventually(func() bool { return ot.current(f) == "B" }, "B comes first")
	require.Equal(t, []string{"B", "A", "C", "REJECT"}, names(f.od))
	preference := ot.groupEvents("preference")
	require.Len(t, preference, 1)
	require.Equal(t, causePreferred, preference[0]["reason"])
	require.Equal(t, map[string]any{"zh": "同场测试", "en": "side by side"}, e["display"].(map[string]any)["categoryText"])
	require.Equal(t, map[string]any{"zh": "偏好", "en": "preference"}, preference[0]["display"].(map[string]any)["categoryText"])
	var selected bool
	for _, s := range ot.groupEvents("selection") {
		if s["cause"].(map[string]any)["reason"] == causePreferred {
			selected = true
		}
	}
	require.True(t, selected, "the selection tells why it moved")
	// the tests only compared: A is as it was
	require.NotEqual(t, probe.Dead, f.od.coord.Evidence(probe.Key{Target: ot.proxies["A"], Cond: f.od.cond}).State)
}

func TestOnDemandKeepsAMemberThatWonOnce(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	noteRound(f.od, now.Add(-4*time.Hour), tiered{a, tierSlow}, tiered{b, tierTimely})
	noteRound(f.od, now.Add(-3*time.Hour), tiered{a, tierTimely}, tiered{b, tierSlow})
	noteRound(f.od, now.Add(-2*time.Hour), tiered{a, tierSlow}, tiered{b, tierTimely})
	id := noteRound(f.od, now.Add(-time.Hour), tiered{a, tierSlow}, tiered{b, tierTimely})
	r := &compareRound{id: id, c: a}
	field := []*participant{{member: a}, {member: b}}
	require.Nil(t, f.od.judgeReliable(r, field, now)["change"], "A won one of the latest four")
	// a failure is worse than a slow success: B failing where A is slow is a win of A
	id = noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierFailed})
	r.id = id
	require.Nil(t, f.od.judgeReliable(r, field, now)["change"])
}

func TestOnDemandOrderTimelyTiers(t *testing.T) {
	defaultThreshold := newPreferState(OrderReliable, probe.CompareTimely)
	raisedThreshold := newPreferState(OrderReliable, 2*time.Second)
	first := func(ms int) C.ProbeResult {
		return C.ProbeResult{FirstSatisfied: true, FirstElapsed: time.Duration(ms) * time.Millisecond}
	}
	require.Equal(t, tierTimely, defaultThreshold.tierOf(first(300)))
	require.Equal(t, tierTimely, defaultThreshold.tierOf(first(650)))
	require.Equal(t, tierTimely, defaultThreshold.tierOf(first(1000)))
	require.Equal(t, tierSlow, defaultThreshold.tierOf(first(1200)))
	require.Equal(t, tierTimely, raisedThreshold.tierOf(first(1500)))
	require.Equal(t, tierTimely, raisedThreshold.tierOf(first(2000)))
	require.Equal(t, tierSlow, raisedThreshold.tierOf(first(3000)))
	for _, ps := range []*preferState{defaultThreshold, raisedThreshold} {
		require.Equal(t, tierFailed, ps.tierOf(C.ProbeResult{Satisfied: true}))
	}
}

func TestOnDemandWaitsAnHourBetweenChangesOfThePreference(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.prefer.lastChange = now.Add(-30 * time.Minute)
	var id uint64
	for i := 3; i > 0; i-- {
		id = noteRound(f.od, now.Add(-time.Duration(i)*time.Minute), tiered{a, tierSlow}, tiered{b, tierTimely})
	}
	r := &compareRound{id: id, c: a}
	d := f.od.judgeReliable(r, []*participant{{member: a}, {member: b}}, now)
	require.Nil(t, d["change"])
	require.Equal(t, "stay", d["why"])
	f.od.prefer.lastChange = now.Add(-time.Hour)
	require.Equal(t, "preferred", f.od.judgeReliable(r, []*participant{{member: a}, {member: b}}, now)["change"])
	require.Equal(t, b, f.od.prefer.preferred)
}

func TestOnDemandDoesNotReplaceAPreferenceItsStandInDoesBetterThan(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	// C is preferred but dead: B stands in
	f.od.prefer.preferred = c
	f.od.prefer.lastChange = now.Add(-2 * time.Hour)
	f.od.view[c] = probe.Evidence{State: probe.Dead, NextAllowed: now.Add(time.Hour)}
	f.od.computeOrder()
	f.od.reselect(now, selectionCause{Reason: causeProbe})
	require.Equal(t, a, f.od.current)
	var id uint64
	for i := 3; i > 0; i-- {
		id = noteRound(f.od, now.Add(-time.Duration(i)*time.Minute), tiered{a, tierSlow}, tiered{b, tierTimely})
	}
	d := f.od.judgeReliable(&compareRound{id: id, c: a}, []*participant{{member: a}, {member: b}}, now)
	require.Equal(t, "awaitPreferred", d["why"])
	require.Equal(t, c, f.od.prefer.preferred)
}

func TestOnDemandAPreferenceEnds(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	prefer := func(now time.Time) {
		noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
		f.od.prefer.preferred, f.od.prefer.lastChange = b, now
		f.od.computeOrder()
		f.od.reselect(now, selectionCause{Reason: causePreferred})
	}
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()

	prefer(now)
	require.Equal(t, b, f.od.current)
	// only dead, it stays preferred
	f.od.view[b] = probe.Evidence{State: probe.Dead, NextAllowed: now.Add(time.Hour)}
	f.od.updatePrefer(now)
	require.Equal(t, b, f.od.prefer.preferred)
	f.od.view[b] = probe.Evidence{State: probe.Alive}

	f.od.qualityOf(b).standing = standingYield
	f.od.updatePrefer(now)
	require.Nil(t, f.od.prefer.preferred, "it yields")
	f.od.qualityOf(b).standing = standingNormal

	prefer(now)
	f.od.qualityOf(b).standing = standingTrial
	f.od.updatePrefer(now)
	require.Nil(t, f.od.prefer.preferred, "it is on trial")
	f.od.qualityOf(b).standing = standingNormal

	prefer(now)
	f.od.member(b).excluded = true
	f.od.updatePrefer(now)
	require.Nil(t, f.od.prefer.preferred, "it is excluded")
	f.od.member(b).excluded = false

	prefer(now)
	f.od.updatePrefer(now.Add(probe.CompareExpiry))
	require.Nil(t, f.od.prefer.preferred, "no result of it for a day")
	require.Equal(t, a, f.od.current, "back to config order")
	var whys []any
	for _, e := range ot.groupEvents("preference") {
		whys = append(whys, e["why"])
	}
	require.Equal(t, []any{"yield", "trial", "excluded", "stale"}, whys)
}

func TestOnDemandAPinLeavesThePreferenceOutOfEffect(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "REJECT")
	a, c := ot.proxies["A"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	noteRound(f.od, f.od.now(), tiered{a, tierSlow}, tiered{c, tierTimely})
	f.od.prefer.preferred = c
	f.od.computeOrder()
	f.od.mutex.Unlock()
	require.Equal(t, []string{"C", "A", "B", "REJECT"}, names(f.od))
	f.od.post(pinNote{name: "B", source: "user"})
	ot.idle(f)
	require.Equal(t, []string{"B", "A", "C", "REJECT"}, names(f.od), "the config order once B is pinned")
	f.od.post(pinNote{name: "", source: "user"})
	ot.idle(f)
	require.Equal(t, []string{"C", "A", "B", "REJECT"}, names(f.od))
}

func TestOnDemandAComparisonFailureIsConfirmedAsAnyTest(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	// B is down from now on: the confirmation finds it so, whenever it runs
	ot.runner.set(false, "B")
	f.od.mutex.Lock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	ok := C.ProbeResult{Satisfied: true, FirstSatisfied: true, FirstElapsed: 10 * time.Millisecond}
	failed := C.ProbeResult{ErrorClass: C.ProbeErrorDial}
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, windowEnd: now.Add(probe.PairWindow), parts: []*participant{
		{member: a, use: useCurrent, exec: 1, started: now, done: true, result: ok},
		{member: b, use: useRotation, exec: 2, started: now, done: true, result: failed, compared: true, ended: now},
	}}
	f.od.updatePrefer(now.Add(2 * probe.PairWindow))
	_, held := f.od.prefer.confirms[b]
	// idle, the group asks for nothing else of B: only the confirmation tests it
	f.od.lastActive.Store(0)
	f.od.mutex.Unlock()
	require.True(t, held)
	e := ot.groupEvents("compare")
	require.Len(t, e[len(e)-1]["confirms"], 1)

	// the confirmation is a test as any other, which finds B dead
	ot.recheck(f)
	ot.eventually(func() bool {
		return f.od.coord.Evidence(probe.Key{Target: b, Cond: f.od.cond}).State == probe.Dead
	}, "the confirmation finds B dead")
	ot.recheck(f)
	confirms := ot.groupEvents("confirm")
	require.Len(t, confirms, 1)
	require.Equal(t, "tested", confirms[0]["end"])
	var confirmNeed, confirmProbe any
	for _, n := range ot.records("need") {
		if n["source"] == probe.SourceConfirm && n["state"] == "created" {
			confirmNeed = n["need"]
		}
		if confirmNeed != nil && n["need"] == confirmNeed && n["state"] == "started" {
			confirmProbe = n["probe"]
		}
	}
	require.NotNil(t, confirmProbe, "a need of its own")
	require.Equal(t, confirmProbe, confirms[0]["by"])
}

func TestOnDemandATestSinceAComparisonFailureDoesForItsConfirmation(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	ok := C.ProbeResult{Satisfied: true, FirstSatisfied: true, FirstElapsed: 10 * time.Millisecond}
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, windowEnd: now.Add(probe.PairWindow), parts: []*participant{
		{member: a, use: useCurrent, exec: 1, started: now, done: true, result: ok},
		{member: b, use: useRotation, exec: 2, started: now, done: true, result: C.ProbeResult{ErrorClass: C.ProbeErrorDial}, compared: true, ended: now},
	}}
	// a standby verification started right after the failure, before the group heard of it
	f.od.view[b] = probe.Evidence{State: probe.Alive, Started: now, Exec: 3}
	f.od.updatePrefer(now.Add(2 * probe.PairWindow))
	require.Empty(t, f.od.prefer.confirms)
}

func TestOnDemandARoundIsVoidWhenAllFirstRequestsFail(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	ot.runner.mutex.Lock()
	ot.runner.firstOnly["A"] = true
	ot.runner.mutex.Unlock()
	ot.runner.set(false, "B")
	ot.runner.set(true, "A")
	ot.runner.mutex.Lock()
	delete(ot.runner.firstOnly, "A")
	ot.runner.alive["A"] = false
	ot.runner.mutex.Unlock()
	e := ot.compareRound(f, clock)
	require.Equal(t, "void", e["end"])
	require.Equal(t, "allFailed", e["reason"])
	require.Empty(t, e["confirms"])
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	require.Empty(t, f.od.prefer.records)
	require.Empty(t, f.od.prefer.confirms)
}

func TestOnDemandARoundIsVoidOnceTheNetworkSeemsOutMeanwhile(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a := ot.proxies["A"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}}}
	// out and normal again in one batch: the round is void all the same
	f.od.latchRound(probe.NetworkChanged{State: probe.NetworkPoor})
	f.od.latchRound(probe.NetworkChanged{State: probe.NetworkNormal})
	f.od.updatePrefer(now)
	f.od.mutex.Unlock()
	e := ot.groupEvents("compare")
	require.Len(t, e, 1)
	require.Equal(t, "network", e[0]["reason"])
}

func TestOnDemandAChallengerStartingLateIsMissedAndGoesFirstNextRound(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "D", "E", "REJECT")
	a, b, c, d := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"], ot.proxies["D"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{
		{member: a, use: useCurrent}, {member: b, use: useRotation}, {member: c, use: useRotation}, {member: d, use: useRotation},
	}}
	f.od.prefer.round = r
	key := func(p C.Proxy) probe.Key { return probe.Key{Target: p, Cond: f.od.cond} }
	// B starts before A, but more than a second before
	f.od.onBegan(probe.Began{Exec: 1, Key: key(b), Started: now.Add(100 * time.Millisecond)})
	f.od.onBegan(probe.Began{Exec: 2, Key: key(a), Started: now.Add(1500 * time.Millisecond)})
	f.od.onBegan(probe.Began{Exec: 3, Key: key(c), Started: now.Add(2400 * time.Millisecond)})
	f.od.onBegan(probe.Began{Exec: 4, Key: key(d), Started: now.Add(2600 * time.Millisecond)})
	require.True(t, r.part(b).missed, "more than a second before A")
	require.False(t, r.part(c).missed, "within a second after A")
	require.True(t, r.part(d).missed, "more than a second after A")
	ok := C.ProbeResult{Satisfied: true, FirstSatisfied: true, FirstElapsed: 10 * time.Millisecond}
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: key(a), Result: ok})
	f.od.preferCompletion(probe.Completion{Exec: 3, Key: key(c), Result: ok})
	f.od.updatePrefer(now.Add(3 * time.Second))
	require.NotNil(t, f.od.prefer.round, "C may still be heard of within a second past the window")
	f.od.updatePrefer(now.Add(4 * time.Second))
	require.Nil(t, f.od.prefer.round)
	require.True(t, f.od.prefer.owed[b])
	require.True(t, f.od.prefer.owed[d])
	require.Len(t, f.od.prefer.records, 2, "A and C, both ways")
	parts := f.od.challengers(a, false, now.Add(4*time.Second))
	var picked []string
	for _, p := range parts {
		picked = append(picked, p.member.Name())
	}
	require.Equal(t, "B", picked[0])
	require.Equal(t, "D", picked[1])
}

func TestOnDemandARoundIsCanceledWhenThePreferredMemberDoesNotStart(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.updatePrefer(now.Add(probe.CompareStart + compareSettle - time.Millisecond))
	require.NotNil(t, f.od.prefer.round, "its start may still be heard of")
	f.od.updatePrefer(now.Add(probe.CompareStart + compareSettle))
	require.Nil(t, f.od.prefer.round)
	require.Equal(t, "notStarted", ot.groupEvents("compare")[0]["reason"])
	require.Equal(t, now.Add(probe.CompareStart+compareSettle+compareRetry), f.od.prefer.retryAt)
}

func TestOnDemandByCostGoesBeforeACheaperMemberOnlyOnABasisOverEach(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliableCost, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	var id uint64
	for i := 3; i > 0; i-- {
		id = noteRound(f.od, now.Add(-time.Duration(i)*time.Minute), tiered{a, tierSlow}, tiered{c, tierTimely})
	}
	r := &compareRound{id: id, c: a}
	field := []*participant{{member: a}, {member: c}}
	require.Nil(t, f.od.judgeCost(r, field, now)["change"], "C never did better than B")
	// the next round pairs C with B, which it was never compared with
	parts := f.od.challengers(a, false, now)
	require.Equal(t, c, parts[0].member)
	require.Equal(t, useContrast, parts[0].use)
	require.Equal(t, b, parts[1].member)
	require.Equal(t, useContrast, parts[1].use)
	require.Len(t, parts, 2, "the third is in turn: none left")

	id = noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierTimely}, tiered{c, tierTimely})
	r.id = id
	field = append(field, &participant{member: b})
	require.Nil(t, f.od.judgeCost(r, field, now)["change"], "C and B even: no basis")
	id = noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierSlow}, tiered{c, tierTimely})
	r.id = id
	require.Equal(t, "preferred", f.od.judgeCost(r, field, now)["change"])
	require.Equal(t, c, f.od.prefer.preferred)
	require.Equal(t, c, f.od.current)
}

func TestOnDemandByCostPrefersTheCheapestCostlierMemberThatStands(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliableCost, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	var id uint64
	for i := 3; i >= 0; i-- {
		// C does better than B and both better than A: either would stand
		id = noteRound(f.od, now.Add(-time.Duration(i)*time.Minute), tiered{a, tierFailed}, tiered{b, tierSlow}, tiered{c, tierTimely})
	}
	field := []*participant{{member: a}, {member: c}, {member: b}}
	require.Empty(t, f.od.unbacked(c, now))
	require.Equal(t, "preferred", f.od.judgeCost(&compareRound{id: id, c: a}, field, now)["change"])
	require.Equal(t, b, f.od.prefer.preferred, "the cheapest of them")
}

func TestOnDemandByCostReturns(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliableCost, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	prefer := func(p C.Proxy) {
		f.od.prefer.preferred = p
		f.od.computeOrder()
		f.od.reselect(now, selectionCause{Reason: causePreferred})
	}

	// C stands over A and B; B over A
	noteRound(f.od, now.Add(-time.Minute), tiered{a, tierFailed}, tiered{b, tierSlow}, tiered{c, tierTimely})
	prefer(c)
	f.od.updatePrefer(now)
	require.Equal(t, c, f.od.prefer.preferred)
	// B does as well as C now: back to B, which still stands over A
	noteRound(f.od, now, tiered{a, tierFailed}, tiered{b, tierTimely}, tiered{c, tierSlow})
	f.od.updatePrefer(now)
	require.Equal(t, b, f.od.prefer.preferred)
	require.Equal(t, b, f.od.current)
	events := ot.groupEvents("preference")
	last := events[len(events)-1]
	require.Equal(t, causeCostReturn, last["reason"])
	unbacked := last["unbacked"].([]any)
	require.Len(t, unbacked, 1)
	require.Equal(t, "notBetter", unbacked[0].(map[string]any)["reason"])

	// a cheaper member dead is not gone before; back, never compared with the preferred one,
	// it comes first again
	f.od.prefer.records = map[[2]C.Proxy]*pairRecords{}
	noteRound(f.od, now, tiered{b, tierTimely}, tiered{c, tierSlow})
	f.od.view[a] = probe.Evidence{State: probe.Dead, NextAllowed: now.Add(time.Hour)}
	prefer(b)
	f.od.updatePrefer(now)
	require.Equal(t, b, f.od.prefer.preferred, "A is dead")
	f.od.view[a] = probe.Evidence{State: probe.Alive}
	f.od.updatePrefer(now)
	require.Nil(t, f.od.prefer.preferred, "A is back and never compared with B")
	require.Equal(t, a, f.od.current)

	// the wins expire
	noteRound(f.od, now.Add(-probe.CompareExpiry+time.Minute), tiered{a, tierFailed}, tiered{b, tierTimely})
	prefer(b)
	f.od.updatePrefer(now)
	require.Equal(t, b, f.od.prefer.preferred)
	f.od.updatePrefer(now.Add(time.Minute))
	require.Nil(t, f.od.prefer.preferred, "its win expired")
}

func TestOnDemandOnlyNodesBeforeBuiltinMembersAllowAnOrder(t *testing.T) {
	ot := newODTest(t)
	inner := ot.group("inner", "X", "REJECT")
	_ = inner
	f := ot.ordered("g", OrderReliable, "A", "inner", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	require.False(t, f.od.prefer.eligible)
	events := ot.groupEvents("order")
	require.Len(t, events, 1)
	require.Equal(t, "group", events[0]["reason"])
	ok, why := orderable([]C.Proxy{ot.proxies["A"], ot.proxies["REJECT"], ot.node("B", "")}, nil)
	require.False(t, ok)
	require.Equal(t, "afterBuiltin", why)
}

func TestOnDemandAComparisonJoinsTheOtherNeedOfItsMember(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.continueMember, f.od.continueAt = a, now
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{
		{member: a, use: useCurrent}, {member: b, use: useRotation}, {member: c, use: useRotation},
	}}
	needs := map[C.Proxy]probe.Need{}
	for _, n := range f.od.deriveLocked(now) {
		needs[n.Target] = n
	}
	require.Equal(t, probe.SourceContinue, needs[a].Source, "the verification stands, and serves the comparison")
	require.True(t, needs[a].Notify)
	require.Equal(t, probe.SourceStandby, needs[b].Source, "so does the standby tested alongside")
	require.True(t, needs[b].Notify)
	require.Equal(t, probe.SourceCompare, needs[c].Source)
	require.Equal(t, now, needs[c].NotBefore)
	require.True(t, needs[c].Notify)
}

func TestOnDemandAGroupByCostHasRoundsOfItsDialersWhileInUse(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "R1", "R2", "X", "Y")
	dialer := ot.ordered("D", OrderReliable, "R1", "R2", "REJECT")
	ot.chainNode("X", "D")
	ot.chainNode("Y", "")
	exit := ot.ordered("exit", OrderReliableCost, "X", "Y", "REJECT")
	ot.eventually(func() bool { return ot.current(exit) == "X" }, "selects X")
	ot.eventually(func() bool { return ot.current(dialer) == "R1" }, "selects R1")

	clock.Advance(probe.CompareEvery * 2)
	exit.od.noteUse("dial")
	ot.recheck(exit)
	ot.eventually(func() bool {
		dialer.od.mutex.Lock()
		defer dialer.od.mutex.Unlock()
		return len(dialer.od.prefer.leases) == 1
	}, "the exit leases rounds of D")
	ot.recheck(dialer)
	ot.eventually(func() bool {
		dialer.od.mutex.Lock()
		defer dialer.od.mutex.Unlock()
		r := dialer.od.prefer.round
		if r == nil {
			return false
		}
		for _, p := range r.parts {
			if !p.done {
				return false
			}
		}
		return true
	}, "the tests of the round of D end")
	clock.Advance(2 * probe.PairWindow)
	ot.recheck(dialer)
	ot.eventually(func() bool {
		for _, e := range ot.groupEvents("compare") {
			if e["group"] == "D" {
				return true
			}
		}
		return false
	}, "D has a round, idle itself")
	var round map[string]any
	for _, e := range ot.groupEvents("compare") {
		if e["group"] == "D" {
			round = e
		}
	}
	require.Equal(t, []any{"exit"}, round["sources"])
	require.Equal(t, "done", round["end"])
	require.Len(t, round["pairs"], 1)
	dialer.od.mutex.Lock()
	inUse := dialer.od.inUseLocked(clock.Now())
	dialer.od.mutex.Unlock()
	require.False(t, inUse, "the round does not put D in use")

	// the exit idle, D has no more rounds
	clock.Advance(probe.UseWindow + probe.CompareEvery*2)
	ot.recheck(exit)
	ot.recheck(dialer)
	before := len(ot.groupEvents("compare"))
	ot.recheck(dialer)
	dialer.od.mutex.Lock()
	idle := dialer.od.prefer.round == nil
	dialer.od.mutex.Unlock()
	require.True(t, idle)
	require.Equal(t, before, len(ot.groupEvents("compare")))

	// retired, the exit takes its lease back
	exit.od.retire(nil)
	ot.eventually(func() bool {
		dialer.od.mutex.Lock()
		defer dialer.od.mutex.Unlock()
		return len(dialer.od.prefer.leases) == 0
	}, "the lease is taken back")
}

func TestOnDemandAGroupReplacingAnotherTakesOverThePreference(t *testing.T) {
	ot := newODTest(t)
	old := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
	old.od.mutex.Lock()
	now := old.od.now()
	noteRound(old.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
	old.od.prefer.preferred, old.od.prefer.lastChange, old.od.prefer.since = b, now, now
	old.od.computeOrder()
	old.od.mutex.Unlock()

	inherits := func() int { return len(ot.groupEvents("inherit")) }
	fresh := ot.buildOption(GroupCommonOption{Name: "g", Order: OrderReliable}, "A", "B", "REJECT")
	before := inherits()
	ot.replace(old, fresh)
	ot.eventually(func() bool { return inherits() > before }, "the state is handed over")
	ot.eventually(func() bool { return ot.current(fresh) == "B" }, "B stays first")
	fresh.od.mutex.Lock()
	lastChange, duels := fresh.od.prefer.lastChange, len(fresh.od.duels(b, a, now, probe.CompareRecent))
	fresh.od.mutex.Unlock()
	require.Equal(t, now, lastChange)
	require.Equal(t, 1, duels)

	// under another order, nothing is taken over
	other := ot.buildOption(GroupCommonOption{Name: "g", Order: OrderReliableCost}, "A", "B", "REJECT")
	before = inherits()
	ot.replace(fresh, other)
	ot.eventually(func() bool { return inherits() > before }, "the state is handed over")
	ot.eventually(func() bool { return ot.current(other) == "A" }, "back to the order by cost")
	other.od.mutex.Lock()
	preferred, records := other.od.prefer.preferred, len(other.od.prefer.records)
	other.od.mutex.Unlock()
	require.Nil(t, preferred)
	require.Zero(t, records)
}

func (ot *odTest) settle(f *Fallback) {
	ot.t.Helper()
	done := make(chan struct{})
	f.od.post(settleNote{done: done})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		ot.t.Fatal("the group did not settle")
	}
}

func TestOnDemandCountsTheDialsThePreferenceCarries(t *testing.T) {
	ot, _ := newClockedODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	noteRound(f.od, f.od.now(), tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.prefer.preferred = b
	f.od.computeOrder()
	f.od.publishSnapshot()
	f.od.mutex.Unlock()
	d := f.od.newGroupDial()
	require.Equal(t, b, d.preferred)
	d.returnedBy(b, true, false)
	f.od.newGroupDial().returnedBy(a, true, false)
	pinned := "A"
	f.od.pin.Store(&pinned)
	pinnedDial := f.od.newGroupDial()
	require.Nil(t, pinnedDial.preferred, "a pin puts it out of effect")
	pinnedDial.returnedBy(b, true, false)
	ot.settle(f)
	f.od.mutex.Lock()
	f.od.flushDialCounts(f.od.now().Unix()/60 + 1)
	f.od.mutex.Unlock()
	dials := ot.groupEvents("dials")
	require.Len(t, dials, 1)
	require.Equal(t, float64(3), dials[0]["dials"])
	require.Equal(t, float64(1), dials[0]["preferred"])
}

func TestOnDemandAPinSetBeforeTheNextRoundPutsThePreferenceOutOfEffect(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	noteRound(f.od, f.od.now(), tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.prefer.preferred = b
	f.od.view[c] = probe.Evidence{State: probe.Dead}
	f.od.computeOrder()
	f.od.publishSnapshot()
	f.od.mutex.Unlock()
	require.Equal(t, b, f.od.planDial(&C.Metadata{}, false, false).route)
	// pinned to the dead C, the group has not heard of it yet: the config order follows C
	pinned := "C"
	f.od.pin.Store(&pinned)
	require.Equal(t, a, f.od.planDial(&C.Metadata{}, false, false).route)
	// the group pinned C; unpinned again before it hears of it, B is preferred again
	f.od.mutex.Lock()
	f.od.pinned = "C"
	f.od.computeOrder()
	f.od.publishSnapshot()
	f.od.mutex.Unlock()
	require.Equal(t, a, f.od.planDial(&C.Metadata{}, false, false).route)
	f.od.pin.Store(new(string))
	require.Equal(t, b, f.od.planDial(&C.Metadata{}, false, false).route)
	require.Equal(t, b, f.od.newGroupDial().preferred)
}

func TestOnDemandAChallengerMayStartWithinTheWindow(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "D", "REJECT")
	a, d := ot.proxies["A"], ot.proxies["D"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: d, use: useRotation}}}
	f.od.prefer.round = r
	f.od.onBegan(probe.Began{Exec: 1, Key: probe.Key{Target: a, Cond: f.od.cond}, Started: now})
	f.od.updatePrefer(now.Add(500 * time.Millisecond))
	asked := false
	for _, n := range f.od.deriveLocked(now.Add(500 * time.Millisecond)) {
		asked = asked || n.Target == d && n.Source == probe.SourceCompare
	}
	require.True(t, asked, "D is still asked for within the window")
	// started 990 ms after A, heard of in the next batch: side by side all the same
	late := now.Add(probe.PairWindow + compareSettle/2)
	f.od.updatePrefer(late)
	require.False(t, r.part(d).missed)
	f.od.onBegan(probe.Began{Exec: 2, Key: probe.Key{Target: d, Cond: f.od.cond}, Started: now.Add(990 * time.Millisecond)})
	require.False(t, r.part(d).missed)
	// one never heard of is missed once the settling time passed
	r.part(d).exec, r.part(d).started = 0, time.Time{}
	f.od.updatePrefer(now.Add(probe.PairWindow + compareSettle))
	require.True(t, r.part(d).missed)
	for _, n := range f.od.deriveLocked(now.Add(probe.PairWindow + compareSettle)) {
		require.False(t, n.Target == d && n.Source == probe.SourceCompare, "nor after it")
	}
}

func TestOnDemandAChallengerToldOfLateIsJudgedByWhenItStarted(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.prefer.round = r
	f.od.onBegan(probe.Began{Exec: 1, Key: probe.Key{Target: a, Cond: f.od.cond}, Started: now.Add(2 * time.Second)})
	f.od.onBegan(probe.Began{Exec: 2, Key: probe.Key{Target: b, Cond: f.od.cond}, Started: now.Add(600 * time.Millisecond)})
	require.True(t, r.part(b).missed, "it started 1.4 seconds before A")
	// A starting past the start limit cancels the round, whenever it is heard of
	r2 := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.prefer.round = r2
	f.od.onBegan(probe.Began{Exec: 3, Key: probe.Key{Target: a, Cond: f.od.cond}, Started: now.Add(probe.CompareStart + time.Millisecond)})
	f.od.lastActive.Store(now.UnixNano())
	f.od.updatePrefer(now.Add(time.Second))
	require.Nil(t, f.od.prefer.round)
	e := ot.groupEvents("compare")
	require.Equal(t, "notStarted", e[len(e)-1]["reason"])
}

func TestOnDemandASearchInARoundKeepsThePreference(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	var id uint64
	for i := 3; i > 0; i-- {
		id = noteRound(f.od, now.Add(-time.Duration(i)*time.Minute), tiered{a, tierSlow}, tiered{b, tierTimely})
	}
	r := &compareRound{id: id, c: a, searched: true}
	d := f.od.judgeReliable(r, []*participant{{member: a}, {member: b}}, now)
	require.Equal(t, "search", d["why"])
	require.Nil(t, f.od.prefer.preferred)
}

func TestOnDemandAComparisonFailureEndsNoSuspension(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a := ot.proxies["A"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.suspended = &suspendState{member: a, reason: "loss", after: now}
	f.od.onCompletion(probe.Completion{Exec: 9, Key: probe.Key{Target: a, Cond: f.od.cond}, Started: now.Add(time.Second),
		Result: C.ProbeResult{ErrorClass: C.ProbeErrorDial}, Compared: true, Comparing: true}, now.Add(2*time.Second))
	require.NotNil(t, f.od.suspended, "the failure kept out of the evidence answers nothing")
}

func TestOnDemandByCostAStaleBasisReturnsToTheCostliestThatStands(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliableCost, "A", "B", "C", "REJECT")
	a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	noteRound(f.od, now.Add(-probe.CompareExpiry+time.Minute), tiered{a, tierFailed}, tiered{b, tierSlow}, tiered{c, tierTimely})
	noteRound(f.od, now, tiered{a, tierFailed}, tiered{b, tierTimely})
	f.od.prefer.preferred = c
	f.od.computeOrder()
	f.od.updatePrefer(now)
	require.Equal(t, c, f.od.prefer.preferred)
	f.od.updatePrefer(now.Add(time.Minute))
	require.Equal(t, b, f.od.prefer.preferred, "C has no basis any more, B still has")
}

func TestOnDemandAnExclusionEndsThePreferenceWhereItTakesEffect(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.prefer.preferred = b
	f.od.prefer.confirms[b] = confirmation{after: now, exec: 7}
	f.od.member(b).excluded = true
	f.od.preferExcluded(now)
	// lifted again in the same batch, it is not preferred again
	f.od.member(b).excluded = false
	require.Nil(t, f.od.prefer.preferred)
	require.Empty(t, f.od.prefer.confirms)
	require.Equal(t, now, f.od.prefer.lastChange)
}

func (ot *odTest) activated(f *Fallback) {
	ot.t.Helper()
	ot.eventually(func() bool {
		f.od.mutex.Lock()
		defer f.od.mutex.Unlock()
		return f.od.activated
	}, "the group is activated")
}

func TestOnDemandComingIntoUseWithoutARoundLatelyStartsOne(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.prefer.lastDone = now.Add(-probe.CompareEvery)
	f.od.prefer.nextAt = now.Add(10 * time.Minute)
	f.od.lastActive.Store(now.UnixNano())
	f.od.startRound(now)
	require.NotNil(t, f.od.prefer.round)
}

func TestOnDemandOrderOptions(t *testing.T) {
	option := GroupCommonOption{Name: "g", Type: "fallback", Probe: ProbeOnDemand, Order: OrderReliable, StrictOrder: true}
	checkProbeOption(&option)
	require.Equal(t, OrderConfig, option.Order, "strict-order excludes it")
	option = GroupCommonOption{Name: "g", Type: "select", Order: OrderReliable}
	checkProbeOption(&option)
	require.Equal(t, OrderConfig, option.Order)
	option = GroupCommonOption{Name: "g", Type: "fallback", Probe: ProbeOnDemand, Order: "fastest"}
	checkProbeOption(&option)
	require.Equal(t, OrderConfig, option.Order)
	option = GroupCommonOption{Name: "g", Type: "fallback", Probe: ProbeOnDemand, Order: OrderReliableCost}
	checkProbeOption(&option)
	require.Equal(t, OrderReliableCost, option.Order)
}

func TestOnDemandMembersNoLongerAllowingTheOrderEndThePreferenceAsTheyChange(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b, reject := ot.proxies["A"], ot.proxies["B"], ot.proxies["REJECT"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.prefer.preferred = b
	// a node after a built-in member, then back, in one batch
	f.od.applyMembers([]C.Proxy{a, reject, b})
	f.od.applyMembers([]C.Proxy{a, b, reject})
	require.Nil(t, f.od.prefer.preferred)
	require.True(t, f.od.prefer.eligible)
	events := ot.groupEvents("preference")
	require.Len(t, events, 1)
	require.Equal(t, "ineligible", events[0]["why"])
}

func TestOnDemandAConfirmationGoesOnWhileTheMembersDoNotAllowTheOrder(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b, reject := ot.proxies["A"], ot.proxies["B"], ot.proxies["REJECT"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.prefer.confirms[b] = confirmation{after: now, exec: 7}
	f.od.applyMembers([]C.Proxy{a, reject, b})
	require.False(t, f.od.prefer.eligible)
	asked := false
	for _, n := range f.od.deriveLocked(now) {
		asked = asked || n.Target == b && n.Source == probe.SourceConfirm
	}
	require.True(t, asked)
	f.od.view[b] = probe.Evidence{State: probe.Dead, Started: now, Exec: 9}
	f.od.updatePrefer(now)
	require.Empty(t, f.od.prefer.confirms)
	confirms := ot.groupEvents("confirm")
	require.Len(t, confirms, 1)
	require.Equal(t, float64(7), confirms[0]["probe"])
	require.Equal(t, float64(9), confirms[0]["by"])
}

func TestOnDemandAUseAfterIdleIsAComingIntoUse(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	f.od.prefer.active = true
	f.od.prefer.lastDone = now.Add(-31 * time.Minute)
	f.od.prefer.nextAt = now.Add(4 * time.Minute)
	// in use all along: the round waits for its time
	f.od.startRound(now)
	require.Nil(t, f.od.prefer.round)
	// idle meanwhile, unheard of, and used again: one as soon as it may
	f.od.handle(useNote{reason: "dial"}, now)
	f.od.startRound(now)
	require.NotNil(t, f.od.prefer.round)
}

func TestOnDemandAGroupReplacingOneInUseGoesOnWithItsRounds(t *testing.T) {
	ot := newODTest(t)
	old := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.activated(old)
	old.od.mutex.Lock()
	now := old.od.now()
	old.od.prefer.active = true
	old.od.prefer.lastDone = now.Add(-31 * time.Minute)
	old.od.prefer.nextAt = now.Add(4 * time.Minute)
	state := old.od.prefer.clone()
	old.od.mutex.Unlock()
	fresh := ot.buildOption(GroupCommonOption{Name: "g", Order: OrderReliable}, "A", "B", "REJECT")
	fresh.od.mutex.Lock()
	defer fresh.od.mutex.Unlock()
	fresh.od.inheritPrefer(state, now, true)
	fresh.od.activated = true
	fresh.od.lastActive.Store(now.UnixNano())
	fresh.od.startRound(now)
	require.Nil(t, fresh.od.prefer.round, "still in use: no round before its time")
}

func TestOnDemandAnOutageAlongTheRouteATestTookVoidsTheRound(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	key := func(p C.Proxy) probe.Key { return probe.Key{Target: p, Cond: f.od.cond} }
	newRound := func() *compareRound {
		r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
		f.od.prefer.round = r
		f.od.onBegan(probe.Began{Exec: 1, Key: key(a), Started: now})
		f.od.onBegan(probe.Began{Exec: 2, Key: key(b), Started: now})
		return r
	}
	// B went through X, which was suspected and disproved before B ended
	r := newRound()
	f.od.memberPools[b] = []string{"Y"}
	f.od.latchRound(probe.OutageChanged{Pool: "X", Cond: f.od.cond, State: probe.OutageSuspected})
	require.Empty(t, r.voided, "B goes through Y as far as known")
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: key(b), Pools: []string{"X"}, Ended: now})
	require.Equal(t, "outage", r.voided)
	// B ended going through X only: an outage of Y, where its route went since, does not tell
	r = newRound()
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: key(b), Pools: []string{"X"}, Ended: now})
	f.od.latchRound(probe.OutageChanged{Pool: "Y", Cond: f.od.cond, State: probe.OutageSuspected})
	require.Empty(t, r.voided)
}

func TestOnDemandATestEndingPastItsTimeIsMissed(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	key := func(p C.Proxy) probe.Key { return probe.Key{Target: p, Cond: f.od.cond} }
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.prefer.round = r
	f.od.onBegan(probe.Began{Exec: 2, Key: key(b), Started: now})
	deadline := now.Add(testTimeoutOf(f.od.cond) + compareGrace)
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: key(b), Ended: deadline.Add(time.Millisecond)})
	require.True(t, r.part(b).missed)
	require.False(t, r.part(b).done)
}

func TestOnDemandASearchStartedInARoundIsLatched(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A", "B")
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.prefer.round = r
	dead := probe.Evidence{State: probe.Dead, Started: now, Version: f.od.view[a].Version + 1}
	f.od.onCompletion(probe.Completion{Exec: 5, Key: probe.Key{Target: a, Cond: f.od.cond}, Started: now, Adopted: true,
		Before: f.od.view[a], After: dead, Result: C.ProbeResult{ErrorClass: C.ProbeErrorDial}}, now)
	require.NotNil(t, f.od.search)
	require.True(t, r.searched)
}

func TestOnDemandAFirstRequestDecidesTheRoundWhateverTheWholeTest(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	// A answered its first request in time, then failed; B failed the first one
	firstOnly := C.ProbeResult{ErrorClass: C.ProbeErrorTimeout, FirstSatisfied: true, FirstElapsed: 100 * time.Millisecond}
	failed := C.ProbeResult{ErrorClass: C.ProbeErrorDial}
	f.od.prefer.round = &compareRound{id: compareRounds.Add(1), t0: now, c: a, windowEnd: now.Add(probe.PairWindow), parts: []*participant{
		{member: a, use: useCurrent, exec: 1, started: now, done: true, result: firstOnly},
		{member: b, use: useRotation, exec: 2, started: now, done: true, result: failed},
	}}
	f.od.updatePrefer(now.Add(probe.PairWindow + compareSettle))
	e := ot.groupEvents("compare")
	require.Equal(t, "done", e[len(e)-1]["end"])
	list := f.od.duels(a, b, now, probe.CompareRecent)
	require.Len(t, list, 1)
	require.Equal(t, tierTimely, list[0].x.tier)
}

func TestOnDemandAPreferenceEndsOnceAllItsWinsTurnOutCovered(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	// the local network turns out not normal at the time of the first win, then the second
	first, second := now.Add(-time.Hour), now.Add(-30*time.Minute)
	noteRound(f.od, first, tiered{a, tierSlow}, tiered{b, tierTimely})
	noteRound(f.od, second, tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.prefer.preferred = b
	f.od.prefer.basis = []pairResult{
		{sides: [2]side{{started: first, tier: tierTimely}, {started: first, tier: tierSlow}}},
		{sides: [2]side{{started: second, tier: tierTimely}, {started: second, tier: tierSlow}}},
	}
	var spans [][2]time.Time
	f.od.coverage = func(started time.Time, _ []string) bool {
		for _, span := range spans {
			if !started.Before(span[0]) && started.Before(span[1]) {
				return true
			}
		}
		return false
	}
	covered := func(at time.Time) { spans = append(spans, [2]time.Time{at.Add(-time.Minute), at.Add(time.Minute)}) }
	covered(first)
	d := f.od.judgeReliable(&compareRound{id: compareRounds.Add(1), c: b}, nil, now)
	require.NotEqual(t, "end", d["change"], "one win still stands")
	require.Equal(t, b, f.od.prefer.preferred)
	covered(second)
	f.od.judgeReliable(&compareRound{id: compareRounds.Add(1), c: b}, nil, now)
	require.Nil(t, f.od.prefer.preferred)
	events := ot.groupEvents("preference")
	require.Equal(t, "basisCovered", events[len(events)-1]["why"])
}

func TestOnDemandAChallengerStartedInTimeAndHeardOfLateCounts(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	key := func(p C.Proxy) probe.Key { return probe.Key{Target: p, Cond: f.od.cond} }
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: a, parts: []*participant{{member: a, use: useCurrent}, {member: b, use: useRotation}}}
	f.od.prefer.round = r
	f.od.onBegan(probe.Began{Exec: 1, Key: key(a), Started: now})
	// past the window, B is no longer asked for
	f.od.updatePrefer(now.Add(probe.PairWindow + compareSettle/2))
	for _, n := range f.od.deriveLocked(now.Add(probe.PairWindow + compareSettle/2)) {
		require.False(t, n.Target == b && n.Source == probe.SourceCompare)
	}
	// it started at 990 ms, heard of now
	f.od.onBegan(probe.Began{Exec: 2, Key: key(b), Started: now.Add(990 * time.Millisecond)})
	require.Equal(t, uint64(2), r.part(b).exec)
	ok := C.ProbeResult{Satisfied: true, FirstSatisfied: true, FirstElapsed: 10 * time.Millisecond}
	slow := C.ProbeResult{Satisfied: true, FirstSatisfied: true, FirstElapsed: 1500 * time.Millisecond}
	f.od.preferCompletion(probe.Completion{Exec: 1, Key: key(a), Result: slow, Ended: now.Add(2 * time.Second)})
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: key(b), Result: ok, Ended: now.Add(time.Second)})
	f.od.updatePrefer(now.Add(3 * time.Second))
	require.Nil(t, f.od.prefer.round)
	list := f.od.duels(b, a, now.Add(3*time.Second), probe.CompareRecent)
	require.Len(t, list, 1)
	require.Greater(t, list[0].x.tier, list[0].y.tier)
}

func TestOnDemandAMemberNotPickedStaysOwed(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "C", "D", "E", "REJECT")
	a, b, c, d, e := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"], ot.proxies["D"], ot.proxies["E"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	for _, p := range []C.Proxy{b, c, d, e} {
		f.od.prefer.owed[p] = true
	}
	parts := f.od.challengers(a, false, now)
	require.Len(t, parts, probe.CompareChallenge)
	require.Equal(t, map[C.Proxy]bool{e: true}, f.od.prefer.owed, "E was not picked: it stays owed")
	parts = f.od.challengers(a, false, now)
	require.Equal(t, e, parts[0].member)
}

func TestOnDemandAnOutageSuspectedBeforeTheRoundTellsOfATestThroughIt(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	b := ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	f.od.lastActive.Store(now.UnixNano())
	f.od.outagePools["X"] = true
	f.od.memberPools[b] = []string{"Y"}
	f.od.startRound(now)
	r := f.od.prefer.round
	require.NotNil(t, r)
	f.od.onBegan(probe.Began{Exec: 2, Key: probe.Key{Target: b, Cond: f.od.cond}, Started: now})
	// X disproved meanwhile; B turns out to have gone through it
	delete(f.od.outagePools, "X")
	f.od.preferCompletion(probe.Completion{Exec: 2, Key: probe.Key{Target: b, Cond: f.od.cond}, Pools: []string{"X"}, Ended: now})
	require.Equal(t, "outage", r.voided)
}

func TestOnDemandTheOnlyOpponentReplacedLeavesThePreference(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	a, b := ot.proxies["A"], ot.proxies["B"]
	ot.activated(f)
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	noteRound(f.od, now, tiered{a, tierSlow}, tiered{b, tierTimely})
	f.od.changePreference(b, causePreferred, "", nil, nil, now)
	fresh := ot.node("A2", "")
	f.od.applyMembers([]C.Proxy{fresh, b, ot.proxies["REJECT"]})
	f.od.updatePrefer(now.Add(time.Minute))
	require.Equal(t, b, f.od.prefer.preferred, "B took part a minute ago")
	f.od.updatePrefer(now.Add(probe.CompareExpiry))
	require.Nil(t, f.od.prefer.preferred)
}

func TestOnDemandALeaseToldAgainIsAComingIntoUse(t *testing.T) {
	ot := newODTest(t)
	f := ot.ordered("g", OrderReliable, "A", "B", "REJECT")
	ot.activated(f)
	exit := ot.ordered("exit", OrderReliableCost, "B", "REJECT")
	f.od.mutex.Lock()
	defer f.od.mutex.Unlock()
	now := f.od.now()
	exit.od.lastActive.Store(now.UnixNano())
	f.od.prefer.leases[exit.od] = true
	f.od.prefer.active = true
	f.od.prefer.lastDone = now.Add(-31 * time.Minute)
	f.od.prefer.nextAt = now.Add(4 * time.Minute)
	f.od.startRound(now)
	require.Nil(t, f.od.prefer.round)
	f.od.onLease(exitLease{exit: exit.od, on: true}, now)
	f.od.startRound(now)
	require.NotNil(t, f.od.prefer.round)
}
