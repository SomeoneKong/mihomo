package outboundgroup

import (
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"golang.org/x/exp/slices"
)

// The orders a group testing on demand keeps its members in.
const (
	OrderConfig = "config"
	// OrderReliable puts first a member of the same cost that does better than the preferred
	// one, again and again, in rounds testing them side by side.
	OrderReliable = "reliable"
	// OrderReliableCost keeps the members by cost, the cheapest first, and puts a costlier one
	// first only while it does better than each cheaper one it goes before.
	OrderReliableCost = "reliable-cost"
)

// Why the preference of a group changed.
const (
	causePreferred     = "preferred"     // a member did better again and again
	causeCostReturn    = "costReturn"    // a costlier member no longer does better than a cheaper one
	causePreferenceEnd = "preferenceEnd" // the preferred member yields, is on trial, gone or stale
)

// What a member takes part in a round for.
const (
	useCurrent   = "current"
	useRotation  = "rotation"
	useContrast  = "contrast"  // a costlier candidate and a member it would go before, not compared yet
	useOvertaken = "overtaken" // a member the preferred one goes before
	useTrial     = "trial"     // a member on trial, tested for its witness only
)

const (
	// compareGrace is how long past the timeout a test of a round is waited for.
	compareGrace = 30 * time.Second
	// compareRetry is when a round that could not start, or was canceled, is tried again.
	compareRetry = time.Minute
	// compareRecheck is how often a group preferring a costlier member reads its basis again: a
	// coverage known later tells it nothing.
	compareRecheck = 5 * time.Minute
	// compareSettle is how long past a limit a test that started within it is waited to be
	// heard of: the start of tests is told by message.
	compareSettle = time.Second
)

// compareRounds numbers the rounds of this process.
var compareRounds atomic.Uint64

// tier is how the first request of a test went.
type tier uint8

const (
	tierFailed tier = iota
	tierSlow
	tierTimely
)

func (t tier) String() string {
	switch t {
	case tierTimely:
		return "timely"
	case tierSlow:
		return "slow"
	default:
		return "failed"
	}
}

func tierOf(r C.ProbeResult) tier {
	switch {
	case !r.FirstSatisfied:
		return tierFailed
	case r.FirstElapsed <= probe.CompareTimely:
		return tierTimely
	default:
		return tierSlow
	}
}

// side is what the test of one member of a pair found in a round.
type side struct {
	exec    uint64
	started time.Time
	tier    tier
	first   time.Duration
	pools   []string
	unknown bool
}

// pairResult is a round as two members of it did.
type pairResult struct {
	round uint64
	at    time.Time // when the test of the preferred member of the round started
	sides [2]side   // of the members of the pair, in their order
}

// pairRecords are the latest rounds two members took part in together.
type pairRecords struct {
	members [2]C.Proxy
	results []pairResult // oldest first
}

// duel is a result as seen by one member of the pair, x.
type duel struct {
	round uint64
	x, y  side
}

func score(list []duel) (x, y int) {
	for _, d := range list {
		switch {
		case d.x.tier > d.y.tier:
			x++
		case d.y.tier > d.x.tier:
			y++
		}
	}
	return x, y
}

// participant is a member of a round.
type participant struct {
	member  C.Proxy
	use     string
	exec    uint64
	started time.Time
	missed  bool
	done    bool
	result  C.ProbeResult
	// compared is set on a failure only comparisons asked for, ended when the coordinator
	// took it
	compared  bool
	comparing bool // the test started only for comparisons
	ended     time.Time
	pools     []string
	unknown   bool
}

// compareRound is a round testing the preferred member and challengers side by side.
type compareRound struct {
	id        uint64
	t0        time.Time
	c         C.Proxy
	parts     []*participant
	windowEnd time.Time
	voided    string          // why it is void, as a message took effect
	searched  bool            // a search started meanwhile: the round changes no preference
	late      bool            // the preferred member started too late
	suspected map[string]bool // pools suspected out meanwhile, for the tests ending later
	sources   []string
}

func (r *compareRound) part(p C.Proxy) *participant {
	for _, q := range r.parts {
		if q.member == p {
			return q
		}
	}
	return nil
}

// confirmation is a member a comparison found failing, to be tested as any other.
type confirmation struct {
	after time.Time
	exec  uint64
}

// preferState is what a group preferring its more reliable members keeps.
type preferState struct {
	mode     string
	eligible bool
	why      string
	checked  bool

	nextAt    time.Time
	retryAt   time.Time // a round canceled or void is tried again no sooner
	recheckAt time.Time // a round due but blocked is looked at again then
	active    bool      // in use or leased, as the last update found it
	lastDone  time.Time
	cursor    C.Proxy
	owed      map[C.Proxy]bool
	round     *compareRound
	records   map[[2]C.Proxy]*pairRecords
	confirms  map[C.Proxy]confirmation
	contrast  int     // the next costlier candidate to find a contrast for
	overtaken C.Proxy // the last member gone before the round tested

	preferred  C.Proxy
	since      time.Time
	seen       time.Time // when the preferred member last took part in a round with another
	lastChange time.Time
	basis      []pairResult // the wins a preference of the same cost was taken on

	// leases are the groups by cost whose members go through this group: while one is in use,
	// it has rounds; sent are the groups this one leases rounds of, told again each time it
	// comes into use
	leases     map[*onDemand]bool
	sent       map[*onDemand]bool
	sentActive bool
	resend     bool // came into use: tell the groups again
	reused     bool // came into use again, as a use after the group was idle tells
}

func newPreferState(mode string) *preferState {
	return &preferState{
		mode:     mode,
		owed:     map[C.Proxy]bool{},
		records:  map[[2]C.Proxy]*pairRecords{},
		confirms: map[C.Proxy]confirmation{},
		leases:   map[*onDemand]bool{},
		sent:     map[*onDemand]bool{},
	}
}

// exitLease tells a group of members of one cost that a group by cost going through it wants
// it compared, or no longer does.
type exitLease struct {
	exit *onDemand
	on   bool
}

// orderable reports whether members allow an order by reliability: nodes, then built-in
// members not tested.
func orderable(members []C.Proxy, probed map[C.Proxy]bool) (bool, string) {
	builtin := false
	for _, p := range members {
		if probe.IsBuiltin(p) {
			if probed[p] {
				return false, "testedDirect"
			}
			builtin = true
			continue
		}
		if _, isGroup := p.Adapter().(ProxyGroup); isGroup {
			return false, "group"
		}
		if builtin {
			return false, "afterBuiltin"
		}
	}
	return true, ""
}

// checkOrderable takes the members of the group in: whether they allow the order, and what
// it kept of members gone.
func (od *onDemand) checkOrderable() {
	ps := od.prefer
	if ps == nil {
		return
	}
	ok, why := orderable(od.members, od.probed)
	if !ps.checked || ok != ps.eligible || why != ps.why {
		if ps.checked || !ok {
			fields := od.event("order")
			fields["order"] = ps.mode
			fields["eligible"] = ok
			if !ok {
				fields["reason"] = why
				log.Warnln("The group [%s] keeps its members in config order: order: %s takes nodes followed by built-in members only (%s)", od.name(), ps.mode, why)
			}
			od.record(fields)
		}
		ps.checked, ps.eligible, ps.why = true, ok, why
		if !ok && ps.round != nil && ps.round.voided == "" {
			ps.round.voided = "ineligible"
		}
	}
	// ended as the members change, also when a later change of the same batch undoes it; the
	// order and the selection follow from the caller
	switch {
	case ps.preferred == nil:
	case !ok:
		od.recordPreferenceEnd(ps.preferred, causePreferenceEnd, "ineligible")
	case !od.isMember(ps.preferred):
		od.recordPreferenceEnd(ps.preferred, causePreferenceEnd, "removed")
	}
	for key, pr := range ps.records {
		if !od.isMember(pr.members[0]) || !od.isMember(pr.members[1]) {
			delete(ps.records, key)
		}
	}
	for p := range ps.confirms {
		if !od.isMember(p) {
			od.endConfirm(p, "removed")
		}
	}
	for p := range ps.owed {
		if !od.isMember(p) {
			delete(ps.owed, p)
		}
	}
	if ps.cursor != nil && !od.isMember(ps.cursor) {
		ps.cursor = nil
	}
	if ps.overtaken != nil && !od.isMember(ps.overtaken) {
		ps.overtaken = nil
	}
	if r := ps.round; r != nil {
		for _, p := range r.parts {
			if !od.isMember(p.member) {
				p.missed = true
			}
		}
	}
}

// preferredMember is the preference in effect: none while a member is pinned.
func (od *onDemand) preferredMember() C.Proxy {
	if od.pinned != "" {
		return nil
	}
	return od.unpinnedPreference()
}

// unpinnedPreference is the preference that is in effect while no member is pinned.
func (od *onDemand) unpinnedPreference() C.Proxy {
	ps := od.prefer
	if ps == nil || !ps.eligible || ps.preferred == nil || !od.isMember(ps.preferred) {
		return nil
	}
	return ps.preferred
}

// baseOrder is the order the effective one is computed from: the preferred member first, the
// others in config order.
func (od *onDemand) baseOrder() []C.Proxy {
	return od.baseFor(od.preferredMember())
}

func (od *onDemand) baseFor(p C.Proxy) []C.Proxy {
	if p == nil {
		return od.members
	}
	base := make([]C.Proxy, 0, len(od.members))
	base = append(base, p)
	for _, m := range od.members {
		if m != p {
			base = append(base, m)
		}
	}
	return base
}

func (od *onDemand) leased(now time.Time) bool {
	for exit := range od.prefer.leases {
		if exit.gone.Load() {
			delete(od.prefer.leases, exit)
			continue
		}
		if exit.activeAt(now) {
			return true
		}
	}
	return false
}

// activeAt reports whether the group is in use, read without its lock.
func (od *onDemand) activeAt(now time.Time) bool {
	active := od.lastActive.Load()
	return active != 0 && now.Sub(time.Unix(0, active)) < probe.UseWindow
}

// viaSuspectedPool reports whether a member goes through a pool suspected out.
func (od *onDemand) viaSuspectedPool(p C.Proxy) bool {
	for _, pool := range od.memberPools[p] {
		if od.outagePools[pool] {
			return true
		}
	}
	return false
}

// contender reports whether a member may be tested as a challenger now.
func (od *onDemand) contender(p C.Proxy, now time.Time) bool {
	if od.unprobed(p) || od.excluded(p) || od.standingOf(p) != standingNormal || od.delegated(p) || od.viaSuspectedPool(p) {
		return false
	}
	ev := od.view[p]
	return ev.State != probe.Dead || !now.Before(ev.NextAllowed)
}

// qualified reports whether a member counts as gone before by a costlier one.
func (od *onDemand) qualified(p C.Proxy) bool {
	return !od.unprobed(p) && !od.excluded(p) && od.standingOf(p) == standingNormal && od.view[p].State != probe.Dead
}

func (od *onDemand) covered(s side) bool {
	pools := s.pools
	if s.unknown {
		// the pools it went through are not known: only the local network tells
		pools = nil
	}
	if od.coverage != nil {
		return od.coverage(s.started, pools)
	}
	return od.coord.Covered(s.started, pools, od.cond)
}

func (od *onDemand) validResult(r *pairResult, now time.Time) bool {
	return now.Sub(r.at) < probe.CompareExpiry && !od.covered(r.sides[0]) && !od.covered(r.sides[1])
}

// duels are the latest n valid results of x and y, the latest first.
func (od *onDemand) duels(x, y C.Proxy, now time.Time, n int) []duel {
	pr := od.prefer.records[[2]C.Proxy{x, y}]
	if pr == nil {
		return nil
	}
	xi := 0
	if pr.members[1] == x {
		xi = 1
	}
	var list []duel
	for i := len(pr.results) - 1; i >= 0 && len(list) < n; i-- {
		r := &pr.results[i]
		if od.validResult(r, now) {
			list = append(list, duel{round: r.round, x: r.sides[xi], y: r.sides[1-xi]})
		}
	}
	return list
}

func (od *onDemand) note(x, y C.Proxy, result pairResult) {
	ps := od.prefer
	pr := ps.records[[2]C.Proxy{x, y}]
	if pr == nil {
		pr = &pairRecords{members: [2]C.Proxy{x, y}}
		ps.records[[2]C.Proxy{x, y}] = pr
		ps.records[[2]C.Proxy{y, x}] = pr
	}
	if pr.members[0] != x {
		result.sides[0], result.sides[1] = result.sides[1], result.sides[0]
	}
	if p := ps.preferred; (p == x || p == y) && result.at.After(ps.seen) {
		ps.seen = result.at
	}
	pr.results = append(pr.results, result)
	if len(pr.results) > probe.CompareKept {
		pr.results = append(pr.results[:0], pr.results[len(pr.results)-probe.CompareKept:]...)
	}
}

// lastRecord is when the latest round the member took part in with another started; for the
// preferred member, also once the results with an opponent replaced are gone.
func (od *onDemand) lastRecord(p C.Proxy) time.Time {
	var last time.Time
	if p == od.prefer.preferred {
		last = od.prefer.seen
	}
	for key, pr := range od.prefer.records {
		if key[0] != p || len(pr.results) == 0 {
			continue
		}
		last = maxTime(last, pr.results[len(pr.results)-1].at)
	}
	return last
}

// unbacked are the members a costlier one goes before without doing better than them.
func (od *onDemand) unbacked(x C.Proxy, now time.Time) []C.Proxy {
	var missing []C.Proxy
	for _, y := range od.members {
		if y == x {
			break
		}
		if !od.qualified(y) {
			continue
		}
		if xw, yw := score(od.duels(x, y, now, probe.CompareCostWindow)); xw <= yw {
			missing = append(missing, y)
		}
	}
	return missing
}

// unbackedWhy tells why a costlier member has no basis over a cheaper one.
func (od *onDemand) unbackedWhy(x, y C.Proxy, now time.Time) string {
	pr := od.prefer.records[[2]C.Proxy{x, y}]
	if pr == nil || len(pr.results) == 0 {
		return "uncompared"
	}
	if len(od.duels(x, y, now, probe.CompareCostWindow)) > 0 {
		return "notBetter"
	}
	for i := range pr.results {
		if now.Sub(pr.results[i].at) < probe.CompareExpiry {
			return "covered"
		}
	}
	return "expired"
}

func jittered(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (1 + (rand.Float64()*2-1)*probe.CompareJitter))
}

// updatePrefer runs the rounds and keeps the preference: after the standings are taken in,
// before the standbys and the needs are derived from the order.
func (od *onDemand) updatePrefer(now time.Time) {
	ps := od.prefer
	if ps == nil {
		return
	}
	for p, c := range ps.confirms {
		switch {
		case od.excluded(p):
			od.endConfirm(p, "excluded")
		case !od.view[p].Started.Before(c.after):
			od.endConfirm(p, "tested")
		}
	}
	if !ps.eligible {
		if ps.round != nil {
			od.endRound(now, "void", "ineligible", nil)
		}
		if ps.preferred != nil {
			od.changePreference(nil, causePreferenceEnd, "ineligible", nil, nil, now)
		}
		return
	}
	od.preferenceGone(now)
	od.progressRound(now)
	// with what the round found, whether the preference still has its basis
	od.checkBasis(now)
	if ps.round == nil {
		od.startRound(now)
	}
}

// endConfirm ends the confirmation of a member.
func (od *onDemand) endConfirm(p C.Proxy, end string) {
	ps := od.prefer
	c, ok := ps.confirms[p]
	if !ok {
		return
	}
	delete(ps.confirms, p)
	od.recordConfirmEnd(p, c, end)
}

func (od *onDemand) recordConfirmEnd(p C.Proxy, c confirmation, end string) {
	fields := od.event("confirm")
	fields["member"] = od.ref(p)
	fields["probe"] = c.exec
	fields["end"] = end
	if end == "tested" {
		fields["by"] = od.view[p].Exec
		fields["state"] = od.view[p].State
	}
	od.record(fields)
}

// preferExcluded takes a change of the exclusions in where it takes effect: a preferred member
// excluded is preferred no more, a confirmation of one ends.
func (od *onDemand) preferExcluded(now time.Time) {
	ps := od.prefer
	if ps == nil {
		return
	}
	for p := range ps.confirms {
		if od.excluded(p) {
			od.endConfirm(p, "excluded")
		}
	}
	if p := ps.preferred; p != nil && od.isMember(p) && od.excluded(p) {
		od.changePreference(nil, causePreferenceEnd, "excluded", nil, nil, now)
	}
}

// preferenceGone ends a preference whose member is gone, yields, is on trial or excluded, or
// by cost comes first already.
func (od *onDemand) preferenceGone(now time.Time) {
	ps := od.prefer
	p := ps.preferred
	if p == nil {
		return
	}
	why := ""
	switch {
	case !od.isMember(p):
		why = "removed"
	case od.standingOf(p) == standingYield:
		why = "yield"
	case od.standingOf(p) == standingTrial:
		why = "trial"
	case od.excluded(p):
		why = "excluded"
	case ps.mode == OrderReliableCost && slices.Index(od.members, p) == 0:
		why = "first"
	}
	if why != "" {
		od.changePreference(nil, causePreferenceEnd, why, nil, nil, now)
	}
}

// checkBasis moves a preference by cost back once it no longer stands, and ends one without
// any result for a day.
func (od *onDemand) checkBasis(now time.Time) {
	ps := od.prefer
	if ps.preferred == nil {
		return
	}
	if ps.mode == OrderReliableCost {
		od.costReturn(now)
	}
	if p := ps.preferred; p != nil && now.Sub(od.lastRecord(p)) >= probe.CompareExpiry {
		od.changePreference(nil, causePreferenceEnd, "stale", nil, nil, now)
	}
}

// costReturn moves a preference by cost back to the costliest cheaper member it would stand
// for, or ends it.
func (od *onDemand) costReturn(now time.Time) {
	ps := od.prefer
	p := ps.preferred
	missing := od.unbacked(p, now)
	if len(missing) == 0 {
		return
	}
	var reasons []any
	for _, y := range missing {
		fields := od.duelFields(od.duels(p, y, now, probe.CompareCostWindow))
		fields["member"], fields["reason"] = od.ref(y), od.unbackedWhy(p, y, now)
		reasons = append(reasons, fields)
	}
	fields := map[string]any{"unbacked": reasons}
	for i := slices.Index(od.members, p) - 1; i >= 0; i-- {
		y := od.members[i]
		if !od.qualified(y) || len(od.unbacked(y, now)) > 0 {
			continue
		}
		goesBefore := false
		for _, z := range od.members[:i] {
			if od.qualified(z) {
				goesBefore = true
				break
			}
		}
		if !goesBefore {
			break
		}
		fields["goesBefore"] = od.basisFields(y, now)
		od.changePreference(y, causeCostReturn, "", nil, fields, now)
		return
	}
	od.changePreference(nil, causeCostReturn, "", nil, fields, now)
}

// changePreference records a change of the preference and orders the members by it.
func (od *onDemand) changePreference(to C.Proxy, reason, why string, basis []pairResult, extra map[string]any, now time.Time) {
	ps := od.prefer
	from := ps.preferred
	ps.preferred, ps.lastChange, ps.basis = to, now, basis
	ps.since, ps.seen = time.Time{}, time.Time{}
	if to != nil {
		ps.since, ps.seen = now, od.lastRecord(to)
	}
	od.recordPreference(from, to, reason, why, extra)
	od.computeOrder()
	od.reselect(now, selectionCause{Reason: reason})
}

// recordPreferenceEnd ends a preference as the members change, leaving the order and the
// selection to the caller.
func (od *onDemand) recordPreferenceEnd(from C.Proxy, reason, why string) {
	ps := od.prefer
	ps.preferred, ps.lastChange, ps.basis, ps.since = nil, od.now(), nil, time.Time{}
	od.recordPreference(from, nil, reason, why, nil)
}

func (od *onDemand) recordPreference(from, to C.Proxy, reason, why string, extra map[string]any) {
	fields := od.event("preference")
	fields["order"] = od.prefer.mode
	fields["from"] = od.ref(from)
	fields["to"] = od.ref(to)
	fields["reason"] = reason
	if why != "" {
		fields["why"] = why
	}
	for k, v := range extra {
		fields[k] = v
	}
	fields["display"] = od.preferenceDisplay(from, to, reason, why)
	od.record(fields)
}

// roundBlocked tells why no round may start now, "" when one may.
func (od *onDemand) roundBlocked(c C.Proxy) string {
	switch {
	case od.network != probe.NetworkNormal:
		return "network"
	case od.search != nil:
		return "search"
	case od.pinned != "":
		return "pinned"
	case c == nil || od.unprobed(c) || od.delegated(c):
		return "noCurrent"
	case od.onTrial(c):
		return "trial"
	case od.suspended != nil && od.suspended.member == c:
		return "suspended"
	}
	for _, pool := range od.memberPools[c] {
		if od.outagePools[pool] {
			return "outage"
		}
	}
	return ""
}

func (od *onDemand) startRound(now time.Time) {
	ps := od.prefer
	inUse := od.inUseLocked(now)
	active := inUse || od.leased(now)
	if active && (!ps.active || ps.reused) && now.Sub(ps.lastDone) >= probe.CompareEvery {
		// coming into use without a round lately: one as soon as it may
		ps.nextAt = now
	}
	ps.active, ps.reused = active, false
	if !od.activated || !active || now.Before(ps.nextAt) || now.Before(ps.retryAt) {
		return
	}
	c := od.current
	if od.roundBlocked(c) != "" {
		ps.recheckAt = now.Add(compareRetry)
		return
	}
	parts := od.challengers(c, !inUse, now)
	if len(parts) == 0 {
		// nobody to compare with, as members may come
		ps.recheckAt = now.Add(compareRetry)
		return
	}
	r := &compareRound{id: compareRounds.Add(1), t0: now, c: c}
	r.parts = append([]*participant{{member: c, use: useCurrent}}, parts...)
	// a test may turn out to go through a pool suspected already
	for pool, suspected := range od.outagePools {
		if suspected {
			if r.suspected == nil {
				r.suspected = map[string]bool{}
			}
			r.suspected[pool] = true
		}
	}
	if inUse {
		r.sources = append(r.sources, "inUse")
	}
	for exit := range ps.leases {
		if exit.activeAt(now) {
			r.sources = append(r.sources, exit.name())
		}
	}
	if inUse {
		slices.Sort(r.sources[1:])
	} else {
		slices.Sort(r.sources)
	}
	ps.round = r
}

// challengers picks the members tested with c: at most CompareChallenge of them.
func (od *onDemand) challengers(c C.Proxy, leasedOnly bool, now time.Time) []*participant {
	ps := od.prefer
	var picked []*participant
	taken := map[C.Proxy]bool{c: true}
	take := func(p C.Proxy, use string) bool {
		if len(picked) >= probe.CompareChallenge || taken[p] {
			return false
		}
		taken[p] = true
		picked = append(picked, &participant{member: p, use: use})
		return true
	}
	// a member on trial still lacking its witness, in a round only groups by cost ask for:
	// nothing else of the group tests it
	if leasedOnly {
		for _, p := range od.members {
			if q := od.quality[p]; q != nil && q.trial != nil && !q.trial.witness && !od.unprobed(p) && !od.excluded(p) && !od.viaSuspectedPool(p) {
				if take(p, useTrial) {
					break
				}
			}
		}
	}
	if ps.mode == OrderReliableCost {
		// one is left for the members in turn
		od.priorities(c, take, probe.CompareChallenge-1-len(picked), now)
	}
	// the members missed last round, then in turn from where the last round left off
	for _, p := range od.members {
		if ps.owed[p] && od.contender(p, now) {
			take(p, useRotation)
		}
	}
	start := 0
	if ps.cursor != nil {
		start = slices.Index(od.members, ps.cursor) + 1
	}
	for i := 0; i < len(od.members) && len(picked) < probe.CompareChallenge; i++ {
		p := od.members[(start+i)%len(od.members)]
		if od.contender(p, now) && take(p, useRotation) {
			ps.cursor = p
		}
	}
	for _, p := range picked {
		delete(ps.owed, p.member)
	}
	return picked
}

// priorities picks for a group by cost, at most ComparePriority members and one left in turn:
// a costlier candidate and a member it would go before that it was never compared with, then
// while a costlier member is preferred, those it goes before.
func (od *onDemand) priorities(c C.Proxy, take func(C.Proxy, string) bool, free int, now time.Time) {
	ps := od.prefer
	budget := probe.ComparePriority
	if free < budget {
		budget = free
	}
	ci := slices.Index(od.members, c)
	var candidates []C.Proxy
	for _, x := range od.members[ci+1:] {
		if !od.contender(x, now) {
			continue
		}
		list := od.duels(x, c, now, probe.CompareRecent)
		if xw, cw := score(list); len(list) >= probe.CompareMinimum && xw >= probe.CompareWins && cw == 0 {
			candidates = append(candidates, x)
		}
	}
	used := 0
	for n := 0; n < len(candidates) && used == 0 && budget >= 2; n++ {
		x := candidates[(ps.contrast+n)%len(candidates)]
		for _, y := range od.members {
			if y == x {
				break
			}
			if y == c || !od.qualified(y) || !od.contender(y, now) || len(od.duels(x, y, now, 1)) > 0 {
				continue
			}
			if take(x, useContrast) {
				used++
			}
			if take(y, useContrast) {
				used++
			}
			ps.contrast = (ps.contrast + n + 1) % len(candidates)
			break
		}
	}
	p := od.preferredMember()
	if p == nil || p != c {
		return
	}
	pi := slices.Index(od.members, p)
	start := 0
	if ps.overtaken != nil {
		start = slices.Index(od.members[:pi], ps.overtaken) + 1
	}
	for i := 0; i < pi && used < budget; i++ {
		y := od.members[(start+i)%pi]
		if od.qualified(y) && od.contender(y, now) && take(y, useOvertaken) {
			used++
			ps.overtaken = y
		}
	}
}

// preferNeeds are the tests of the round and the confirmations the group holds.
func (od *onDemand) preferNeeds(now time.Time, add func(p C.Proxy, rank int, source string, notBefore time.Time, notify bool)) {
	ps := od.prefer
	if ps == nil {
		return
	}
	if r := ps.round; r != nil && r.voided == "" && ps.eligible {
		for i, p := range r.parts {
			if p.missed || p.done {
				continue
			}
			// past its start limit, a test not started yet would not count: it is not asked for,
			// though one that started in time is still waited to be heard of
			if p.started.IsZero() && (p.member == r.c && !now.Before(r.t0.Add(probe.CompareStart)) ||
				p.member != r.c && !r.windowEnd.IsZero() && now.After(r.windowEnd)) {
				continue
			}
			source := probe.SourceCompare
			if p.use == useTrial {
				source = probe.SourceTrial
			}
			add(p.member, i, source, r.t0, true)
		}
	}
	for p, c := range ps.confirms {
		add(p, 10, probe.SourceConfirm, c.after, false)
	}
}

func (od *onDemand) onBegan(b probe.Began) {
	ps := od.prefer
	if ps == nil || ps.round == nil || b.Key.Cond != od.cond {
		return
	}
	r := ps.round
	p := r.part(b.Key.Target)
	if p == nil || p.missed || !p.started.IsZero() || b.Started.Before(r.t0) {
		return
	}
	p.exec, p.started = b.Exec, b.Started
	if p.member == r.c {
		if b.Started.After(r.t0.Add(probe.CompareStart)) {
			r.late = true
			return
		}
		r.windowEnd = b.Started.Add(probe.PairWindow)
		for _, q := range r.parts {
			if q != p && !q.started.IsZero() && !withinPair(q.started, b.Started) {
				q.missed = true
			}
		}
		return
	}
	if c := r.part(r.c); !c.started.IsZero() && !withinPair(b.Started, c.started) {
		p.missed = true
	}
}

// withinPair reports whether tests that started then are side by side.
func withinPair(a, b time.Time) bool {
	d := a.Sub(b)
	return d <= probe.PairWindow && d >= -probe.PairWindow
}

func (od *onDemand) preferCompletion(c probe.Completion) {
	ps := od.prefer
	if ps == nil || ps.round == nil {
		return
	}
	r := ps.round
	for _, p := range r.parts {
		if p.exec != c.Exec || p.member != c.Key.Target || p.done || p.missed {
			continue
		}
		if c.Ended.After(p.started.Add(testTimeoutOf(od.cond) + compareGrace)) {
			p.missed = true
			continue
		}
		p.done = true
		p.result, p.compared, p.comparing, p.ended = c.Result, c.Compared, c.Comparing, c.Ended
		p.pools, p.unknown = c.Pools, c.UnknownPath
		for _, pool := range p.pools {
			if r.suspected[pool] && r.voided == "" {
				r.voided = "outage"
			}
		}
	}
}

// poolsOf are the pools a member of a round goes through as far as known: those its test went
// through once it ended with a known route, those of its route otherwise.
func (od *onDemand) poolsOf(p *participant) []string {
	if p.done && !p.unknown {
		return p.pools
	}
	return od.memberPools[p.member]
}

// latchRound makes the round in progress void as a message takes effect, also when a later
// one of the same batch undoes it.
func (od *onDemand) latchRound(msg any) {
	ps := od.prefer
	if ps == nil || ps.round == nil || ps.round.voided != "" {
		return
	}
	r := ps.round
	switch msg := msg.(type) {
	case probe.NetworkChanged:
		if msg.State != probe.NetworkNormal {
			r.voided = "network"
		}
	case probe.OutageChanged:
		if msg.Cond != od.cond || msg.State != probe.OutageSuspected {
			return
		}
		// a test still running may turn out to have gone through it
		if r.suspected == nil {
			r.suspected = map[string]bool{}
		}
		r.suspected[msg.Pool] = true
		for _, p := range r.parts {
			if !p.missed && slices.Contains(od.poolsOf(p), msg.Pool) {
				r.voided = "outage"
				return
			}
		}
	case pinNote:
		if msg.name != "" {
			r.voided = "pinned"
		}
	}
}

func (od *onDemand) progressRound(now time.Time) {
	ps := od.prefer
	r := ps.round
	if r == nil {
		return
	}
	reason := r.voided
	switch {
	case reason != "":
	case od.network != probe.NetworkNormal:
		reason = "network"
	case od.pinned != "":
		reason = "pinned"
	case !od.inUseLocked(now) && !od.leased(now):
		reason = "unused"
	case !od.isMember(r.c):
		reason = "removed"
	}
	if reason == "" {
		for _, p := range r.parts {
			for _, pool := range od.poolsOf(p) {
				if !p.missed && od.outagePools[pool] {
					reason = "outage"
				}
			}
		}
	}
	if reason != "" {
		od.endRound(now, "void", reason, nil)
		return
	}
	c := r.part(r.c)
	if r.late {
		od.endRound(now, "canceled", "notStarted", nil)
		return
	}
	if c.started.IsZero() {
		if !now.Before(r.t0.Add(probe.CompareStart + compareSettle)) {
			od.endRound(now, "canceled", "notStarted", nil)
		}
		return
	}
	// a test that started within the window may be heard of a little after it
	if now.Before(r.windowEnd.Add(compareSettle)) {
		return
	}
	timeout := testTimeoutOf(od.cond)
	pending := false
	for _, p := range r.parts {
		switch {
		case p.missed || p.done:
		case p.started.IsZero():
			p.missed = true
		case now.Before(p.started.Add(timeout + compareGrace + compareSettle)):
			pending = true
		default:
			p.missed = true
		}
	}
	switch {
	case c.missed:
		od.endRound(now, "canceled", "lost", nil)
	case !pending:
		od.finishRound(now)
	}
}

func testTimeoutOf(cond probe.Condition) time.Duration {
	if cond.Timeout > 0 {
		return cond.Timeout
	}
	return probe.DefaultTestTimeout
}

// finishRound takes the results of a round in: the pairs it compared, the confirmations of
// the failures only it asked for, and the preference.
func (od *onDemand) finishRound(now time.Time) {
	ps := od.prefer
	r := ps.round
	var field []*participant
	for _, p := range r.parts {
		if p.use != useTrial && p.done && !p.missed && !p.result.Canceled {
			field = append(field, p)
		}
	}
	if c := r.part(r.c); !c.done || c.missed || c.result.Canceled {
		od.endRound(now, "canceled", "lost", nil)
		return
	}
	allFailed := true
	for _, p := range field {
		if tierOf(p.result) != tierFailed {
			allFailed = false
		}
	}
	if allFailed {
		if ps.mode == OrderReliable && od.basisCovered() {
			od.changePreference(nil, causePreferenceEnd, "basisCovered", nil, nil, now)
		}
		od.endRound(now, "void", "allFailed", nil)
		return
	}
	at := r.part(r.c).started
	var pairs []any
	for i, x := range field {
		for _, y := range field[i+1:] {
			od.note(x.member, y.member, pairResult{round: r.id, at: at, sides: [2]side{sideOf(x), sideOf(y)}})
			winner := any(nil)
			switch tx, ty := tierOf(x.result), tierOf(y.result); {
			case tx > ty:
				winner = od.ref(x.member)
			case ty > tx:
				winner = od.ref(y.member)
			}
			pairs = append(pairs, map[string]any{"members": od.refs([]C.Proxy{x.member, y.member}), "winner": winner})
		}
	}
	var confirms []any
	for _, p := range field {
		if !p.compared || !od.isMember(p.member) || od.excluded(p.member) {
			continue
		}
		// a test that started since it ended does for the confirmation already
		if ev := od.view[p.member]; !ev.Started.Before(p.ended) {
			continue
		}
		if _, ok := ps.confirms[p.member]; !ok {
			ps.confirms[p.member] = confirmation{after: p.ended, exec: p.exec}
			confirms = append(confirms, map[string]any{"member": od.ref(p.member), "probe": p.exec, "after": formatTime(p.ended)})
		}
	}
	var decision map[string]any
	if ps.mode == OrderReliableCost {
		decision = od.judgeCost(r, field, now)
	} else {
		decision = od.judgeReliable(r, field, now)
	}
	od.endRound(now, "done", "", map[string]any{"pairs": pairs, "confirms": confirms, "decision": decision})
}

func sideOf(p *participant) side {
	return side{exec: p.exec, started: p.started, tier: tierOf(p.result), first: p.result.FirstElapsed, pools: p.pools, unknown: p.unknown}
}

// gate tells why no change of the preference may follow a round, "" when one may.
func (od *onDemand) gate(r *compareRound, now time.Time) string {
	ps := od.prefer
	switch {
	case od.search != nil, r.searched:
		return "search"
	case od.current != r.c:
		return "currentChanged"
	case ps.preferred != nil && od.isMember(ps.preferred) && ps.preferred != r.c:
		return "awaitPreferred"
	case !ps.lastChange.IsZero() && now.Sub(ps.lastChange) < probe.CompareStay:
		return "stay"
	}
	return ""
}

// beats reports whether x did better than c as a preference asks: in the latest valid
// results, which this round leads, enough wins and none for c.
func (od *onDemand) beats(r *compareRound, x, c C.Proxy, now time.Time) (bool, []duel) {
	list := od.duels(x, c, now, probe.CompareRecent)
	if len(list) == 0 || list[0].round != r.id || list[0].x.tier <= list[0].y.tier {
		return false, list
	}
	xw, cw := score(list)
	return len(list) >= probe.CompareMinimum && xw >= probe.CompareWins && cw == 0, list
}

func (od *onDemand) duelFields(list []duel) map[string]any {
	x, y := score(list)
	rounds := make([]uint64, 0, len(list))
	for _, d := range list {
		rounds = append(rounds, d.round)
	}
	return map[string]any{"rounds": rounds, "wins": x, "losses": y}
}

func (od *onDemand) judgeReliable(r *compareRound, field []*participant, now time.Time) map[string]any {
	if od.basisCovered() {
		od.changePreference(nil, causePreferenceEnd, "basisCovered", nil, nil, now)
		return map[string]any{"change": "end", "why": "basisCovered"}
	}
	var best C.Proxy
	var bestList []duel
	bestWins := 0
	for _, p := range field {
		x := p.member
		if x == r.c || !od.contender(x, now) {
			continue
		}
		ok, list := od.beats(r, x, r.c, now)
		if !ok {
			continue
		}
		wins, _ := score(list)
		if best == nil || wins > bestWins || wins == bestWins && slices.Index(od.members, x) < slices.Index(od.members, best) {
			best, bestList, bestWins = x, list, wins
		}
	}
	if best == nil {
		return map[string]any{"change": nil}
	}
	if why := od.gate(r, now); why != "" {
		return map[string]any{"change": nil, "candidate": od.ref(best), "why": why, "basis": od.duelFields(bestList)}
	}
	var basis []pairResult
	for _, d := range bestList {
		if d.x.tier > d.y.tier {
			basis = append(basis, pairResult{round: d.round, sides: [2]side{d.x, d.y}})
		}
	}
	od.changePreference(best, causePreferred, "", basis, map[string]any{"over": od.ref(r.c), "basis": od.duelFields(bestList)}, now)
	return map[string]any{"change": "preferred", "to": od.ref(best), "basis": od.duelFields(bestList)}
}

// basisCovered reports whether the wins a preference of the same cost was taken on all turned
// out to fall in a time the local network or a pool did not seem normal.
func (od *onDemand) basisCovered() bool {
	ps := od.prefer
	if ps.preferred == nil || len(ps.basis) == 0 {
		return false
	}
	for _, b := range ps.basis {
		if !od.covered(b.sides[0]) && !od.covered(b.sides[1]) {
			return false
		}
	}
	return true
}

func (od *onDemand) judgeCost(r *compareRound, field []*participant, now time.Time) map[string]any {
	ci := slices.Index(od.members, r.c)
	var candidate C.Proxy
	var candidateList []duel
	for _, p := range field {
		x := p.member
		if xi := slices.Index(od.members, x); xi <= ci || !od.contender(x, now) {
			continue
		}
		ok, list := od.beats(r, x, r.c, now)
		if !ok || len(od.unbacked(x, now)) > 0 {
			continue
		}
		if candidate == nil || slices.Index(od.members, x) < slices.Index(od.members, candidate) {
			candidate, candidateList = x, list
		}
	}
	if candidate == nil {
		return map[string]any{"change": nil}
	}
	if why := od.gate(r, now); why != "" {
		return map[string]any{"change": nil, "candidate": od.ref(candidate), "why": why, "basis": od.duelFields(candidateList)}
	}
	od.changePreference(candidate, causePreferred, "", nil, map[string]any{"over": od.ref(r.c), "basis": od.duelFields(candidateList), "goesBefore": od.basisFields(candidate, now)}, now)
	return map[string]any{"change": "preferred", "to": od.ref(candidate), "basis": od.duelFields(candidateList)}
}

// basisFields tell how a costlier member did against each member it goes before.
func (od *onDemand) basisFields(x C.Proxy, now time.Time) []any {
	var list []any
	for _, y := range od.members {
		if y == x {
			break
		}
		if od.qualified(y) {
			fields := od.duelFields(od.duels(x, y, now, probe.CompareCostWindow))
			fields["member"] = od.ref(y)
			list = append(list, fields)
		}
	}
	return list
}

// endRound records how a round ended and when the next one is.
func (od *onDemand) endRound(now time.Time, end, reason string, extra map[string]any) {
	ps := od.prefer
	r := ps.round
	ps.round = nil
	counted := end == "done" || reason == "allFailed"
	switch {
	case counted:
		ps.lastDone = now
		ps.nextAt = now.Add(jittered(probe.CompareEvery))
	case reason != "unused":
		// tried again in a while, not over and over as what voided it goes on
		ps.retryAt = now.Add(compareRetry)
	}
	for _, p := range r.parts {
		if p.member != r.c && p.use != useTrial && p.missed && counted && od.isMember(p.member) {
			ps.owed[p.member] = true
		}
	}
	fields := od.event("compare")
	fields["round"] = r.id
	fields["order"] = ps.mode
	fields["started"] = formatTime(r.t0)
	fields["end"] = end
	if reason != "" {
		fields["reason"] = reason
	}
	fields["sources"] = r.sources
	members := make([]any, 0, len(r.parts))
	for _, p := range r.parts {
		m := map[string]any{"member": od.ref(p.member), "use": p.use}
		if p.exec != 0 {
			m["probe"] = p.exec
			m["started"] = formatTime(p.started)
		}
		if p.comparing {
			m["comparing"] = true
		}
		switch {
		case p.missed:
			m["missed"] = true
		case p.done && !p.result.Canceled:
			m["first"] = tierOf(p.result).String()
			if p.result.FirstSatisfied {
				m["firstElapsed"] = p.result.FirstElapsed.Milliseconds()
			}
			m["ok"] = p.result.Satisfied
			if p.compared {
				m["compared"] = true
			}
			if p.unknown {
				m["unknownPath"] = true
			}
		}
		members = append(members, m)
	}
	fields["members"] = members
	for k, v := range extra {
		fields[k] = v
	}
	fields["display"] = od.compareDisplay(r, end, reason, extra)
	od.record(fields)
}

// preferTimes are the times the rounds and the preference wait for.
func (od *onDemand) preferTimes(consider func(time.Time), now time.Time) {
	ps := od.prefer
	if ps == nil || !ps.eligible {
		return
	}
	for exit := range ps.leases {
		if active := exit.lastActive.Load(); active != 0 {
			consider(time.Unix(0, active).Add(probe.UseWindow))
		}
	}
	if r := ps.round; r != nil {
		consider(r.t0.Add(probe.CompareStart + compareSettle))
		if !r.windowEnd.IsZero() {
			consider(r.windowEnd.Add(compareSettle))
		}
		timeout := testTimeoutOf(od.cond)
		for _, p := range r.parts {
			if !p.started.IsZero() && !p.done {
				consider(p.started.Add(timeout + compareGrace + compareSettle))
			}
		}
	} else if od.inUseLocked(now) || od.leased(now) {
		consider(ps.nextAt)
		consider(ps.retryAt)
		consider(ps.recheckAt)
	}
	if len(ps.leases) > 0 {
		// a group by cost may come into use again unheard of
		consider(now.Add(compareRecheck))
	}
	if p := ps.preferred; p != nil {
		consider(od.lastRecord(p).Add(probe.CompareExpiry))
		if ps.mode == OrderReliableCost {
			// a win it stands on may expire, or turn out covered
			for key, pr := range ps.records {
				if key[0] != p {
					continue
				}
				for i := range pr.results {
					consider(pr.results[i].at.Add(probe.CompareExpiry))
				}
			}
			consider(now.Add(compareRecheck))
		}
	}
}

// dialersOf finds, outside the strategy lock, the groups of members of one cost that members
// go through: those a group by cost has rounds of compared.
func (od *onDemand) dialersOf(members []C.Proxy) map[C.Proxy]*onDemand {
	if od.ordering != OrderReliableCost {
		return nil
	}
	dialers := map[C.Proxy]*onDemand{}
	for _, p := range members {
		name := p.ProxyInfo().DialerProxy
		if name == "" {
			continue
		}
		if dialer := od.coord.Lookup(name); dialer != nil {
			if d := onDemandOf(dialer); d != nil && d != od && d.ordering == OrderReliable {
				dialers[p] = d
			}
		}
	}
	return dialers
}

// leaseChanges are the groups of members of one cost whose rounds this group by cost asks for
// anew, or no longer does: those its members not excluded go through, while it is in use.
func (od *onDemand) leaseChanges(dialers map[C.Proxy]*onDemand, now time.Time) (on, off []*onDemand) {
	ps := od.prefer
	if ps == nil || od.ordering != OrderReliableCost {
		return nil, nil
	}
	want := map[*onDemand]bool{}
	if ps.eligible {
		for _, p := range od.members {
			if d := dialers[p]; d != nil && !od.unprobed(p) && !od.excluded(p) {
				want[d] = true
			}
		}
	}
	// the groups read whether it is in use; coming into use, it tells them again, which
	// starts their rounds
	active := od.inUseLocked(now)
	again := active && (!ps.sentActive || ps.resend)
	ps.sentActive, ps.resend = active, false
	for d := range want {
		if !ps.sent[d] || again {
			on = append(on, d)
			ps.sent[d] = true
		}
	}
	for d := range ps.sent {
		if !want[d] {
			off = append(off, d)
			delete(ps.sent, d)
		}
	}
	return on, off
}

// sendLeases tells the groups of the changes, outside the strategy lock.
func (od *onDemand) sendLeases(on, off []*onDemand) {
	for _, d := range on {
		d.post(exitLease{exit: od, on: true})
	}
	for _, d := range off {
		d.post(exitLease{exit: od, on: false})
	}
}

// revokeLeases withdraws what the group asked of others, as it stops.
func (od *onDemand) revokeLeases() {
	od.mutex.Lock()
	var off []*onDemand
	if ps := od.prefer; ps != nil {
		for d := range ps.sent {
			off = append(off, d)
		}
		ps.sent = map[*onDemand]bool{}
	}
	od.mutex.Unlock()
	od.sendLeases(nil, off)
}

func (od *onDemand) onLease(l exitLease, now time.Time) {
	ps := od.prefer
	if ps == nil {
		return
	}
	if l.on {
		// told again as the group by cost comes into use
		ps.leases[l.exit] = true
		ps.reused = true
		return
	}
	delete(ps.leases, l.exit)
}

// whyText tells why a preference ended.
func whyText(why string) probe.Text {
	switch why {
	case "removed":
		return probe.T("偏好成员已不在组中", "the preferred member left the group")
	case "yield":
		return probe.T("偏好成员让位", "the preferred member yields")
	case "trial":
		return probe.T("偏好成员进入试用", "the preferred member is on trial")
	case "excluded":
		return probe.T("偏好成员被排除", "the preferred member is excluded")
	case "stale":
		return probe.T("24 小时没有它的同场结果", "no result of it side by side for 24 hours")
	case "first":
		return probe.T("它已排在成本顺序首位", "it comes first by cost already")
	case "basisCovered":
		return probe.T("所依据的胜场都落在本机网络或流量池异常期间", "the wins it was taken on all fell in a time the network or a pool did not seem normal")
	case "ineligible":
		return probe.T("成员不再适用该顺序", "the members no longer allow the order")
	}
	return probe.T("偏好结束", "the preference ended")
}

func (od *onDemand) preferenceDisplay(from, to C.Proxy, reason, why string) map[string]any {
	var text probe.Text
	switch {
	case reason == causePreferred:
		text = probe.Tf("偏好 %s：同场测试中反复更可靠", "Prefers %s: it did better side by side again and again", od.label(to))
	case reason == causeCostReturn && to != nil:
		text = probe.Tf("偏好从 %s 回到更便宜的 %s：它已不比被越过的成员更可靠", "Preference back from %s to the cheaper %s: it no longer does better than the members it went before", od.label(from), od.label(to))
	case reason == causeCostReturn:
		text = probe.Tf("不再偏好 %s，回到成本顺序：它已不比被越过的成员更可靠", "No longer prefers %s, back to the order by cost: it no longer does better than the members it went before", od.label(from))
	default:
		w := whyText(why)
		text = probe.Text{
			"zh": "不再偏好 " + od.label(from) + "，回到配置顺序：" + w["zh"],
			"en": "No longer prefers " + od.label(from) + ", back to config order: " + w["en"],
		}
	}
	return od.display("preference", probe.LevelInfo, text, from, to)
}

func (od *onDemand) compareDisplay(r *compareRound, end, reason string, extra map[string]any) map[string]any {
	members := make([]C.Proxy, 0, len(r.parts))
	for _, p := range r.parts {
		members = append(members, p.member)
	}
	var text probe.Text
	switch end {
	case "done":
		var zh, en []string
		for _, p := range r.parts {
			if p.missed || !p.done {
				zh = append(zh, od.label(p.member)+" 缺测")
				en = append(en, od.label(p.member)+" missed")
				continue
			}
			t := tierOf(p.result)
			zh = append(zh, od.label(p.member)+" "+map[tier]string{tierTimely: "及时", tierSlow: "慢", tierFailed: "失败"}[t])
			en = append(en, od.label(p.member)+" "+t.String())
		}
		text = probe.Text{"zh": "同场测试：" + strings.Join(zh, "，"), "en": "Tested side by side: " + strings.Join(en, ", ")}
	case "canceled":
		text = probe.Tf("同场测试取消（%s）", "Side-by-side test canceled (%s)", reason)
	default:
		text = probe.Tf("同场测试作废（%s）", "Side-by-side test void (%s)", reason)
	}
	return od.display("compare", probe.LevelInfo, text, members...)
}

// clone copies what a group replacing this one takes over: not the round in progress, whose
// tests the old registration asked for.
func (ps *preferState) clone() *preferState {
	if ps == nil {
		return nil
	}
	copied := newPreferState(ps.mode)
	copied.nextAt, copied.lastDone, copied.cursor, copied.retryAt = ps.nextAt, ps.lastDone, ps.cursor, ps.retryAt
	copied.active = ps.active
	copied.preferred, copied.since, copied.lastChange, copied.seen = ps.preferred, ps.since, ps.lastChange, ps.seen
	copied.basis = append([]pairResult(nil), ps.basis...)
	copied.contrast, copied.overtaken = ps.contrast, ps.overtaken
	for p := range ps.owed {
		copied.owed[p] = true
	}
	for p, c := range ps.confirms {
		copied.confirms[p] = c
	}
	for key, pr := range ps.records {
		if key[0] != pr.members[0] {
			continue
		}
		dup := &pairRecords{members: pr.members, results: append([]pairResult(nil), pr.results...)}
		copied.records[key] = dup
		copied.records[[2]C.Proxy{key[1], key[0]}] = dup
	}
	return copied
}

// inheritPrefer takes over what the group replaced found, under the same order and test: the
// results of members both still in it, the preference of one still in it.
func (od *onDemand) inheritPrefer(old *preferState, now time.Time) bool {
	ps := od.prefer
	if old == nil {
		return false
	}
	if ps == nil || old.mode != ps.mode {
		// the confirmations of another order end with it
		for p, c := range old.confirms {
			od.recordConfirmEnd(p, c, "orderChanged")
		}
		return false
	}
	ps.nextAt, ps.lastDone, ps.lastChange, ps.retryAt = old.nextAt, old.lastDone, old.lastChange, old.retryAt
	ps.active = old.active
	ps.contrast = old.contrast
	for key, pr := range old.records {
		if od.isMember(key[0]) && od.isMember(key[1]) {
			ps.records[key] = pr
		}
	}
	for p := range old.owed {
		if od.isMember(p) {
			ps.owed[p] = true
		}
	}
	for p, c := range old.confirms {
		if od.isMember(p) {
			ps.confirms[p] = c
		} else {
			od.recordConfirmEnd(p, c, "removed")
		}
	}
	if od.isMember(old.cursor) {
		ps.cursor = old.cursor
	}
	if od.isMember(old.overtaken) {
		ps.overtaken = old.overtaken
	}
	if od.isMember(old.preferred) {
		ps.preferred, ps.since, ps.basis, ps.seen = old.preferred, old.since, old.basis, old.seen
		return true
	}
	if old.preferred != nil {
		// gone with the reload: the preference ended then
		ps.lastChange = now
		od.recordPreference(old.preferred, nil, causePreferenceEnd, "removed", nil)
	}
	return false
}

// preferSection shows the rounds and the preference of the group.
func (od *onDemand) preferSection(now time.Time) map[string]any {
	ps := od.prefer
	if ps == nil {
		return nil
	}
	mode := probe.T("同档按可靠性", "by reliability")
	back := probe.T("无，按配置顺序", "none, in config order")
	if ps.mode == OrderReliableCost {
		mode = probe.T("按成本，可为可靠性越过便宜成员", "by cost, a costlier member first while it does better")
		back = probe.T("无，按成本顺序", "none, in order of cost")
	}
	items := []map[string]any{detail(probe.T("顺序", "order"), mode, "")}
	if !ps.eligible {
		items = append(items, detail(probe.T("未生效", "not in effect"), probe.T(
			"该顺序只用于成员都是节点、内置成员排在最后的组", "the order takes nodes followed by built-in members only"), probe.LevelWarn))
		return section(probe.T("可靠性偏好", "preference"), items...)
	}
	switch p := od.preferredMember(); {
	case p != nil:
		items = append(items, detail(probe.T("偏好", "preferred"), probe.T(od.label(p), od.label(p)), probe.LevelInfo))
		items = append(items, moment("preferredSince", probe.T("偏好开始", "since"), ps.since))
		if ps.mode == OrderReliableCost {
			var gone []C.Proxy
			for _, y := range od.members {
				if y == p {
					break
				}
				if od.qualified(y) {
					gone = append(gone, y)
				}
			}
			items = append(items, detail(probe.T("越过", "goes before"), od.labels(gone), ""))
		}
	case ps.preferred != nil:
		items = append(items, detail(probe.T("偏好", "preferred"), probe.Tf(
			"%s（固定期间不生效）", "%s (not while a member is pinned)", od.label(ps.preferred)), probe.LevelMuted))
	default:
		items = append(items, detail(probe.T("偏好", "preferred"), back, ""))
	}
	if !ps.lastDone.IsZero() {
		items = append(items, moment("lastRound", probe.T("上次同场测试", "last round"), ps.lastDone))
	}
	switch {
	case ps.round != nil:
		items = append(items, detail(probe.T("同场测试", "round"), probe.T("进行中", "in progress"), probe.LevelInfo))
	case od.inUseLocked(now) || od.leased(now):
		next := maxTime(ps.nextAt, ps.retryAt)
		if next.Before(now) {
			next = maxTime(now, ps.recheckAt)
		}
		items = append(items, moment("nextRound", probe.T("下次同场测试", "next round"), next))
	default:
		items = append(items, detail(probe.T("同场测试", "rounds"), probe.T("空闲时不做", "none while idle"), probe.LevelMuted))
	}
	var exits []string
	for exit := range ps.leases {
		if exit.activeAt(now) {
			exits = append(exits, exit.name())
		}
	}
	if len(exits) > 0 {
		slices.Sort(exits)
		items = append(items, detail(probe.T("应出口组要求", "for the groups by cost"), probe.T(strings.Join(exits, "、"), strings.Join(exits, ", ")), probe.LevelInfo))
	}
	items = append(items, detail(probe.T("说明", "about"), probe.T(
		"在用时每 30 分钟同时测一次当前选择与至多 3 个其他成员，按第一次请求是否 1 秒内成功比较；只为比较的测试失败不影响共享的判定",
		"in use, the member in use and up to 3 others are tested side by side every 30 minutes, compared by whether the first request succeeds within a second; a failure of a test only for the comparison changes nothing shared"),
		probe.LevelMuted))
	return section(probe.T("可靠性偏好", "preference"), items...)
}

// preferMemberSection shows how a member did side by side with the member in use.
func (od *onDemand) preferMemberSection(p C.Proxy, now time.Time) map[string]any {
	ps := od.prefer
	if ps == nil || !ps.eligible || od.current == nil || od.unprobed(p) {
		return nil
	}
	items := []map[string]any{}
	if p != od.current {
		list := od.duels(p, od.current, now, probe.CompareRecent)
		if len(list) == 0 {
			items = append(items, detail(probe.Tf("与 %s", "with %s", od.label(od.current)), probe.T("近期没有有效的同场结果", "no valid result side by side lately"), probe.LevelMuted))
		} else {
			wins, losses := score(list)
			items = append(items, detail(probe.Tf("与 %s", "with %s", od.label(od.current)), probe.Tf(
				"最近 %d 次：胜 %d，负 %d，平 %d", "latest %d: %d won, %d lost, %d even", len(list), wins, losses, len(list)-wins-losses), ""))
		}
	}
	if ps.mode == OrderReliableCost && p == od.preferredMember() {
		for _, y := range od.members {
			if y == p {
				break
			}
			if !od.qualified(y) {
				continue
			}
			x, z := score(od.duels(p, y, now, probe.CompareCostWindow))
			level := ""
			if x <= z {
				level = probe.LevelWarn
			}
			items = append(items, detail(probe.Tf("越过 %s", "before %s", od.label(y)), probe.Tf("胜 %d，负 %d", "%d won, %d lost", x, z), level))
		}
	}
	if len(items) == 0 {
		return nil
	}
	return section(probe.T("同场比较", "side by side"), items...)
}

// endPrefer ends what the group holds of its rounds as it stops for good: the round in
// progress and the confirmations.
func (od *onDemand) endPrefer(reason string) {
	ps := od.prefer
	if ps == nil {
		return
	}
	if ps.round != nil {
		od.endRound(od.now(), "canceled", reason, nil)
	}
	for p := range ps.confirms {
		od.endConfirm(p, reason)
	}
}

// cancelRoundForReload records the round in progress as canceled by the reload: the group
// replacing this one starts its own, with no wait for it.
func (od *onDemand) cancelRoundForReload() {
	ps := od.prefer
	if ps == nil || ps.round == nil {
		return
	}
	retryAt := ps.retryAt
	od.endRound(od.now(), "canceled", "reload", nil)
	ps.retryAt = retryAt
}
