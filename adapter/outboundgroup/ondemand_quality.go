package outboundgroup

import (
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

// The standing of a member in a group, by what its memory of anomalies tells.
type standing uint8

const (
	standingNormal standing = iota
	standingYield           // moved after the members not yielding, until it may try again
	standingTrial           // back in place, dialed alongside a standby until it proved itself
)

func (s standing) String() string {
	switch s {
	case standingYield:
		return "yield"
	case standingTrial:
		return "trial"
	default:
		return "normal"
	}
}

// Reasons to put a member on trial.
const (
	trialReturned   = "returned"   // its counts fell and its readmission time came
	trialPersistent = "persistent" // back from a persistent failure
	trialOutage     = "outage"     // back from an outage of its pool
	trialRepeat     = "repeat"     // a second failure within RepeatWindow
)

// trialState is a round of trial of a member.
type trialState struct {
	round     uint64
	reason    string
	started   time.Time
	effective time.Duration // time on trial, the times of common events left out
	resumed   time.Time     // since when it counts, zero while paused
	witness   bool          // a new connection received data, or a test found it alive, since it started
	// pending are the anomalies of the round still to be attributed, by the test that found
	// a failure or the test of the preferred member a degradation compared, with when it
	// started.
	pending map[uint64]time.Time
	failed  bool
}

// memberQuality is what a group keeps of the anomalies of a member and its standing.
type memberQuality struct {
	standing     standing
	since        time.Time
	readmitAt    time.Time
	failedTrials int
	trial        *trialState
	// reason is a reason to put the member on trial, waiting until it is available again.
	reason       string
	degradations []probe.QualityEvent
	// consumed are the failures that already put the member on trial for coming again, with
	// when they started; forgotten once out of the long window.
	consumed map[uint64]time.Time
}

// trialRounds numbers the rounds of trial of this process.
var trialRounds atomic.Uint64

// clone copies what a group keeps of a member, for the group replacing it.
func (q *memberQuality) clone() *memberQuality {
	copied := *q
	copied.degradations = append([]probe.QualityEvent(nil), q.degradations...)
	copied.consumed = make(map[uint64]time.Time, len(q.consumed))
	for k, v := range q.consumed {
		copied.consumed[k] = v
	}
	if q.trial != nil {
		trial := *q.trial
		trial.pending = make(map[uint64]time.Time, len(q.trial.pending))
		for k, v := range q.trial.pending {
			trial.pending[k] = v
		}
		copied.trial = &trial
	}
	return &copied
}

// absorb adds to the state a group handed over what the group replacing it found itself since
// it was published: the degradations and the failures it counted, and a reason for a trial.
// The standing handed over stands, as the replaced group followed the member longer.
func (q *memberQuality) absorb(own *memberQuality) {
	for exec, at := range own.consumed {
		q.consumed[exec] = at
	}
	for _, event := range own.degradations {
		known := false
		for _, have := range q.degradations {
			diff := have.At.Sub(event.At)
			if have.Exec == event.Exec || (diff > -probe.DegradeMerge && diff < probe.DegradeMerge) {
				known = true
				break
			}
		}
		if !known {
			q.degradations = append(q.degradations, event)
		}
	}
	slices.SortFunc(q.degradations, func(a, b probe.QualityEvent) int { return a.At.Compare(b.At) })
	if q.standing != standingNormal {
		return
	}
	switch {
	case own.standing == standingTrial && own.trial != nil:
		// it went on trial on what only it heard of: as the replaced group would have
		q.standing, q.since, q.trial = standingTrial, own.since, own.trial
	case q.reason == "":
		q.reason = own.reason
	}
}

func (od *onDemand) qualityOf(p C.Proxy) *memberQuality {
	q := od.quality[p]
	if q == nil {
		q = &memberQuality{consumed: map[uint64]time.Time{}}
		od.quality[p] = q
	}
	return q
}

func (od *onDemand) standingOf(p C.Proxy) standing {
	if q := od.quality[p]; q != nil {
		return q.standing
	}
	return standingNormal
}

func (od *onDemand) onTrial(p C.Proxy) bool {
	return od.standingOf(p) == standingTrial
}

// countsOf counts the anomalies of a member: its failures, its degradations in this group,
// and the outages of the pools it goes through, the worst of them.
func (od *onDemand) countsOf(p C.Proxy, now time.Time) probe.Counts {
	if p == nil || probe.IsBuiltin(p) {
		return probe.Counts{}
	}
	counts := od.coord.MemberCounts(probe.Key{Target: p, Cond: od.cond})
	if q := od.quality[p]; q != nil {
		counts = counts.Add(probe.CountEvents(q.degradations, now))
	}
	var pools probe.Counts
	for _, pool := range od.memberPools[p] {
		pools = pools.Max(od.coord.PoolCounts(pool, od.cond))
	}
	return counts.Add(pools)
}

// paused reports whether the trial of a member does not count now: the local network does not
// seem normal, or a pool it goes through seems out.
func (od *onDemand) paused(p C.Proxy) bool {
	if od.network != probe.NetworkNormal {
		return true
	}
	for _, pool := range od.memberPools[p] {
		if od.outagePools[pool] {
			return true
		}
	}
	return false
}

// giveTrialReason gives a member a reason to go on trial, as the table of entries decides: a
// member yielding stays so until its counts and readmission allow, one on trial goes on with
// the round it is in.
func (od *onDemand) giveTrialReason(p C.Proxy, reason string) {
	if probe.IsBuiltin(p) || !od.isMember(p) {
		return
	}
	q := od.qualityOf(p)
	if q.standing != standingNormal {
		return
	}
	if q.reason == "" {
		q.reason = reason
	}
}

// updateStandings moves the members between the standings, and reports whether the
// effective order changed.
func (od *onDemand) updateStandings(now time.Time) (changed bool, cause string) {
	for _, p := range od.members {
		if probe.IsBuiltin(p) {
			continue
		}
		q := od.qualityOf(p)
		counts := od.countsOf(p, now)
		if t := q.trial; t != nil {
			od.advanceTrial(p, t, now)
		}
		before := q.standing
		switch q.standing {
		case standingNormal:
			switch {
			case counts.Recent >= probe.YieldRecent || counts.Long >= probe.YieldLong:
				od.yield(p, q, counts, now, "counts")
			case q.reason != "" && od.available(p) && !now.Before(q.readmitAt):
				od.startTrial(p, q, counts, now, q.reason)
			}
		case standingYield:
			if counts.Recent <= probe.ReturnRecent && counts.Long <= probe.ReturnLong && !now.Before(q.readmitAt) {
				od.startTrial(p, q, counts, now, trialReturned)
			}
		case standingTrial:
			t := q.trial
			switch {
			case !t.failed && len(t.pending) == 0 && (counts.Recent >= probe.YieldRecent || counts.Long >= probe.YieldLong):
				// the counts call for yielding, by what the trial did not fail on; an anomaly
				// of the trial still to be attributed is waited for, as the counts may already
				// hold it
				od.yield(p, q, counts, now, "counts")
			case t.failed:
				q.failedTrials++
				readmit := probe.ReadmitFirst
				for i := 1; i < q.failedTrials && readmit < probe.ReadmitMax; i++ {
					readmit *= 2
				}
				if readmit > probe.ReadmitMax {
					readmit = probe.ReadmitMax
				}
				od.yieldAfterTrial(p, q, counts, now, now.Add(readmit))
			case t.effective >= probe.TrialLength && len(t.pending) == 0 && t.witness:
				od.restore(p, q, counts, now)
			}
		}
		if q.standing != before {
			changed = true
			switch q.standing {
			case standingYield:
				cause = causeDemoted
			case standingTrial:
				if cause != causeDemoted {
					cause = causeTrial
				}
			default:
				if cause == "" {
					cause = causeRestored
				}
			}
		}
	}
	return changed, cause
}

func (od *onDemand) advanceTrial(p C.Proxy, t *trialState, now time.Time) {
	paused := od.paused(p)
	if !t.resumed.IsZero() {
		if now.After(t.resumed) {
			t.effective += now.Sub(t.resumed)
		} else {
			now = t.resumed
		}
		t.resumed = time.Time{}
	}
	if !paused {
		t.resumed = now
	}
}

// settleTrials counts the time of the trials up to at, when a common event began or ended,
// so that it counts as of when it changed however late the notice is handled.
func (od *onDemand) settleTrials(at time.Time) {
	if at.IsZero() {
		at = od.now()
	}
	for p, q := range od.quality {
		if t := q.trial; t != nil {
			od.advanceTrial(p, t, at)
		}
	}
}

func (od *onDemand) yield(p C.Proxy, q *memberQuality, counts probe.Counts, now time.Time, reason string) {
	var round uint64 // of the trial it ends, if any
	if q.trial != nil {
		round = q.trial.round
	}
	q.standing, q.since, q.readmitAt, q.trial, q.reason = standingYield, now, now, nil, ""
	od.recordStanding(p, q, counts, reason, round)
}

func (od *onDemand) yieldAfterTrial(p C.Proxy, q *memberQuality, counts probe.Counts, now, readmit time.Time) {
	round := q.trial.round
	q.standing, q.since, q.readmitAt, q.trial, q.reason = standingYield, now, readmit, nil, ""
	od.recordStanding(p, q, counts, "trialFailed", round)
}

func (od *onDemand) startTrial(p C.Proxy, q *memberQuality, counts probe.Counts, now time.Time, reason string) {
	q.standing, q.since, q.reason = standingTrial, now, ""
	q.trial = &trialState{round: trialRounds.Add(1), reason: reason, started: now, pending: map[uint64]time.Time{}}
	if !od.paused(p) {
		q.trial.resumed = now
	}
	od.recordStanding(p, q, counts, reason, 0)
}

func (od *onDemand) restore(p C.Proxy, q *memberQuality, counts probe.Counts, now time.Time) {
	round := q.trial.round
	q.standing, q.since, q.trial, q.failedTrials = standingNormal, now, nil, 0
	od.recordStanding(p, q, counts, "trialPassed", round)
}

// recordStanding records the standing of a member, with the round of the trial it is in or,
// as given, the one it ended.
func (od *onDemand) recordStanding(p C.Proxy, q *memberQuality, counts probe.Counts, reason string, ended uint64) {
	fields := od.event("standing")
	fields["member"] = od.ref(p)
	fields["standing"] = q.standing.String()
	fields["reason"] = reason
	fields["counts"] = counts
	fields["readmitAt"] = nilIfZero(q.readmitAt)
	fields["failedTrials"] = q.failedTrials
	if q.trial != nil {
		fields["round"] = q.trial.round
	} else if ended != 0 {
		fields["round"] = ended
	}
	fields["failures"] = eventTimes(od.coord.Failures(probe.Key{Target: p, Cond: od.cond}))
	fields["degradations"] = eventTimes(q.degradations)
	fields["pools"] = od.memberPools[p]
	fields["display"] = od.standingDisplay(p, q, counts, reason)
	od.record(fields)
}

func eventTimes(events []probe.QualityEvent) []any {
	list := make([]any, 0, len(events))
	for _, event := range events {
		list = append(list, map[string]any{"at": formatTime(event.At), "weight": event.Weight, "probe": event.Exec})
	}
	return list
}

// onAttributed takes a failure of a member attributed: counted for it, it may fail its trial
// or, coming soon after another, put it on trial.
func (od *onDemand) onAttributed(a probe.Attributed, now time.Time) {
	p := a.Key.Target
	if a.Key.Cond != od.cond || !od.isMember(p) || probe.IsBuiltin(p) {
		return
	}
	od.noteAttributed(a, now)
	q := od.qualityOf(p)
	od.resolvePending(q, a)
	for exec, at := range q.consumed {
		if now.Sub(at) > probe.LongWindow {
			delete(q.consumed, exec)
		}
	}
	if _, used := q.consumed[a.Exec]; a.Outcome != probe.OutcomeMember || used {
		return
	}
	for _, event := range od.coord.Failures(a.Key) {
		if _, used := q.consumed[event.Exec]; event.Exec == a.Exec || used {
			continue
		}
		// the attributions of two failures may come in either order
		gap := a.Started.Sub(event.At)
		if gap < 0 {
			gap = -gap
		}
		if gap > 0 && gap <= probe.RepeatWindow {
			q.consumed[event.Exec], q.consumed[a.Exec] = event.At, a.Started
			od.giveTrialReason(p, trialRepeat)
			fields := od.event("repeat")
			fields["member"] = od.ref(p)
			fields["probes"] = []uint64{event.Exec, a.Exec}
			od.record(fields)
			return
		}
	}
}

// noteAttributed keeps an attribution a while, for the trials a group replacing this one
// takes over.
func (od *onDemand) noteAttributed(a probe.Attributed, now time.Time) {
	od.attributed[a.Exec] = a
	for exec, seen := range od.attributed {
		if now.Sub(seen.Started) > probe.LongWindow {
			delete(od.attributed, exec)
		}
	}
}

// resolvePending takes the attribution of an anomaly a trial waits for: one of the member
// fails the trial, any other lets it go on.
func (od *onDemand) resolvePending(q *memberQuality, a probe.Attributed) {
	if t := q.trial; t != nil {
		if _, waiting := t.pending[a.Exec]; waiting {
			delete(t.pending, a.Exec)
			if a.Outcome == probe.OutcomeMember {
				t.failed = true
			}
		}
	}
}

// onOutageChanged follows the outages of the pools the members go through.
func (od *onDemand) onOutageChanged(o probe.OutageChanged, now time.Time) {
	if o.Cond != od.cond {
		return
	}
	if o.State == probe.OutageSuspected {
		od.settleTrials(o.Since)
		od.outagePools[o.Pool] = true
		od.settleTrials(o.Since)
		return
	}
	od.settleTrials(o.End)
	delete(od.outagePools, o.Pool)
	od.settleTrials(o.End)
	if o.State == probe.OutageEstablished && o.End.Sub(o.First) >= probe.PersistentAfter {
		for _, p := range od.members {
			if slices.Contains(od.memberPools[p], o.Pool) {
				od.giveTrialReason(p, trialOutage)
			}
		}
	}
}

// trialCompletion takes a test of a member on trial: a success is a witness, a failure an
// anomaly pending its attribution.
func (od *onDemand) trialCompletion(c probe.Completion) {
	q := od.quality[c.Key.Target]
	if q == nil || q.trial == nil || !c.Adopted || c.Started.Before(q.trial.started) {
		return
	}
	if c.Result.Satisfied {
		q.trial.witness = true
	} else if c.Before.State != probe.Dead {
		q.trial.pending[c.Exec] = c.Started
	}
}

// trusted reports whether a standby can be relied on: available, in good standing, with no
// obligation, fresh, and without anomalies lately.
func (od *onDemand) trusted(p C.Proxy, now time.Time) bool {
	if p == nil || probe.IsBuiltin(p) || !od.available(p) || od.standingOf(p) != standingNormal {
		return false
	}
	if ms := od.states[p]; ms != nil && len(ms.signals) > 0 {
		return false
	}
	return od.fresh(p, now) && od.countsOf(p, now).Recent == 0
}

// trustedTarget is how many trusted standbys the pool keeps.
func (od *onDemand) trustedTarget(now time.Time) int {
	if p := od.current; p != nil && !probe.IsBuiltin(p) && (od.onTrial(p) || od.countsOf(p, now).Recent > 0) {
		return probe.TrustedStandbysAt
	}
	return probe.TrustedStandbys
}

// persistentlyDead reports whether a member has been dead long enough not to count as a
// standby candidate.
func (od *onDemand) persistentlyDead(p C.Proxy, now time.Time) bool {
	return od.view[p].State == probe.Dead && od.coord.Persistent(probe.Key{Target: p, Cond: od.cond})
}

// degradation is a degradation of the preferred member a pair of tests found, waiting for its
// attribution.
type degradation struct {
	member   C.Proxy
	exec     uint64 // the test of the preferred member
	started  time.Time
	commitAt time.Time
	// the pools the test went through, as its route tells, and whether the route is known
	pools   []string
	unknown bool
}

// pairRun is a test a group compares with another: of the preferred member verifying it, or
// of the standby tested alongside.
type pairRun struct {
	member  C.Proxy
	exec    uint64
	started time.Time
	elapsed time.Duration
	ok      bool
	done    bool
	standby bool
	paired  bool
	pools   []string
	unknown bool
}

// pairRunFrom is the run of a completed test.
func pairRunFrom(c probe.Completion, standby bool) *pairRun {
	return &pairRun{
		member: c.Key.Target, exec: c.Exec, started: c.Started, elapsed: c.Result.Elapsed, ok: c.Result.Satisfied,
		done: true, standby: standby, pools: c.Pools, unknown: c.UnknownPath,
	}
}

// notePairRun keeps a test to compare and compares it with its counterpart, if any.
func (od *onDemand) notePairRun(run *pairRun, now time.Time) {
	// A test is kept while its counterpart may still end: its time limit and the window.
	keep := od.cond.Timeout
	if keep <= 0 {
		keep = probe.DefaultTestTimeout
	}
	keep = 2*keep + probe.PairWindow + time.Minute
	for exec, other := range od.pairRuns {
		if now.Sub(other.started) > keep {
			delete(od.pairRuns, exec)
		}
	}
	if existing := od.pairRuns[run.exec]; existing != nil {
		if run.done {
			existing.elapsed, existing.ok, existing.done = run.elapsed, run.ok, true
			existing.pools, existing.unknown = run.pools, run.unknown
		}
		run = existing
	} else {
		od.pairRuns[run.exec] = run
	}
	if !run.done || run.paired {
		return
	}
	for _, other := range od.pairRuns {
		if other == run || other.paired || !other.done || other.standby == run.standby {
			continue
		}
		gap := run.started.Sub(other.started)
		if gap < 0 {
			gap = -gap
		}
		if gap > probe.PairWindow {
			continue
		}
		preferred, standby := run, other
		if run.standby {
			preferred, standby = other, run
		}
		preferred.paired, standby.paired = true, true
		od.comparePair(preferred, standby, now)
		return
	}
}

// comparePair records a pair of tests and finds whether the preferred member degraded: both
// succeeded, and it took at least the fast tier and twice as long as the standby.
func (od *onDemand) comparePair(preferred, standby *pairRun, now time.Time) {
	fast := probe.FastProbeFor(od.cond.Timeout)
	valid := preferred.ok && standby.ok
	degraded := valid && preferred.elapsed >= fast && preferred.elapsed >= probe.SlowerRatio*standby.elapsed
	fields := od.event("pair")
	fields["preferred"] = map[string]any{"member": od.ref(preferred.member), "probe": preferred.exec, "started": formatTime(preferred.started), "elapsed": preferred.elapsed.Milliseconds(), "ok": preferred.ok}
	fields["standby"] = map[string]any{"member": od.ref(standby.member), "probe": standby.exec, "started": formatTime(standby.started), "elapsed": standby.elapsed.Milliseconds(), "ok": standby.ok}
	fields["valid"] = valid
	fields["degraded"] = degraded
	od.coord.Records().Append("performance", fields)
	if !degraded {
		return
	}
	for _, d := range od.degradations {
		if d.member == preferred.member && d.exec == preferred.exec {
			return
		}
	}
	// each is attributed on its own; those within DegradeMerge count once
	od.degradations = append(od.degradations, &degradation{
		member: preferred.member, exec: preferred.exec, started: preferred.started,
		commitAt: preferred.started.Add(probe.AttributionDelay), pools: preferred.pools, unknown: preferred.unknown,
	})
	if q := od.quality[preferred.member]; q != nil && q.trial != nil && !preferred.started.Before(q.trial.started) {
		q.trial.pending[preferred.exec] = preferred.started
	}
}

// commitDegradations counts the degradations due, as the coordinator attributes them.
func (od *onDemand) commitDegradations(now time.Time) {
	kept := od.degradations[:0]
	for _, d := range od.degradations {
		if now.Before(d.commitAt) || !od.isMember(d.member) {
			if od.isMember(d.member) {
				kept = append(kept, d)
			}
			continue
		}
		a := od.coord.Attribute(d.started, d.pools, od.cond, d.unknown)
		if a.Wait {
			kept = append(kept, d)
			continue
		}
		q := od.qualityOf(d.member)
		attributed := probe.Attributed{Key: probe.Key{Target: d.member, Cond: od.cond}, Exec: d.exec, Started: d.started, Outcome: a.Outcome}
		od.resolvePending(q, attributed)
		od.noteAttributed(attributed, now)
		fields := od.event("degradation")
		fields["member"] = od.ref(d.member)
		fields["probe"] = d.exec
		fields["started"] = formatTime(d.started)
		fields["outcome"] = a.Outcome
		if a.Outcome == probe.OutcomeMember && now.Sub(d.started) < probe.LongWindow {
			merged := false
			for _, event := range q.degradations {
				if diff := d.started.Sub(event.At); diff > -probe.DegradeMerge && diff < probe.DegradeMerge {
					merged = true
					break
				}
			}
			if merged {
				fields["merged"] = true
			} else {
				q.degradations = append(q.degradations, probe.QualityEvent{At: d.started, Weight: probe.DegradeWeight, Exec: d.exec})
			}
		}
		fields["counts"] = od.countsOf(d.member, now)
		od.coord.Records().Append("performance", fields)
	}
	for i := len(kept); i < len(od.degradations); i++ {
		od.degradations[i] = nil
	}
	od.degradations = kept
	for _, q := range od.quality {
		kept := q.degradations[:0]
		for _, event := range q.degradations {
			if now.Sub(event.At) < probe.LongWindow {
				kept = append(kept, event)
			}
		}
		q.degradations = kept
	}
}

// suspendState is the preferred member set aside for a while: new dials start from the
// member after it until the test it waits for ends.
type suspendState struct {
	member C.Proxy
	since  time.Time
	reason string // "loss" or "slowTest"
	// after is the time the test it waits for must start after; exec is that test when known.
	after time.Time
	exec  uint64
	// the comparison of a slow test
	standby        C.Proxy
	standbyElapsed time.Duration
}

// verifyRun is a test verifying the preferred member that was still running at the fast tier.
type verifyRun struct {
	member  C.Proxy
	exec    uint64
	started time.Time
}

// suspendCandidate is the member after the preferred one new dials start from while it is
// suspended: the first available member after it, before the first built-in member.
func (od *onDemand) suspendCandidate() C.Proxy {
	after := false
	for _, p := range od.order {
		if !after {
			after = p == od.current
			continue
		}
		if probe.IsBuiltin(p) {
			return nil
		}
		// a member ending at a built-in outbound by the selection of the groups along it
		// must not carry the traffic set aside
		if od.available(p) && !od.leafBuiltin[p] {
			return p
		}
	}
	return nil
}

// suspend sets the preferred member aside.
func (od *onDemand) suspend(s *suspendState, now time.Time) {
	if od.suspended != nil || od.network == probe.NetworkDown || od.pinned != "" && od.current != nil && od.current.Name() == od.pinned {
		return
	}
	route := od.suspendCandidate()
	if route == nil {
		return
	}
	od.suspended = s
	fields := od.event("suspend")
	fields["member"] = od.ref(s.member)
	fields["route"] = od.ref(route)
	fields["reason"] = s.reason
	fields["after"] = formatTime(s.after)
	if s.exec != 0 {
		fields["probe"] = s.exec
	}
	if s.standby != nil {
		fields["standby"] = od.ref(s.standby)
		fields["standbyElapsed"] = s.standbyElapsed.Milliseconds()
	}
	od.record(fields)
	od.setRoute(now, selectionCause{Reason: causeSuspect, Exec: s.exec})
}

// endSuspend ends the suspension of the preferred member.
func (od *onDemand) endSuspend(now time.Time, reason string, exec uint64) {
	s := od.suspended
	if s == nil {
		return
	}
	od.suspended = nil
	fields := od.event("suspendEnd")
	fields["member"] = od.ref(s.member)
	fields["reason"] = reason
	if exec != 0 {
		fields["probe"] = exec
	}
	od.record(fields)
	od.setRoute(now, selectionCause{Reason: causeSuspectEnd, Exec: exec})
}

// updateSuspend ends a suspension that no longer holds, and starts one a slow test calls for.
func (od *onDemand) updateSuspend(now time.Time) {
	if s := od.suspended; s != nil {
		ms := od.states[s.member]
		verifying := od.verifyRun != nil && od.verifyRun.member == s.member
		switch {
		case od.current != s.member:
			od.endSuspend(now, "notPreferred", 0)
		case od.suspendCandidate() == nil:
			od.endSuspend(now, "noCandidate", 0)
		case s.exec == 0 && !verifying && (ms == nil || len(ms.signals) == 0):
			// the verification it waited for is no longer asked for: nothing will end it
			od.endSuspend(now, "unsupported", 0)
		default:
			// the route may have to move on past a member found dead meanwhile
			od.setRoute(now, selectionCause{Reason: causeSuspect, Exec: s.exec})
		}
		return
	}
	run := od.verifyRun
	if run == nil || run.member != od.current {
		return
	}
	// The verification of the preferred member still runs past the fast tier, and the standby
	// tested alongside succeeded much faster.
	fast := probe.FastProbeFor(od.cond.Timeout)
	for _, other := range od.pairRuns {
		if !other.standby || !other.done || !other.ok {
			continue
		}
		gap := other.started.Sub(run.started)
		if gap < 0 {
			gap = -gap
		}
		if gap > probe.PairWindow {
			continue
		}
		bound := maxDuration(fast, probe.SlowerRatio*other.elapsed)
		if timeout := od.cond.Timeout; timeout > 0 && bound > timeout {
			return
		}
		if now.Sub(run.started) >= bound {
			od.suspend(&suspendState{
				member: run.member, since: now, reason: "slowTest", after: run.started, exec: run.exec,
				standby: other.member, standbyElapsed: other.elapsed,
			}, now)
		} else {
			od.suspendCheck = run.started.Add(bound)
		}
		return
	}
}

// onRunning takes a verification of the preferred member still running at the fast tier: a
// standby tested alongside that succeeded much faster sets the preferred member aside.
func (od *onDemand) onRunning(r probe.Running, now time.Time) {
	if r.Key.Cond != od.cond || r.Key.Target != od.current {
		return
	}
	for _, request := range r.Requests {
		if request.Group == od.name() && (request.Source == probe.SourceVerify || request.Source == probe.SourceContinue) {
			od.verifyRun = &verifyRun{member: r.Key.Target, exec: r.Exec, started: r.Started}
			od.notePairRun(&pairRun{member: r.Key.Target, exec: r.Exec, started: r.Started}, now)
			return
		}
	}
}

// suspendCompletion ends the suspension a test of the preferred member answers.
func (od *onDemand) suspendCompletion(c probe.Completion, now time.Time) {
	if run := od.verifyRun; run != nil && run.exec == c.Exec {
		od.verifyRun = nil
	}
	s := od.suspended
	if s == nil || c.Key.Target != s.member {
		return
	}
	if (s.exec != 0 && c.Exec == s.exec) || (s.exec == 0 && !c.Started.Before(s.after)) {
		od.endSuspend(now, "answered", c.Exec)
	}
}

// setRoute moves the route: the preferred member, or while it is suspended the member after
// it. A route not known alive lately is verified once it carries traffic.
func (od *onDemand) setRoute(now time.Time, cause selectionCause) {
	route := od.current
	if od.suspended != nil {
		if next := od.suspendCandidate(); next != nil {
			route = next
		}
	}
	if route == od.route {
		return
	}
	prev := od.route
	od.route = route
	od.routeCause = cause
	// the selection of the preferred member records itself; the route moving off it and back
	// is recorded here
	if prev != nil && (route != od.current || cause.Reason == causeSuspectEnd) {
		fields := od.event("selection")
		fields["from"] = od.ref(prev)
		fields["to"] = od.ref(route)
		fields["preferred"] = od.ref(od.current)
		fields["cause"] = cause
		fields["display"] = od.selectionDisplay(prev, route, cause)
		od.record(fields)
	}
	if route != nil && route != od.current && !probe.IsBuiltin(route) && !od.fresh(route, now) {
		od.continueMember, od.continueAt = route, now
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// standbyFor is the member tested alongside the verification of the preferred member: the
// first available member after it, before the first built-in member.
func (od *onDemand) standbyFor() C.Proxy {
	return od.suspendCandidate()
}

// dynamicPool returns the standbys: the available members after the preferred one, in the
// effective order, up to the trusted standbys it needs, at least PoolSize and at most PoolMax
// of them. limited is set when it holds PoolMax and still falls short.
func (od *onDemand) dynamicPool(refill []C.Proxy, now time.Time) (pool []C.Proxy, trustedCount, target int, limited bool) {
	target = od.trustedTarget(now)
	for _, p := range refill {
		if !od.available(p) {
			continue
		}
		pool = append(pool, p)
		if od.trusted(p, now) {
			trustedCount++
		}
		if trustedCount >= target && len(pool) >= probe.PoolSize {
			return pool, trustedCount, target, false
		}
		if len(pool) == probe.PoolMax {
			return pool, trustedCount, target, trustedCount < target
		}
	}
	return pool, trustedCount, target, false
}
