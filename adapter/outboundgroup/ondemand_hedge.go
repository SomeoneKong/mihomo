package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
)

// dialBudget caps the attempts in flight across the groups one dial goes through: the first
// attempt is the dial itself, each other one a group starts takes a share.
type dialBudget struct {
	inFlight atomic.Int32
}

type budgetKey struct{}

func withBudget(ctx context.Context) (context.Context, *dialBudget) {
	if b, ok := ctx.Value(budgetKey{}).(*dialBudget); ok {
		return ctx, b
	}
	b := &dialBudget{}
	b.inFlight.Store(1)
	return context.WithValue(ctx, budgetKey{}, b), b
}

func (b *dialBudget) acquire() bool {
	for {
		v := b.inFlight.Load()
		if v >= probe.DialBudget {
			return false
		}
		if b.inFlight.CompareAndSwap(v, v+1) {
			return true
		}
	}
}

func (b *dialBudget) release() {
	b.inFlight.Add(-1)
}

// dialPlan is what a dial of the group starts from: the route, and the candidates it may
// call on, as the members are now.
type dialPlan struct {
	preferred  C.Proxy
	route      C.Proxy
	candidates []C.Proxy
	certain    bool
	// eligible checks a candidate again when it is about to be dialed.
	eligible func(p C.Proxy) bool
}

// leafBuiltin reports whether a member ends up at a built-in outbound through the selection of
// the groups along it: a dial must not be moved to a direct or reject outbound.
func leafBuiltin(p C.Proxy, metadata *C.Metadata) bool {
	for i := 0; i < 16 && p != nil; i++ {
		if probe.IsBuiltin(p) {
			return true
		}
		if _, ok := p.Adapter().(ProxyGroup); !ok {
			return false
		}
		if metadata == nil && p.Type() == C.LoadBalance {
			return false // each connection picks its own member
		}
		next := p.Unwrap(metadata, false)
		if next == p {
			return false
		}
		p = next
	}
	return false
}

// detached keeps the values of a context but not its cancellation: the route of a dial goes
// on after another attempt carried it, until a loss can be decided, while the caller cancels
// the context of the dial once it returns.
type detached struct{ parent context.Context }

func (d detached) Deadline() (time.Time, bool) { return time.Time{}, false }
func (d detached) Done() <-chan struct{}       { return nil }
func (d detached) Err() error                  { return nil }
func (d detached) Value(key any) any           { return d.parent.Value(key) }

// raceMark tells the attempts along a dial whether a group above them dropped the branch
// they belong to: a route going on after its group carried the dial stops once a group above
// dropped it, as it no longer serves the dial.
type raceMark struct {
	lost   atomic.Bool
	parent *raceMark
}

type raceKey struct{}

func markOf(ctx context.Context) *raceMark {
	m, _ := ctx.Value(raceKey{}).(*raceMark)
	return m
}

func (m *raceMark) eliminated() bool {
	for ; m != nil; m = m.parent {
		if m.lost.Load() {
			return true
		}
	}
	return false
}

// attempt is one member a dial tries.
type attempt struct {
	member   C.Proxy
	route    bool
	started  time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	mark     *raceMark
	budgeted bool // it holds a share of the budget, until it ends without a connection in use

	done    bool
	conn    C.Conn
	err     error
	elapsed time.Duration
	// why it ended without a connection: "error", "timeout", "race" or "canceled"
	ending string
}

type hedgeResult struct {
	conn    C.Conn
	err     error
	carrier *attempt
}

// hedge is one dial with the fast tier: the route alone while it is certain and quick, a
// candidate alongside once it is not, delivered by the wall clock past the fast tier.
type hedge struct {
	od       *onDemand
	ctx      context.Context
	metadata *C.Metadata
	plan     dialPlan
	budget   *dialBudget
	mode     string

	results   chan *attempt
	deliver   chan hedgeResult
	attempts  []*attempt
	next      int // the next candidate of the plan
	skipped   []any
	inFlight  int
	pastFast  bool
	delivered *attempt
	failed    bool // the dial ended without a connection
	routeLost bool
	lossAt    time.Time
	lossKnown bool
	started   time.Time
	fastAt    time.Time // the end of the fast tier
	deadline  time.Time // of the dial, zero for none
}

// endedAt is when an attempt that came back ended.
func (a *attempt) endedAt() time.Time {
	return a.started.Add(a.elapsed)
}

// undecided reports whether the dial runs out of time before the route can be compared with
// the candidate that carried it: the route is then neither lost nor timed out.
func (h *hedge) undecided() bool {
	return !h.deadline.IsZero() && h.deadline.Before(h.lossAt)
}

// hedgedDial dials through the route, calling on the candidates as the fast tier allows.
func (od *onDemand) hedgedDial(ctx context.Context, metadata *C.Metadata, plan dialPlan) (C.Conn, error) {
	ctx, budget := withBudget(ctx)
	h := &hedge{
		od: od, ctx: ctx, metadata: metadata, plan: plan, budget: budget,
		results: make(chan *attempt, probe.HedgeAttempts), deliver: make(chan hedgeResult, 1),
		started: time.Now(), mode: "fastTier",
	}
	h.fastAt = h.started.Add(probe.FastDial)
	if deadline, ok := ctx.Deadline(); ok {
		h.deadline = deadline
	}
	if !plan.certain {
		h.mode = "parallel"
	}
	go h.run()
	r := <-h.deliver
	if r.err != nil {
		return nil, r.err
	}
	return od.wrap(r.conn, r.carrier.member, r.carrier.started, !r.carrier.route), nil
}

func (h *hedge) launch(p C.Proxy, route bool, budgeted bool) {
	// The attempts keep the deadline of the dial, and are canceled by the hedge.
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := h.ctx.Deadline(); ok {
		ctx, cancel = context.WithDeadline(detached{h.ctx}, deadline)
	} else {
		ctx, cancel = context.WithCancel(detached{h.ctx})
	}
	mark := &raceMark{parent: markOf(h.ctx)}
	ctx = context.WithValue(ctx, raceKey{}, mark)
	a := &attempt{member: p, route: route, started: time.Now(), ctx: ctx, cancel: cancel, mark: mark, budgeted: budgeted}
	h.attempts = append(h.attempts, a)
	h.inFlight++
	go func() {
		c, err := p.DialContext(ctx, h.metadata)
		a.conn, a.err, a.elapsed = c, err, time.Since(a.started)
		h.results <- a
	}()
}

// launchNext starts the next candidate the attempts, the attempts in flight and the budget
// allow; it reports whether it started one.
func (h *hedge) launchNext() bool {
	// nothing new once the dial is canceled or out of time
	if len(h.attempts) >= probe.HedgeAttempts || h.inFlight >= probe.HedgeInFlight || h.delivered != nil || h.failed || h.ctx.Err() != nil {
		return false
	}
	for h.next < len(h.plan.candidates) {
		p := h.plan.candidates[h.next]
		h.next++
		if !h.plan.eligible(p) {
			h.skipped = append(h.skipped, map[string]any{"member": h.od.ref(p), "reason": "ineligible"})
			continue
		}
		if leafBuiltin(p, h.metadata) {
			h.skipped = append(h.skipped, map[string]any{"member": h.od.ref(p), "reason": "builtin"})
			continue
		}
		if !h.budget.acquire() {
			h.skipped = append(h.skipped, map[string]any{"member": h.od.ref(p), "reason": "budget"})
			h.next = len(h.plan.candidates)
			return false
		}
		h.launch(p, false, true)
		return true
	}
	return false
}

func (h *hedge) route() *attempt {
	return h.attempts[0]
}

func (h *hedge) routeDone() bool {
	return h.route().done
}

func (h *hedge) run() {
	h.launch(h.plan.route, true, false)
	if !h.plan.certain {
		h.launchNext()
	}
	fast := time.NewTimer(probe.FastDial)
	defer fast.Stop()
	fastC := fast.C
	var lossC <-chan time.Time
	var loss *time.Timer
	defer func() {
		if loss != nil {
			loss.Stop()
		}
	}()
	doneC := h.ctx.Done()
	for !h.finished() {
		select {
		case a := <-h.results:
			h.drain(a)
		case <-doneC:
			doneC = nil
			switch {
			case h.delivered == nil && !h.failed && errors.Is(h.ctx.Err(), context.Canceled):
				// Canceled by the caller, or dropped by a group above, before any attempt
				// carried the dial: the dial ends with it. Past the deadline the attempts end
				// by their own.
				h.abandon("canceled")
			case h.delivered != nil && markOf(h.ctx).eliminated():
				// a group above dropped the branch this dial carried: what goes on of it serves
				// nothing, and decides no loss
				if r := h.route(); !r.done {
					h.lossKnown = true
					h.drop(r)
				}
			}
		case <-fastC:
			h.drain(nil)
			fastC = nil
			h.pastFast = true
			// a candidate that succeeded carries the dial; otherwise another is called on
			if h.delivered == nil && !h.failed && !h.deliverHeld() && !h.routeDone() {
				h.launchNext()
			}
		case <-lossC:
			h.drain(nil)
			lossC = nil
			if r := h.route(); !r.done && !h.lossKnown {
				h.decideLoss(r)
			}
		}
		if h.delivered != nil && !h.delivered.route && !h.lossKnown && loss == nil {
			if wait := time.Until(h.lossAt); wait > 0 {
				loss = time.NewTimer(wait)
				lossC = loss.C
			} else if r := h.route(); !r.done {
				h.decideLoss(r)
			}
		}
	}
	h.finish()
}

// drain handles first and the other results ready now in the order the attempts ended, so
// that what a timer decides does not depend on whether it is seen before them.
func (h *hedge) drain(first *attempt) {
	var batch []*attempt
	if first != nil {
		batch = append(batch, first)
	}
	for ready := true; ready; {
		select {
		case a := <-h.results:
			batch = append(batch, a)
		default:
			ready = false
		}
	}
	sort.SliceStable(batch, func(i, j int) bool { return batch[i].endedAt().Before(batch[j].endedAt()) })
	for _, a := range batch {
		h.onResult(a)
	}
}

// decideLoss ends the route still running past the time to compare it: it lost, unless the
// dial ran out of time before that.
func (h *hedge) decideLoss(r *attempt) {
	h.lossKnown = true
	if h.undecided() {
		r.ending = "undecided"
		r.mark.lost.Store(true)
		r.cancel()
		return
	}
	h.routeLost = true
	h.drop(r)
}

// canceled reports whether the caller canceled the dial, or a group above dropped it, before
// any attempt carried it: whatever is ready besides, nothing is delivered.
func (h *hedge) canceled() bool {
	return h.delivered == nil && !h.failed && errors.Is(h.ctx.Err(), context.Canceled)
}

// drop cancels an attempt the dial no longer needs, and the attempts of the groups below it.
func (h *hedge) drop(a *attempt) {
	a.ending = "race"
	a.mark.lost.Store(true)
	a.cancel()
}

// release returns the share of the budget an attempt holds.
func (h *hedge) release(a *attempt) {
	if a.budgeted {
		a.budgeted = false
		h.budget.release()
	}
}

// abandon ends a dial no attempt carried: the attempts in flight are canceled, the
// connections waiting to be delivered closed, and the caller gets the error of its context.
func (h *hedge) abandon(ending string) {
	for _, a := range h.attempts {
		switch {
		case !a.done:
			a.ending = ending
			a.mark.lost.Store(true)
			a.cancel()
		case a.conn != nil:
			_ = a.conn.Close()
			a.conn = nil
			a.ending = ending
			h.release(a)
		}
	}
	h.failed = true
	h.deliver <- hedgeResult{err: fmt.Errorf("%s: %w", h.od.name(), h.ctx.Err())}
}

// finished reports whether every attempt ended and nothing is left to decide.
func (h *hedge) finished() bool {
	return h.inFlight == 0 && (h.delivered != nil || h.failed)
}

func (h *hedge) onResult(a *attempt) {
	a.done = true
	h.inFlight--
	if a.err == nil {
		h.onSuccess(a)
		return
	}
	h.release(a)
	if a.conn != nil {
		_ = a.conn.Close()
		a.conn = nil
	}
	if a.ending == "" {
		if errors.Is(a.ctx.Err(), context.DeadlineExceeded) {
			a.ending = "timeout"
		} else {
			a.ending = "error"
		}
	}
	if a.route && h.delivered != nil && !h.delivered.route && !h.lossKnown {
		if a.ending == "timeout" && h.undecided() {
			// the dial ran out of time before the route could be compared: it decides nothing
			a.ending = "undecided"
		}
		// the route failed after a candidate carried the dial: it lost
		h.routeLost, h.lossKnown = a.ending == "error" || a.ending == "timeout", true
	}
	if h.delivered != nil || h.failed {
		return
	}
	if h.canceled() {
		h.abandon("canceled")
		return
	}
	if a.route {
		if h.deliverHeld() {
			return
		}
	}
	if a.ending == "error" || a.ending == "timeout" {
		h.launchNext()
	}
	if h.delivered == nil && h.inFlight == 0 && !h.anyHeld() {
		h.failed = true
		h.deliver <- hedgeResult{err: h.failure()}
	}
}

func (h *hedge) onSuccess(a *attempt) {
	if h.delivered != nil || h.failed {
		// late: the dial was carried by another, or ended without one
		if a.route && h.delivered != nil && !h.delivered.route && !h.lossKnown {
			h.lossKnown = true
			if !a.endedAt().Before(h.lossAt) {
				if h.undecided() {
					a.ending = "undecided"
				} else {
					h.routeLost = true // it came, but too late
				}
			}
		}
		_ = a.conn.Close()
		a.conn = nil
		if a.ending == "" {
			a.ending = "late"
		}
		h.release(a)
		return
	}
	if a.route {
		// past the fast tier a candidate that succeeded earlier carries the dial, as it would
		// have once the fast tier came
		if a.endedAt().After(h.fastAt) && h.deliverHeld() {
			return
		}
		h.deliverAttempt(a)
		return
	}
	if h.pastFast || !a.endedAt().Before(h.fastAt) || h.route().done && h.route().err != nil {
		h.deliverAttempt(a)
		return
	}
	// a candidate before the fast tier: the route is preferred until then
}

// anyHeld reports whether a candidate succeeded and waits to be delivered.
func (h *hedge) anyHeld() bool {
	for _, a := range h.attempts {
		if a.done && a.conn != nil && !a.route {
			return true
		}
	}
	return false
}

// deliverHeld delivers the first candidate that succeeded, if any.
func (h *hedge) deliverHeld() bool {
	var first *attempt
	for _, a := range h.attempts {
		if a.done && a.conn != nil && !a.route && (first == nil || a.started.Add(a.elapsed).Before(first.started.Add(first.elapsed))) {
			first = a
		}
	}
	if first == nil {
		return false
	}
	h.deliverAttempt(first)
	return true
}

func (h *hedge) deliverAttempt(a *attempt) {
	if h.canceled() {
		h.abandon("canceled")
		return
	}
	h.delivered = a
	for _, other := range h.attempts {
		if other == a {
			continue
		}
		if other.done {
			if other.conn != nil {
				_ = other.conn.Close()
				other.conn = nil
				other.ending = "race"
				h.release(other)
			}
			continue
		}
		if other.route && !a.route {
			continue // it goes on until a loss can be decided
		}
		h.drop(other)
	}
	if !a.route {
		r := h.route()
		if r.done {
			h.lossAt = r.started.Add(maxDuration(probe.FastDial, probe.SlowerRatio*a.elapsed))
			switch {
			case r.ending == "timeout" && h.undecided():
				// the dial ran out of time before the route could be compared
				r.ending = "undecided"
			case r.ending == "error" || r.ending == "timeout":
				h.routeLost = true
			case r.err == nil:
				// it succeeded, later than the candidate allows or not
				h.routeLost = !r.endedAt().Before(h.lossAt)
			}
			h.lossKnown = true
			h.release(a)
		} else {
			// the share of the candidate now stands for the route going on beside it
			a.budgeted, r.budgeted = false, a.budgeted || r.budgeted
			// past the deadline of the dial the route ends undecided, not lost
			h.lossAt = r.started.Add(maxDuration(probe.FastDial, probe.SlowerRatio*a.elapsed))
		}
	}
	h.deliver <- hedgeResult{conn: a.conn, carrier: a}
}

// failure is the error of a dial no attempt carried: the route's, or the last candidate's when
// the route's is not known.
func (h *hedge) failure() error {
	r := h.route()
	err := r.err
	if err == nil {
		for _, a := range h.attempts {
			if a.err != nil {
				err = a.err
			}
		}
	}
	if err == nil {
		err = errNoAvailable
	}
	return fmt.Errorf("%s: %w", h.od.name(), err)
}

// finish records the dial, and tells the group what it found: the failures of its attempts, a
// route that lost to a candidate, and the counts of the minute.
func (h *hedge) finish() {
	od := h.od
	now := time.Now()
	for _, a := range h.attempts {
		if a.ending == "error" || a.ending == "timeout" {
			class := "dial"
			if a.ending == "timeout" {
				class = "timeout"
			}
			od.dialFailedAs(a.member, a.err, a.started, "tcp", class, !a.route)
		}
	}
	for _, a := range h.attempts {
		if a != h.delivered {
			a.cancel()
		}
		h.release(a)
	}
	slow := h.routeLost && h.route().ending != "error" && h.route().ending != "timeout"
	if slow {
		r := h.route()
		od.post(signalNote{
			member: r.member, cond: od.cond, network: "tcp", strong: true, class: "slow",
			err: fmt.Sprintf("lost to %s", h.delivered.member.Name()), at: now, dialed: r.started,
			age: now.Sub(r.started),
		})
	}
	od.post(hedgeNote{h: h, slow: slow})
}

// fields of the record of a dial with its attempts.
func (h *hedge) fields() map[string]any {
	od := h.od
	var attempts []any
	for _, a := range h.attempts {
		entry := map[string]any{
			"member":  od.ref(a.member),
			"route":   a.route,
			"started": a.started.Sub(h.started).Milliseconds(),
			"elapsed": a.elapsed.Milliseconds(),
			"ok":      a.err == nil,
		}
		if a.ending != "" {
			entry["ending"] = a.ending
		}
		if a.err != nil && a.ending != "race" && a.ending != "canceled" {
			entry["error"] = shortError(a.err)
		}
		attempts = append(attempts, entry)
	}
	fields := od.event("hedge")
	fields["mode"] = h.mode
	fields["certain"] = h.plan.certain
	fields["attempts"] = attempts
	if len(h.skipped) > 0 {
		fields["skipped"] = h.skipped
	}
	if h.delivered != nil {
		fields["carrier"] = od.ref(h.delivered.member)
	}
	fields["lost"] = h.routeLost
	if h.routeLost && !h.lossAt.IsZero() {
		fields["lossBound"] = h.lossAt.Sub(h.route().started).Milliseconds()
	}
	return fields
}

// hedgeNote tells the group of a dial with the fast tier once it ended.
type hedgeNote struct {
	h    *hedge
	slow bool
}

// dialNote counts a dial without the fast tier in the summary of its minute.
type dialNote struct {
	member   C.Proxy
	dialed   time.Time
	ok       bool
	canceled bool // by the caller: no failure of the member
}

// onHedge records a dial with its attempts and counts them in the summaries.
func (od *onDemand) onHedge(n hedgeNote, now time.Time) {
	h := n.h
	always := n.slow
	od.coord.Records().Limited(od.name(), always, "group", "hedge", h.fields())
	for _, a := range h.attempts {
		s := od.summaryOf(a.member, a.started)
		if s == nil {
			continue
		}
		s.dialed++
		switch {
		case a == h.delivered:
			s.delivered++
		case a.ending == "race" || a.ending == "late":
			s.canceled++
		case a.ending == "error" || a.ending == "timeout":
			s.dialFailed++
		case a.ending == "canceled":
			s.canceled++
		case a.ending == "undecided":
			s.undecided++
		}
	}
}

func (od *onDemand) onDial(n dialNote, now time.Time) {
	s := od.summaryOf(n.member, n.dialed)
	if s == nil {
		return
	}
	s.dialed++
	switch {
	case n.ok:
		s.delivered++
	case n.canceled:
		s.canceled++
	default:
		s.dialFailed++
	}
}

// summaryKey is a member and the minute of the dials a summary counts.
type summaryKey struct {
	member C.Proxy
	minute int64
}

// summary counts the new connections of a member dialed within a minute. undecided are the
// routes the dial ran out of time for after another attempt carried it.
type summary struct {
	dialed, delivered, firstData, dialFailed, canceled, undecided int
	failed                                                        map[string]int
	ref                                                           any // the member as it was then
}

func (od *onDemand) summaryOf(member C.Proxy, dialed time.Time) *summary {
	if member == nil || probe.IsBuiltin(member) || !od.isMember(member) {
		return nil
	}
	minute := dialed.Unix() / 60
	if minute <= od.summarized {
		return nil // already summed up: counted as without a result
	}
	key := summaryKey{member: member, minute: minute}
	s := od.summaries[key]
	if s == nil {
		s = &summary{failed: map[string]int{}, ref: od.ref(member)}
		od.summaries[key] = s
	}
	return s
}

// failurePhase is the phase of a connection a signal class tells.
func failurePhase(class string) string {
	switch class {
	case "handshake":
		return "handshake"
	case "read", "eof":
		return "beforeData"
	case "silent":
		return "silent"
	}
	return ""
}

// flushSummaries records the summaries of the minutes ended SummaryDelay ago, or all of them.
func (od *onDemand) flushSummaries(now time.Time, all bool) {
	last := now.Add(-probe.SummaryDelay).Unix()/60 - 1
	if all {
		last = now.Unix() / 60
	}
	if last <= od.summarized {
		return
	}
	var keys []summaryKey
	for key := range od.summaries {
		if key.minute <= last {
			keys = append(keys, key)
		}
	}
	od.recordSummaries(keys)
	od.summarized = last
}

// flushLeaving records the summaries of the members the group replacing this one does not
// have, which it cannot take over.
func (od *onDemand) flushLeaving(successor *onDemand) {
	// the members as the activation published them: the successor's own are its lock's
	members := *successor.memberSet.Load()
	var keys []summaryKey
	for key := range od.summaries {
		if _, kept := members[key.member]; !kept {
			keys = append(keys, key)
		}
	}
	od.recordSummaries(keys)
}

// recordSummaries records the summaries of keys, in order, and forgets them.
func (od *onDemand) recordSummaries(keys []summaryKey) {
	sortSummaryKeys(od, keys)
	for _, key := range keys {
		s := od.summaries[key]
		delete(od.summaries, key)
		failedAfter := 0
		for _, count := range s.failed {
			failedAfter += count
		}
		incomplete := s.delivered - s.firstData - failedAfter
		if incomplete < 0 {
			incomplete = 0
		}
		fields := od.event("connections")
		fields["member"] = s.ref
		if s.ref == nil {
			fields["member"] = od.ref(key.member)
		}
		fields["minute"] = formatTime(time.Unix(key.minute*60, 0))
		fields["dialed"] = s.dialed
		fields["delivered"] = s.delivered
		fields["firstData"] = s.firstData
		fields["dialFailed"] = s.dialFailed
		fields["canceled"] = s.canceled
		fields["undecided"] = s.undecided
		fields["failed"] = s.failed
		fields["incomplete"] = incomplete
		od.record(fields)
	}
}

func sortSummaryKeys(od *onDemand, keys []summaryKey) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && lessSummary(od, keys[j], keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

func lessSummary(od *onDemand, a, b summaryKey) bool {
	if a.minute != b.minute {
		return a.minute < b.minute
	}
	return od.coord.KnownObjectID(a.member) < od.coord.KnownObjectID(b.member)
}
