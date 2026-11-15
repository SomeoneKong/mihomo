package probe

import (
	"sort"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// QualityEvent is a failure counted in the memory of a member or a traffic pool, at the time
// the test that found it started.
type QualityEvent struct {
	At     time.Time
	Weight float64
	Exec   uint64
}

// poolKey identifies a traffic pool under a test condition.
type poolKey struct {
	pool string
	cond Condition
}

// pendingFailure is a failure waiting to be attributed: the facts of its test are kept for
// that, as the test may have left the ring of records by then.
type pendingFailure struct {
	key      Key
	exec     uint64
	started  time.Time
	pools    []string
	unknown  bool // the route of the test is not known
	commitAt time.Time
	object   uint64 // the object number of the target when it failed
	// reconfirmed is set on a failure of a proxy dead already, found once the local network
	// seemed normal again after the failure before
	reconfirmed bool
}

// quality is the memory of the failures the tests found, shared by the groups.
type quality struct {
	failures map[Key][]QualityEvent
	pools    map[poolKey][]QualityEvent
	pending  []*pendingFailure
	timer    *time.Timer
	timerAt  time.Time
}

func newQuality() *quality {
	return &quality{failures: map[Key][]QualityEvent{}, pools: map[poolKey][]QualityEvent{}}
}

// Counts are the counts of the memory within its two windows.
type Counts struct {
	Recent float64 `json:"recent"`
	Long   float64 `json:"long"`
}

// Add returns the sum of two counts.
func (c Counts) Add(o Counts) Counts {
	return Counts{Recent: c.Recent + o.Recent, Long: c.Long + o.Long}
}

// Max returns the greater of two counts in each window.
func (c Counts) Max(o Counts) Counts {
	if o.Recent > c.Recent {
		c.Recent = o.Recent
	}
	if o.Long > c.Long {
		c.Long = o.Long
	}
	return c
}

// CountEvents counts events within the two windows ending at now.
func CountEvents(events []QualityEvent, now time.Time) Counts {
	var counts Counts
	for _, event := range events {
		age := now.Sub(event.At)
		if age < LongWindow {
			counts.Long += event.Weight
		}
		if age < RecentWindow {
			counts.Recent += event.Weight
		}
	}
	return counts
}

// clearAfter clears what a slice compacted in place no longer holds, so that it keeps nothing
// alive.
func clearAfter[T any](s []T, n int) {
	var zero T
	for i := n; i < len(s); i++ {
		s[i] = zero
	}
}

func pruneEvents(events []QualityEvent, now time.Time) []QualityEvent {
	kept := events[:0]
	for _, event := range events {
		if now.Sub(event.At) < LongWindow {
			kept = append(kept, event)
		}
	}
	return kept
}

// Failures returns the failures counted for a member.
func (c *Coordinator) Failures(key Key) []QualityEvent {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	events := pruneEvents(c.quality.failures[key], c.now())
	c.quality.failures[key] = events
	return append([]QualityEvent(nil), events...)
}

// MemberCounts counts the failures counted for a member.
func (c *Coordinator) MemberCounts(key Key) Counts {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return CountEvents(c.quality.failures[key], c.now())
}

// PoolEvents lists the outages counted for a traffic pool.
func (c *Coordinator) PoolEvents(pool string, cond Condition) []QualityEvent {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	key := poolKey{pool, cond}
	events := pruneEvents(c.quality.pools[key], c.now())
	c.quality.pools[key] = events
	return append([]QualityEvent(nil), events...)
}

// PoolCounts counts the outages counted for a traffic pool.
func (c *Coordinator) PoolCounts(pool string, cond Condition) Counts {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return CountEvents(c.quality.pools[poolKey{pool, cond}], c.now())
}

// poolsOfRoute lists the traffic pools a test of target went through: the pool of the target
// and those of the nodes along the dialer route the test recorded. unknown is set when the
// route of a target dialing through another is not known.
func (c *Coordinator) poolsOfRoute(target C.Proxy, result C.ProbeResult, route [][]string) (pools []string, unknown bool) {
	add := func(pool string) {
		if pool != "" && !containsString(pools, pool) {
			pools = append(pools, pool)
		}
	}
	if _, isGroup := target.Adapter().(groupView); isGroup {
		if result.Outbound == nil {
			return nil, true
		}
		if p := c.lookupNode(result.Outbound.Name()); p != nil {
			add(poolOf(p))
		} else {
			unknown = true
		}
	} else {
		add(poolOf(target))
	}
	if target.ProxyInfo().DialerProxy != "" && len(route) == 0 {
		return pools, true
	}
	for _, chain := range route {
		if len(chain) == 0 || chain[0] == "" {
			unknown = true
			continue
		}
		if p := c.lookupNode(chain[0]); p != nil {
			add(poolOf(p))
		} else {
			unknown = true // gone since: what it went through is not known
		}
	}
	return pools, unknown
}

func poolOf(p C.Proxy) string {
	if owner, ok := p.(C.TrafficPoolOwner); ok {
		return owner.TrafficPool()
	}
	return ""
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// addPendingLocked keeps a failure a test found until it can be attributed.
func (c *Coordinator) addPendingLocked(p *pendingFailure) {
	c.quality.pending = append(c.quality.pending, p)
	c.armQualityLocked()
}

// armQualityLocked wakes the attribution when the earliest pending failure is due.
func (c *Coordinator) armQualityLocked() {
	// Failures due that wait for an outage are attributed once it is, not by a timer of theirs;
	// an established outage is attributed when it is due.
	var next time.Time
	now := c.now()
	for _, p := range c.quality.pending {
		if p.commitAt.After(now) && (next.IsZero() || p.commitAt.Before(next)) {
			next = p.commitAt
		}
	}
	for _, o := range c.outages.past {
		if o.state == OutageEstablished && !o.committed && o.commitAt.After(now) && (next.IsZero() || o.commitAt.Before(next)) {
			next = o.commitAt
		}
	}
	q := c.quality
	if next.IsZero() {
		if q.timer != nil {
			q.timer.Stop()
			q.timer, q.timerAt = nil, time.Time{}
		}
		return
	}
	if !q.timerAt.IsZero() && !next.Before(q.timerAt) && q.timer != nil {
		return
	}
	if q.timer != nil {
		q.timer.Stop()
	}
	q.timerAt = next
	q.timer = time.AfterFunc(next.Sub(c.now()), func() {
		c.mutex.Lock()
		q.timer, q.timerAt = nil, time.Time{}
		c.attributeDueLocked()
		c.mutex.Unlock()
		c.kick()
	})
}

// Attribution tells how a failure found at a time along some pools counts.
type Attribution struct {
	Outcome string
	Pool    string
	Wait    bool // it depends on an outage not attributed yet
}

// attributionLocked decides how a failure counts, as things are now. An established outage
// that did not count against its pool explains nothing: the other pools along the way may.
func (c *Coordinator) attributionLocked(started time.Time, pools []string, cond Condition, unknown bool) Attribution {
	if c.network.covers(started) {
		return Attribution{Outcome: OutcomeLocal}
	}
	if unknown {
		return Attribution{Outcome: OutcomeUnknown}
	}
	// a pool along the route whose outage counted explains it, whatever the others are waiting
	// for
	for _, pool := range pools {
		if o := c.outageCovering(poolKey{pool, cond}, started); o != nil && o.state == OutageEstablished && o.counted {
			return Attribution{Outcome: OutcomePool, Pool: pool}
		}
	}
	// then one that may still come to count it is waited for, before one that cannot tell
	var undecided string
	for _, pool := range pools {
		o := c.outageCovering(poolKey{pool, cond}, started)
		if o == nil {
			continue
		}
		switch {
		case o.state == OutageSuspected, o.state == OutageEstablished && !o.committed:
			return Attribution{Wait: true, Pool: pool}
		case o.state == OutageUndecided && undecided == "":
			undecided = pool
		}
	}
	if undecided != "" {
		return Attribution{Outcome: OutcomeUndecided, Pool: undecided}
	}
	return Attribution{Outcome: OutcomeMember}
}

// Attribute tells a group how a degradation it found at a time along some pools counts:
// wait while an outage it depends on is not attributed.
func (c *Coordinator) Attribute(started time.Time, pools []string, cond Condition, unknown bool) Attribution {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.expireNetworkLocked()
	c.commitOutagesLocked()
	return c.attributionLocked(started, pools, cond, unknown)
}

// FailureRoute tells of a failure of key waiting to be attributed, found by its test exec, when
// the test started and the pools along it.
func (c *Coordinator) FailureRoute(key Key, exec uint64) (started time.Time, pools []string, unknown bool, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, p := range c.quality.pending {
		if p.exec == exec && p.key == key {
			return p.started, p.pools, p.unknown, true
		}
	}
	return time.Time{}, nil, false, false
}

// Waiting tells what an anomaly that started then along some pools waits for, as attribution
// decides it now: the pool whose outage it depends on, and, once that outage is established,
// when the outage is to be attributed.
func (c *Coordinator) Waiting(started time.Time, pools []string, cond Condition, unknown bool) (pool string, at time.Time, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.expireNetworkLocked()
	c.commitOutagesLocked()
	a := c.attributionLocked(started, pools, cond, unknown)
	if !a.Wait {
		return "", time.Time{}, false
	}
	if o := c.outageCovering(poolKey{a.Pool, cond}, started); o != nil && o.state == OutageEstablished {
		at = o.commitAt
	}
	return a.Pool, at, true
}

// PoolsOf lists the traffic pools a proxy goes through now: its own, and those along its
// dialer proxy and the selection of the groups on the way, as the exclusions are evaluated.
func (c *Coordinator) PoolsOf(p C.Proxy) []string {
	var pools []string
	seen := map[C.Proxy]bool{}
	var walk func(p C.Proxy)
	walk = func(p C.Proxy) {
		if p == nil || seen[p] || IsBuiltin(p) {
			return
		}
		seen[p] = true
		if group, ok := p.Adapter().(groupView); ok {
			if p.Type() == C.LoadBalance {
				// each connection picks its own member
				for _, member := range group.Proxies() {
					walk(member)
				}
				return
			}
			walk(p.Unwrap(nil, false))
			return
		}
		if pool := poolOf(p); pool != "" && !containsString(pools, pool) {
			pools = append(pools, pool)
		}
		if dialer := p.ProxyInfo().DialerProxy; dialer != "" {
			walk(c.Lookup(dialer))
		}
	}
	walk(p)
	return pools
}

// AttributeDue attributes the failures that are due now. The coordinator does so by itself when
// the earliest of them is due.
func (c *Coordinator) AttributeDue() {
	c.mutex.Lock()
	c.attributeDueLocked()
	c.mutex.Unlock()
	c.kick()
}

// attributeDueLocked attributes the pending failures that are due and do not wait for an
// outage to be concluded, those that came due while the others were attributed included.
func (c *Coordinator) attributeDueLocked() {
	for {
		next := c.attributeDueOnceLocked()
		if next.IsZero() || next.After(c.now()) {
			break
		}
	}
	c.armQualityLocked()
}

// attributeDueOnceLocked attributes what is due as of now, and returns when the earliest
// failure it found not due yet is.
func (c *Coordinator) attributeDueOnceLocked() (next time.Time) {
	c.expireNetworkLocked()
	c.commitOutagesLocked()
	now := c.now()
	kept := c.quality.pending[:0]
	for _, p := range c.quality.pending {
		if now.Before(p.commitAt) {
			kept = append(kept, p)
			if next.IsZero() || p.commitAt.Before(next) {
				next = p.commitAt
			}
			continue
		}
		if !c.referencedLocked(p.key) {
			c.recordAttributionLocked(p, Attribution{Outcome: OutcomeUndecided}, "removed")
			c.deliverAttributedLocked(p, Attribution{Outcome: OutcomeUndecided})
			continue
		}
		a := c.attributionLocked(p.started, p.pools, p.key.Cond, p.unknown)
		if a.Wait {
			kept = append(kept, p)
			continue
		}
		if a.Outcome == OutcomeMember {
			if now.Sub(p.started) >= LongWindow {
				a.Outcome = OutcomeExpired
			} else {
				events := pruneEvents(c.quality.failures[p.key], now)
				c.quality.failures[p.key] = append(events, QualityEvent{At: p.started, Weight: 1, Exec: p.exec})
				sort.Slice(c.quality.failures[p.key], func(i, j int) bool {
					return c.quality.failures[p.key][i].At.Before(c.quality.failures[p.key][j].At)
				})
			}
		}
		if a.Outcome == OutcomeLocal {
			if ev := c.evidence[p.key]; ev != nil && ev.State == Dead && ev.chargedExec == p.exec {
				ev.chargedLocal = true
			}
		}
		c.recordAttributionLocked(p, a, "")
		c.deliverAttributedLocked(p, a)
	}
	clearAfter(c.quality.pending, len(kept))
	c.quality.pending = kept
	return next
}

func (c *Coordinator) recordAttributionLocked(p *pendingFailure, a Attribution, reason string) {
	fields := map[string]any{
		"event":   "attribution",
		"target":  map[string]any{"name": p.key.Target.Name(), "object": p.object},
		"test":    p.key.Cond.URL,
		"probe":   p.exec,
		"started": formatTime(p.started),
		"pools":   p.pools,
		"outcome": a.Outcome,
	}
	if a.Pool != "" {
		fields["pool"] = a.Pool
	}
	if reason != "" {
		fields["reason"] = reason
	}
	if p.reconfirmed {
		fields["reconfirmed"] = true
	}
	if a.Outcome == OutcomeMember {
		fields["counts"] = CountEvents(c.quality.failures[p.key], c.now())
	}
	c.records.appendPerformance(fields)
}

func (c *Coordinator) deliverAttributedLocked(p *pendingFailure, a Attribution) {
	notice := Attributed{Exec: p.exec, Key: p.key, Started: p.started, Outcome: a.Outcome, Pool: a.Pool}
	for _, reg := range c.registrations {
		if reg.group.References(p.key.Target, p.key.Cond) {
			reg.group.Deliver(notice)
		}
	}
}

// pruneQualityLocked forgets what is out of every window and the failures of keys no group
// refers to any more.
func (c *Coordinator) pruneQualityLocked(referenced map[Key]struct{}) {
	now := c.now()
	for key, events := range c.quality.failures {
		if _, ok := referenced[key]; !ok {
			delete(c.quality.failures, key)
			continue
		}
		if events = pruneEvents(events, now); len(events) == 0 {
			delete(c.quality.failures, key)
		} else {
			c.quality.failures[key] = events
		}
	}
	for key, events := range c.quality.pools {
		if events = pruneEvents(events, now); len(events) == 0 {
			delete(c.quality.pools, key)
		} else {
			c.quality.pools[key] = events
		}
	}
}
