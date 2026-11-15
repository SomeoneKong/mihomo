package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/iface"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
)

// interval is a time span; an open one has no end yet.
type interval struct {
	start, end time.Time
}

func (iv interval) covers(t time.Time) bool {
	return !t.Before(iv.start) && (iv.end.IsZero() || t.Before(iv.end))
}

type failureNote struct {
	target C.Proxy
	at     time.Time
}

// demands are the anomalies waiting for a check of the local network to explain them: only
// the earliest of them matters, what they were, and the tests they came from, which ask for
// one check however many keys, groups or reasons a test tells them to.
type demands struct {
	first   time.Time
	reasons []string
	execs   map[uint64]bool
}

func (d *demands) add(at time.Time, exec uint64, reasons []string) {
	if d.first.IsZero() || at.Before(d.first) {
		d.first = at
	}
	for _, reason := range reasons {
		if !containsString(d.reasons, reason) {
			d.reasons = append(d.reasons, reason)
		}
	}
	if exec != 0 {
		if d.execs == nil {
			d.execs = map[uint64]bool{}
		}
		d.execs[exec] = true
	}
}

func (d *demands) empty() bool {
	return d.first.IsZero()
}

// network follows what the checks of the local network found. While it seems down, the
// automatic tests are throttled: one at a time, in rounds.
type network struct {
	state NetworkState
	since time.Time // of the state
	start time.Time // of the coverage: the earliest anomaly the checks explained
	// downSince is where the backoffs reset when the network stops seeming down start from
	downSince time.Time
	failures  []failureNote
	success   time.Time // of the latest success: an adopted test, or new connection that received data
	// seen is when a success was last heard of, whenever what succeeded started
	seen time.Time
	// activity is when a new anomaly or real traffic was last heard of, traffic the time data
	// last flowed through a group (unix nanoseconds, kept without the lock): a network not
	// normal ends once it has been idle for UseWindow
	activity time.Time
	traffic  atomic.Int64
	// firstData is when the latest real connection to receive data was dialed (unix nanoseconds)
	firstData atomic.Int64

	pending demands // waiting for the next check
	batch   demands // explained by the check running
	// explained are the tests checks explained lately, with when: another group telling of one
	// soon after calls for no check
	explained map[uint64]time.Time
	checking  bool
	// gen numbers the checks: a result of an earlier one changes nothing.
	gen        uint64
	checkStart time.Time // of the check running, or of the last one: the next waits an interval
	// heard is set when a success that tells the network is not out came while the check ran:
	// an adopted test, or a connection dialed since the check started that received data
	heard bool
	// continued is set when a success ended the network seeming down while a check ran: the
	// check goes on, and finding all its entries out it finds the network poor
	continued bool

	normalAt  time.Time // when the last check that found the network normal started
	confirmed time.Time // when a check last found it not normal
	recovered int       // clean checks in a row while it is poor

	past    []interval
	timer   *time.Timer
	timerAt time.Time

	// the throttle of the automatic tests while the network seems down
	round     []Key
	nextStart time.Time
	running   *execution
	retained  map[Key]*Registration // repairs dropped while they waited: one more chance each
}

// explain notes a test a check explained now; those explained a CheckInterval ago are let go.
func (n *network) explain(exec uint64, now time.Time) {
	if exec == 0 {
		return
	}
	for e, at := range n.explained {
		if now.Sub(at) >= CheckInterval {
			delete(n.explained, e)
		}
	}
	if n.explained == nil {
		n.explained = map[uint64]time.Time{}
	}
	n.explained[exec] = now
}

func (n *network) wasExplained(exec uint64, now time.Time) bool {
	at, ok := n.explained[exec]
	return exec != 0 && ok && now.Sub(at) < CheckInterval
}

// until is when a network not normal ends by itself: no check confirmed it for LocalConfirm,
// or nothing used it for UseWindow.
func (n *network) until() time.Time {
	if n.state == NetworkNormal {
		return time.Time{}
	}
	end := n.confirmed.Add(LocalConfirm)
	if idle := n.lastActivity().Add(UseWindow); idle.Before(end) {
		end = idle
	}
	return end
}

// lastActivity is when the network was last used, or a new anomaly called for a check.
func (n *network) lastActivity() time.Time {
	last := n.activity
	if traffic := n.traffic.Load(); traffic != 0 {
		if at := time.Unix(0, traffic); at.After(last) {
			last = at
		}
	}
	return last
}

// current is the time the network has not seemed normal so far, up to until at most.
func (n *network) current(until time.Time) (interval, bool) {
	if n.state == NetworkNormal {
		return interval{}, false
	}
	end := until
	if limit := n.until(); limit.Before(end) {
		end = limit
	}
	return interval{start: n.start, end: end}, true
}

// excused is how long of [since, until) the network did not seem normal, the current time
// included; the times older than the long window are forgotten.
func (n *network) excused(since, until time.Time) time.Duration {
	intervals := append([]interval(nil), n.past...)
	if iv, ok := n.current(until); ok {
		intervals = append(intervals, iv)
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start.Before(intervals[j].start) })
	var total time.Duration
	var reached time.Time // the end of what is counted, so that overlaps count once
	for _, iv := range intervals {
		from, to := iv.start, iv.end
		if from.Before(since) {
			from = since
		}
		if from.Before(reached) {
			from = reached
		}
		if to.After(until) {
			to = until
		}
		if to.After(from) {
			total += to.Sub(from)
			reached = to
		}
	}
	return total
}

func (n *network) covers(t time.Time) bool {
	if n.state != NetworkNormal && !t.Before(n.start) && t.Before(n.until()) {
		return true
	}
	for _, iv := range n.past {
		if iv.covers(t) {
			return true
		}
	}
	return false
}

// lastEnd is when the network last stopped seeming not normal.
func (n *network) lastEnd() time.Time {
	var end time.Time
	for _, iv := range n.past {
		if iv.end.After(end) {
			end = iv.end
		}
	}
	return end
}

// tcpFirstHop reports whether a node reaches its server over TCP, which a check of the local
// network can connect to without the protocol.
func tcpFirstHop(p C.Proxy) bool {
	// what the type does not tell, the config does
	if f, ok := adapterOf(p).(interface{ FirstHopTCP() bool }); ok {
		return f.FirstHopTCP()
	}
	switch p.Type() {
	case C.Shadowsocks, C.ShadowsocksR, C.Snell, C.Socks5, C.Http, C.Vmess, C.Vless, C.Trojan,
		C.Ssh, C.AnyTLS, C.Sudoku, C.GostRelay, C.X365, C.Ninja:
		return true
	}
	return false
}

// adapterOf is the protocol adapter of a node, under the wrappers the config adds.
func adapterOf(p C.Proxy) C.ProxyAdapter {
	a := p.Adapter()
	for i := 0; i < 8; i++ {
		w, ok := a.(interface{ WrappedAdapter() C.ProxyAdapter })
		if !ok {
			break
		}
		a = w.WrappedAdapter()
	}
	return a
}

// refused reports whether a connection was refused by the host, which the network reached.
func refused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(strings.ToLower(err.Error()), "refused")
}

// localError reports whether the local stack itself failed a connection: no route, the network
// unreachable, or the outgoing interface or address unusable.
func localError(err error) bool {
	for _, errno := range []syscall.Errno{syscall.ENETUNREACH, syscall.EHOSTUNREACH, syscall.EADDRNOTAVAIL, syscall.ENETDOWN, syscall.ENODEV} {
		if errors.Is(err, errno) {
			return true
		}
	}
	if errors.Is(err, iface.ErrIfaceNotFound) || errors.Is(err, iface.ErrAddrNotFound) {
		return true
	}
	var errno syscall.Errno
	if runtime.GOOS == "windows" && errors.As(err, &errno) {
		switch errno {
		case 10049, 10050, 10051, 10065: // WSAEADDRNOTAVAIL, WSAENETDOWN, WSAENETUNREACH, WSAEHOSTUNREACH
			return true
		}
	}
	message := strings.ToLower(err.Error())
	for _, text := range []string{"unreachable", "no route to host", "assign requested address", "no such network interface", "no such device", "network is down"} {
		if strings.Contains(message, text) {
			return true
		}
	}
	return false
}

// hopOptions are the dial options of a node: its interface, routing mark and address family,
// without fast open, which connects only once data is written.
func hopOptions(p C.Proxy) []dialer.Option {
	if a, ok := adapterOf(p).(interface{ DialOptions() []dialer.Option }); ok {
		return append(a.DialOptions(), dialer.WithTFO(false))
	}
	var options []dialer.Option
	info := p.ProxyInfo()
	if info.Interface != "" {
		options = append(options, dialer.WithInterface(info.Interface))
	}
	if info.RoutingMark != 0 {
		options = append(options, dialer.WithRoutingMark(info.RoutingMark))
	}
	return options
}

// resolveHop finds the one address a check connects to for a node: of the family the node is
// limited to, else of the one it prefers when there is one, else the first resolved.
func resolveHop(ctx context.Context, p C.Proxy) (netip.AddrPort, error) {
	host, portText, err := net.SplitHostPort(p.Addr())
	if err != nil {
		return netip.AddrPort{}, err
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		return netip.AddrPort{}, err
	}
	prefer := C.DualStack
	if a, ok := adapterOf(p).(interface{ IPVersion() C.DNSPrefer }); ok {
		prefer = a.IPVersion()
	}
	var ips []netip.Addr
	switch prefer {
	case C.IPv4Only:
		ips, err = resolver.LookupIPv4WithResolver(ctx, host, resolver.ProxyServerHostResolver)
	case C.IPv6Only:
		ips, err = resolver.LookupIPv6WithResolver(ctx, host, resolver.ProxyServerHostResolver)
	default:
		ips, err = resolver.LookupIPWithResolver(ctx, host, resolver.ProxyServerHostResolver)
	}
	if err == nil && len(ips) == 0 {
		err = errors.New("no address")
	}
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("dns resolve failed: %w", err)
	}
	pick := ips[0].Unmap()
	for _, ip := range ips {
		ip = ip.Unmap()
		if prefer == C.IPv4Prefer && ip.Is4() || prefer == C.IPv6Prefer && ip.Is6() {
			pick = ip
			break
		}
	}
	// the endpoint the dialer connects to: an IP4P address stands for another
	if pick.Is6() {
		pick, portText = resolver.LookupIP4P(pick, strconv.Itoa(port))
		if port, err = strconv.Atoi(portText); err != nil {
			return netip.AddrPort{}, err
		}
	}
	return netip.AddrPortFrom(pick, uint16(port)), nil
}

// dialHop connects to the address of a node over TCP as the node would, without the protocol.
func dialHop(ctx context.Context, p C.Proxy, addr netip.AddrPort) (time.Duration, error) {
	started := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", addr.String(), hopOptions(p)...)
	elapsed := time.Since(started)
	if err != nil {
		if refused(err) {
			return elapsed, nil // the host answered: the network reaches it
		}
		return elapsed, err
	}
	_ = conn.Close()
	return elapsed, nil
}

// NoteDialFailure tells the coordinator that a real dial through target failed, which may
// suggest the local network is down.
func (c *Coordinator) NoteDialFailure(target C.Proxy, at time.Time) {
	if target == nil {
		return
	}
	c.mutex.Lock()
	c.noteActivityLocked()
	if !c.delegatedTargetLocked(target) {
		if first, down := c.noteFailureLocked(target, at); down {
			c.demandLocked(first, 0, "down")
		}
	}
	c.mutex.Unlock()
}

// NoteNewSuccess tells the coordinator that a connection through target dialed at a time
// received data: it ends a suspected network failure, and tells of the pool of the target.
func (c *Coordinator) NoteNewSuccess(target C.Proxy, dialed time.Time) {
	if target == nil {
		return
	}
	c.mutex.Lock()
	c.noteActivityLocked()
	if !c.delegatedTargetLocked(target) {
		c.noteSuccessLocked(dialed, true)
		c.poolSuccessLocked(target, dialed, false)
	}
	c.mutex.Unlock()
	c.kick()
}

// NoteDegradation tells the coordinator that a pair of tests found the preferred member of a
// group degraded, in the test exec that started then: a check of the local network is to
// explain it.
func (c *Coordinator) NoteDegradation(target C.Proxy, exec uint64, started time.Time) {
	if target == nil {
		return
	}
	c.mutex.Lock()
	c.demandLocked(started, exec, "degradation")
	c.mutex.Unlock()
	c.kick()
}

// NoteFirstData tells the coordinator, as soon as it happens and without the lock, that a real
// connection dialed at a time (unix nanoseconds) received its first data: one dialed while a
// check runs tells the network is not out, however late the group hands the success on.
func (c *Coordinator) NoteFirstData(dialed int64) {
	storeLater(&c.network.firstData, dialed)
}

// NoteTraffic tells the coordinator that real traffic flowed through a group at a time (unix
// nanoseconds): the local network is in use. It takes the lock only after traffic stopped for
// UseWindow, for a network not normal that expired meanwhile to end where it expired.
func (c *Coordinator) NoteTraffic(at int64) {
	n := c.network
	last := n.traffic.Load()
	if at <= last {
		return
	}
	if last == 0 || time.Duration(at-last) >= UseWindow {
		c.mutex.Lock()
		c.expireNetworkLocked()
		storeLater(&n.traffic, at)
		c.mutex.Unlock()
		return
	}
	storeLater(&n.traffic, at)
}

// storeLater raises v to value at least.
func storeLater(v *atomic.Int64, value int64) {
	for {
		last := v.Load()
		if value <= last || v.CompareAndSwap(last, value) {
			return
		}
	}
}

// noteActivityLocked takes a dial failure, a connection that received data or an anomaly,
// once a network not normal that expired meanwhile has ended.
func (c *Coordinator) noteActivityLocked() {
	c.expireNetworkLocked()
	if now := c.now(); now.After(c.network.activity) {
		c.network.activity = now
	}
}

// noteFailureLocked takes a failure that started at. Failures of distinct members within
// LocalWindow, with no success since the first of them, suggest the network is down, whether
// they are new or not: down tells so, and first is when the first of them started. A failure
// that started longer ago tells nothing of the network now.
func (c *Coordinator) noteFailureLocked(target C.Proxy, at time.Time) (first time.Time, down bool) {
	n := c.network
	now := c.now()
	kept := n.failures[:0]
	for _, f := range n.failures {
		if now.Sub(f.at) < LocalWindow {
			kept = append(kept, f)
		}
	}
	clearAfter(n.failures, len(kept))
	n.failures = kept
	if now.Sub(at) >= LocalWindow {
		return time.Time{}, false
	}
	// only the failures that started after the last success heard of tell so, and one that
	// started before calls for nothing, whatever the others tell
	if !at.After(n.seen) {
		return time.Time{}, false
	}
	// a member failing over and over is noted once a second at most: what counts is which
	// members failed, and the first of their failures
	noted := false
	for _, f := range n.failures {
		if f.target == target && f.at.After(n.seen) {
			if gap := at.Sub(f.at); gap > -time.Second && gap < time.Second {
				noted = true
				break
			}
		}
	}
	if !noted {
		n.failures = append(n.failures, failureNote{target: target, at: at})
	}
	distinct := map[C.Proxy]bool{}
	for _, f := range n.failures {
		if !f.at.After(n.seen) {
			continue
		}
		distinct[f.target] = true
		if first.IsZero() || f.at.Before(first) {
			first = f.at
		}
	}
	return first, len(distinct) >= LocalFailures
}

// noteSuccessLocked takes a success that started at: a test, or a connection dialed then that
// received data. A test tells of the network when it ends; a connection only if it is new: one
// dialed before the network seemed down, or long before, receiving late data tells nothing.
// While a check runs, a test, or a connection dialed since it started, tells the network is not
// out.
func (c *Coordinator) noteSuccessLocked(at time.Time, connection bool) {
	n := c.network
	now := c.now()
	if at.After(n.success) {
		n.success = at
	}
	if !connection || now.Sub(at) <= LocalWindow {
		n.seen = now
	}
	if n.checking && (!connection || !at.Before(n.checkStart)) {
		n.heard = true
	}
	c.expireNetworkLocked()
	if n.state == NetworkDown && (!connection || !at.Before(n.since)) {
		c.endNetworkLocked("success", now)
		c.runNetworkLocked()
	}
}

// demandLocked takes an anomaly that started at, found by the test exec (0 for none), for a
// check of the local network to explain: a member newly found dead, a degradation, or failures
// suggesting the network is down. While the network does not seem normal, the coverage reaches
// back to it instead.
func (c *Coordinator) demandLocked(at time.Time, exec uint64, reasons ...string) {
	n := c.network
	c.noteActivityLocked()
	if n.state != NetworkNormal {
		c.extendCoverageLocked(at)
		n.explain(exec, c.now())
		return
	}
	switch {
	case exec != 0 && n.checking && n.batch.execs[exec]:
		n.batch.add(at, exec, reasons) // the check running explains it
		return
	case n.wasExplained(exec, c.now()):
		return // a check explained it lately
	}
	n.pending.add(at, exec, reasons)
	c.runNetworkLocked()
}

// boundLocked is the earliest the coverage of a network not normal may start: after the start
// of the last check that found it normal, and not before the end of the last time it was not,
// nor before the attribution delay.
func (c *Coordinator) boundLocked(start time.Time) time.Time {
	n := c.network
	lower := c.now().Add(-AttributionDelay)
	if after := n.normalAt.Add(time.Nanosecond); !n.normalAt.IsZero() && after.After(lower) {
		lower = after
	}
	if end := n.lastEnd(); end.After(lower) {
		lower = end
	}
	if start.Before(lower) {
		return lower
	}
	return start
}

// extendCoverageLocked lets the coverage of a network not normal reach back to an anomaly that
// started earlier than it.
func (c *Coordinator) extendCoverageLocked(at time.Time) {
	n := c.network
	start := c.boundLocked(at)
	if !start.Before(n.start) {
		return
	}
	if n.state == NetworkDown && n.downSince.Equal(n.start) {
		n.downSince = start
	}
	n.start = start
	c.records.appendPerformance(map[string]any{"event": "network", "state": n.state, "since": formatTime(n.since), "coverageStart": formatTime(n.start), "reason": "extended"})
	c.coveredLocked()
}

// coveredLocked keeps the failures the local network explains out of the outages of pools:
// they are not among the recent failures that suggest one, and an outage suspected from them is
// no outage of the pool.
func (c *Coordinator) coveredLocked() {
	n := c.network
	for pk, list := range c.outages.recent {
		kept := list[:0]
		for _, f := range list {
			if !n.covers(f.started) {
				kept = append(kept, f)
			}
		}
		clearAfter(list, len(kept))
		if len(kept) == 0 {
			delete(c.outages.recent, pk)
		} else {
			c.outages.recent[pk] = kept
		}
	}
	for _, o := range c.outages.active {
		c.markCoveredLocked(o)
		if n.covers(o.first) {
			c.concludeOutageLocked(o, OutageUndecided, map[string]any{"network": n.state})
		}
	}
	for _, o := range c.outages.past {
		if o.state == OutageEstablished && !o.committed {
			c.markCoveredLocked(o)
		}
	}
}

// expireNetworkLocked ends a network not normal that no check confirmed for long enough, or
// that nothing used for long enough.
func (c *Coordinator) expireNetworkLocked() {
	n := c.network
	if n.state == NetworkNormal {
		return
	}
	until := n.until()
	if c.now().Before(until) {
		return
	}
	reason := "unconfirmed"
	if until.Before(n.confirmed.Add(LocalConfirm)) {
		reason = "idle"
	}
	c.endNetworkLocked(reason, until)
}

// nextCheckLocked is when the next check of the local network is due; zero when none is.
func (c *Coordinator) nextCheckLocked() time.Time {
	n := c.network
	switch n.state {
	case NetworkNormal:
		if n.pending.empty() {
			return time.Time{}
		}
		return n.checkStart.Add(CheckInterval)
	case NetworkDown:
		return n.checkStart.Add(DownInterval)
	}
	return n.checkStart.Add(CheckInterval)
}

// runNetworkLocked ends a network not normal that expired, starts the check that is due, and
// arms the timer for what comes next.
func (c *Coordinator) runNetworkLocked() {
	n := c.network
	c.expireNetworkLocked()
	// while a config is published, the nodes it may drop are not checked: Activate kicks
	next := c.nextCheckLocked()
	if !n.checking && !c.publishing && !next.IsZero() && !c.now().Before(next) {
		c.startCheckLocked()
	}
	var at time.Time
	if !n.checking && !c.publishing {
		at = c.nextCheckLocked() // the one after a check running is armed when it ends
	}
	if until := n.until(); !until.IsZero() && (at.IsZero() || until.Before(at)) {
		at = until
	}
	if n.timer != nil && n.timerAt.Equal(at) {
		return
	}
	if n.timer != nil {
		n.timer.Stop()
		n.timer, n.timerAt = nil, time.Time{}
	}
	if at.IsZero() {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(at.Sub(c.now()), func() {
		c.mutex.Lock()
		if n.timer == timer {
			n.timer, n.timerAt = nil, time.Time{}
			c.runNetworkLocked()
		}
		c.mutex.Unlock()
		c.kick()
	})
	n.timer, n.timerAt = timer, at
}

// hopCandidate is a node a check may connect to.
type hopCandidate struct {
	p      C.Proxy
	pool   string // its traffic pool, or its own object number when it has none
	object uint64
}

// hopTarget is an entry a check connects to: a node and the address it resolved to.
type hopTarget struct {
	hopCandidate
	addr netip.AddrPort
}

type hopSample struct {
	elapsed time.Duration
	err     error
	skipped bool // not made: the check stopped early
}

// checkCandidatesLocked lists the nodes a check may connect to: found alive before, over TCP,
// not through a dialer proxy and not excluded. The pools take turns, latest found alive
// first, so that the entries a check reaches are as independent as they can be.
func (c *Coordinator) checkCandidatesLocked() []hopCandidate {
	table := c.exclusions.Current()
	var evaluator *Evaluator
	if len(table.Pools) > 0 {
		evaluator = NewEvaluatorFor(table, c.Lookup)
	}
	type found struct {
		p      C.Proxy
		at     time.Time
		object uint64
	}
	byPool := map[string][]found{}
	for p, at := range c.lastAlive {
		if IsBuiltin(p) || !tcpFirstHop(p) || p.ProxyInfo().DialerProxy != "" {
			continue
		}
		if _, isGroup := p.Adapter().(groupView); isGroup {
			continue
		}
		if evaluator != nil {
			if out, _ := evaluator.Excluded(p); out {
				continue
			}
		}
		object := c.objectIDLocked(p)
		pool := poolOf(p)
		if pool == "" {
			pool = "\x00" + strconv.FormatUint(object, 10) // a node of no pool is a pool of its own
		}
		byPool[pool] = append(byPool[pool], found{p: p, at: at, object: object})
	}
	newer := func(a, b found) bool {
		if !a.at.Equal(b.at) {
			return a.at.After(b.at)
		}
		return a.object < b.object
	}
	type poolNodes struct {
		pool  string
		nodes []found
	}
	pools := make([]poolNodes, 0, len(byPool))
	for pool, nodes := range byPool {
		sort.Slice(nodes, func(i, j int) bool { return newer(nodes[i], nodes[j]) })
		pools = append(pools, poolNodes{pool: pool, nodes: nodes})
	}
	sort.Slice(pools, func(i, j int) bool { return newer(pools[i].nodes[0], pools[j].nodes[0]) })
	var candidates []hopCandidate
	for i := 0; len(candidates) < LocalCandidates; i++ {
		added := false
		for _, pn := range pools {
			if i < len(pn.nodes) && len(candidates) < LocalCandidates {
				f := pn.nodes[i]
				candidates = append(candidates, hopCandidate{p: f.p, pool: pn.pool, object: f.object})
				added = true
			}
		}
		if !added {
			break
		}
	}
	return candidates
}

// startCheckLocked checks the local network in the background, for the anomalies waiting.
func (c *Coordinator) startCheckLocked() {
	n := c.network
	n.checking = true
	n.gen++
	gen := n.gen
	n.checkStart = c.now()
	n.heard = false
	n.firstData.Store(0) // what came before tells nothing of this check, the clock set back too
	n.batch, n.pending = n.pending, demands{}
	candidates := c.checkCandidatesLocked()
	if len(candidates) == 0 {
		c.checkedLocked(gen, nil, nil, nil, false)
		return
	}
	resolve, dial, spacing := c.resolveHop, c.dialHop, c.spacing
	go func() {
		targets, tried := selectTargets(candidates, resolve)
		samples, out := sampleTargets(targets, dial, spacing)
		c.mutex.Lock()
		c.checkedLocked(gen, tried, targets, samples, out)
		c.mutex.Unlock()
		c.kick()
	}()
}

// selectTargets resolves the candidates at once and takes, in their order, up to LocalTargets
// of them of distinct pools and addresses: an entry shared counts once.
func selectTargets(candidates []hopCandidate, resolve func(context.Context, C.Proxy) (netip.AddrPort, error)) ([]hopTarget, []any) {
	ctx, cancel := context.WithTimeout(context.Background(), LocalResolve)
	defer cancel()
	addrs := make([]netip.AddrPort, len(candidates))
	errs := make([]error, len(candidates))
	var wg sync.WaitGroup
	for i, cand := range candidates {
		wg.Add(1)
		go func(i int, p C.Proxy) {
			defer wg.Done()
			addrs[i], errs[i] = resolve(ctx, p)
		}(i, cand.p)
	}
	wg.Wait()
	var targets []hopTarget
	pools := map[string]bool{}
	ips := map[netip.Addr]bool{}
	tried := make([]any, 0, len(candidates))
	for i, cand := range candidates {
		entry := map[string]any{"name": cand.p.Name(), "object": cand.object}
		switch {
		case errs[i] != nil:
			entry["skipped"] = "resolve"
			entry["error"] = shortText(errs[i].Error())
		case pools[cand.pool]:
			entry["skipped"] = "pool"
		case ips[addrs[i].Addr()]:
			entry["skipped"] = "address"
		case len(targets) == LocalTargets:
			entry["skipped"] = "enough"
		default:
			pools[cand.pool], ips[addrs[i].Addr()] = true, true
			targets = append(targets, hopTarget{hopCandidate: cand, addr: addrs[i]})
		}
		if errs[i] == nil {
			entry["addr"] = addrs[i].String()
		}
		tried = append(tried, entry)
	}
	return targets, tried
}

// sampleTargets connects to each target LocalSamples times, spacing apart. When the local stack
// itself fails the first connection to every one of two targets or more, the network is out:
// out tells so, and the rest are not made.
func sampleTargets(targets []hopTarget, dial func(context.Context, C.Proxy, netip.AddrPort) (time.Duration, error), spacing time.Duration) (samples [][]hopSample, out bool) {
	samples = make([][]hopSample, len(targets))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var mutex sync.Mutex
	firsts, local := 0, 0
	var wg sync.WaitGroup
	for i := range targets {
		samples[i] = make([]hopSample, LocalSamples)
		for j := 0; j < LocalSamples; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				if j > 0 {
					wait := time.NewTimer(time.Duration(j) * spacing)
					select {
					case <-wait.C:
					case <-ctx.Done():
						wait.Stop()
					}
				}
				if ctx.Err() != nil {
					samples[i][j] = hopSample{skipped: true}
					return
				}
				dialCtx, cancel := context.WithTimeout(ctx, LocalCheckTimeout)
				elapsed, err := dial(dialCtx, targets[i].p, targets[i].addr)
				cancel()
				if j > 0 && err != nil && ctx.Err() != nil {
					samples[i][j] = hopSample{skipped: true}
					return
				}
				samples[i][j] = hopSample{elapsed: elapsed, err: err}
				if j == 0 {
					mutex.Lock()
					firsts++
					if err != nil && localError(err) {
						local++
					}
					if firsts == len(targets) && local == len(targets) && len(targets) >= 2 {
						out = true
						stop()
					}
					mutex.Unlock()
				}
			}(i, j)
		}
	}
	wg.Wait()
	return samples, out
}

const checkInsufficient = "insufficient"

// verdict of a check: what it found of the local network, from how many independent entries.
type checkVerdict struct {
	result  string // down, poor, normal, or insufficient
	clean   bool   // normal, with LocalClean bad samples at most
	reached int
	bad     int
	samples int // made to the entries reached
}

// judge tells what the samples of a check find: the network down when no entry of two or
// more is reached, or when the local stack failed the first connection to each (out); poor
// when two or more are out while another is reached, when the bad samples of two entries or
// more make LocalBadShare of those reached, or when every entry reached is slow; normal when
// two or more are reached otherwise. Fewer entries tell nothing.
func judge(samples [][]hopSample, out bool) checkVerdict {
	var v checkVerdict
	targets, unreachable, badTargets := len(samples), 0, 0
	allSlow := true
	for _, list := range samples {
		fastest := time.Duration(-1)
		for _, s := range list {
			if !s.skipped && s.err == nil && (fastest < 0 || s.elapsed < fastest) {
				fastest = s.elapsed
			}
		}
		if fastest < 0 {
			unreachable++
			continue
		}
		v.reached++
		if fastest <= FastDial {
			allSlow = false
		}
		bad := 0
		for _, s := range list {
			if s.skipped {
				continue
			}
			v.samples++
			if s.err != nil || s.elapsed-fastest >= LocalSlowExtra {
				bad++
			}
		}
		v.bad += bad
		if bad > 0 {
			badTargets++
		}
	}
	switch {
	case targets >= 2 && (v.reached == 0 || out):
		v.result = NetworkDown.String()
	case targets >= 2 && (unreachable >= 2 && v.reached >= 1 ||
		v.reached >= 2 && badTargets >= 2 && float64(v.bad) >= LocalBadShare*float64(v.samples) ||
		v.reached >= 2 && allSlow):
		v.result = NetworkPoor.String()
	case v.reached >= 2:
		v.result = NetworkNormal.String()
		v.clean = v.bad <= LocalClean
	default:
		v.result = checkInsufficient
	}
	return v
}

func (c *Coordinator) checkedLocked(gen uint64, tried []any, targets []hopTarget, samples [][]hopSample, out bool) {
	n := c.network
	// a network not normal that expired while the check ran ends where it expired; the check
	// then changes nothing, and the next may start
	c.expireNetworkLocked()
	n.checking = false
	if gen != n.gen {
		n.continued = false
		c.records.appendPerformance(map[string]any{"event": "networkCheck", "result": "void", "candidates": tried, "reached": judge(samples, out).reached})
		c.runNetworkLocked()
		return
	}
	now := c.now()
	v := judge(samples, out)
	heard := n.heard || n.continued || n.firstData.Load() >= n.checkStart.UnixNano()
	n.continued = false
	if v.result == NetworkDown.String() && heard {
		// the entries are all out while something else works: poor, not down
		v.result = NetworkPoor.String()
	}
	noTargets := len(tried) == 0
	batch := n.batch
	n.batch = demands{}
	for exec := range batch.execs {
		n.explain(exec, now)
	}
	reasons := batch.reasons
	if n.state != NetworkNormal {
		reasons = []string{"recheck"}
	}
	var checks []any
	for i, t := range targets {
		var list []any
		for _, s := range samples[i] {
			entry := map[string]any{"elapsed": s.elapsed.Milliseconds(), "ok": s.err == nil && !s.skipped}
			switch {
			case s.skipped:
				entry["skipped"] = true
			case s.err != nil:
				entry["error"] = shortText(s.err.Error())
				entry["local"] = localError(s.err)
			}
			list = append(list, entry)
		}
		checks = append(checks, map[string]any{
			"target":    map[string]any{"name": t.p.Name(), "object": t.object, "addr": t.addr.String()},
			"transport": "tcp", "samples": list,
		})
	}
	fields := map[string]any{
		"event": "networkCheck", "reason": reasons, "candidates": tried, "checks": checks, "result": v.result,
		"reached": v.reached, "bad": v.bad, "samples": v.samples, "before": n.state,
	}
	if noTargets {
		fields["result"] = "noTargets"
	}
	if v.result == NetworkNormal.String() {
		fields["clean"] = v.clean
	}
	if heard {
		fields["success"] = formatTime(n.seen)
	}
	if !batch.empty() {
		fields["first"] = formatTime(batch.first)
	}
	c.records.appendPerformance(fields)

	switch n.state {
	case NetworkNormal:
		switch v.result {
		case NetworkDown.String():
			c.enterNetworkLocked(NetworkDown, batch)
		case NetworkPoor.String():
			c.enterNetworkLocked(NetworkPoor, batch)
		case NetworkNormal.String():
			n.normalAt = n.checkStart
		}
	case NetworkDown:
		switch v.result {
		case NetworkDown.String():
			n.confirmed = now
		case NetworkPoor.String():
			c.resetDownBackoffLocked(now)
			n.state, n.since, n.confirmed, n.recovered = NetworkPoor, now, now, 0
			n.round, n.nextStart, n.running, n.retained = nil, time.Time{}, nil, nil
			c.recordNetworkLocked("check")
			c.deliverNetworkLocked()
		case NetworkNormal.String():
			c.endNetworkLocked("recovered", now)
		default:
			if noTargets {
				c.endNetworkLocked("noTargets", now)
			}
		}
	case NetworkPoor:
		switch v.result {
		case NetworkDown.String():
			n.state, n.since, n.downSince, n.confirmed, n.recovered = NetworkDown, now, now, now, 0
			n.round, n.nextStart, n.running = nil, time.Time{}, nil
			c.recordNetworkLocked("check")
			c.deliverNetworkLocked()
		case NetworkPoor.String():
			n.confirmed, n.recovered = now, 0
		case NetworkNormal.String():
			if !v.clean {
				n.recovered = 0
				break
			}
			n.recovered++
			if n.recovered >= LocalRecovered {
				c.endNetworkLocked("recovered", now)
			}
		default:
			n.recovered = 0
			if noTargets {
				c.endNetworkLocked("noTargets", now)
			}
		}
	}
	c.runNetworkLocked()
}

// enterNetworkLocked takes a check that found a network seeming normal down or poor. Its
// coverage starts at the earliest anomaly waiting, this check's or the next one's, or at the
// check when there is none, as far back as boundLocked allows.
func (c *Coordinator) enterNetworkLocked(state NetworkState, batch demands) {
	n := c.network
	now := c.now()
	start := n.checkStart
	for _, d := range []demands{batch, n.pending} {
		if !d.empty() && d.first.Before(start) {
			start = d.first
		}
	}
	start = c.boundLocked(start)
	// the anomalies waiting are explained too
	for exec := range n.pending.execs {
		n.explain(exec, now)
	}
	n.pending = demands{}
	n.state, n.since, n.start = state, now, start
	n.confirmed, n.recovered = now, 0
	if state == NetworkDown {
		n.downSince = start
	}
	n.round, n.nextStart, n.running = nil, time.Time{}, nil
	c.recordNetworkLocked("check")
	c.deliverNetworkLocked()
	c.coveredLocked()
}

func (c *Coordinator) recordNetworkLocked(reason string) {
	n := c.network
	c.records.appendPerformance(map[string]any{
		"event": "network", "state": n.state, "since": formatTime(n.since), "coverageStart": formatTime(n.start),
		"reason": reason, "display": networkDisplay(n.state, reason),
	})
}

func (c *Coordinator) deliverNetworkLocked() {
	notice := NetworkChanged{State: c.network.state, Since: c.network.since}
	for _, reg := range c.registrations {
		reg.group.Deliver(notice)
	}
}

// resetDownBackoffLocked lets the members found dead while the network seemed down be tested
// again without waiting for a backoff.
func (c *Coordinator) resetDownBackoffLocked(now time.Time) {
	for _, ev := range c.evidence {
		if ev.State == Dead && !ev.DeadSince.IsZero() && !ev.DeadSince.Before(c.network.downSince) {
			ev.Failures = 0
			ev.NextAllowed = now
		}
	}
}

// endNetworkLocked ends, at a time, a time the network did not seem normal. The evidence
// stays; the members found dead while it seemed down are tested again without waiting for a
// backoff. A check running goes on only when a success ended it.
func (c *Coordinator) endNetworkLocked(reason string, at time.Time) {
	n := c.network
	if n.state == NetworkNormal {
		return
	}
	now := c.now()
	if n.state == NetworkDown {
		c.resetDownBackoffLocked(now)
	}
	n.past = append(n.past, interval{start: n.start, end: at})
	kept := n.past[:0]
	for _, p := range n.past {
		if now.Sub(p.end) < LongWindow {
			kept = append(kept, p)
		}
	}
	n.past = kept
	n.state, n.since, n.recovered = NetworkNormal, at, 0
	n.round, n.running, n.retained = nil, nil, nil
	if reason == "success" && n.checking {
		n.continued = true
	} else {
		// a check still running changes nothing; it keeps its place until it ends
		n.gen++
		n.continued = false
	}
	c.recordNetworkLocked(reason)
	c.deliverNetworkLocked()
	c.attributeDueLocked()
}

// Network returns the state of the local network and since when.
func (c *Coordinator) Network() (NetworkState, time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.expireNetworkLocked()
	return c.network.state, c.network.since
}

func shortText(text string) string {
	if len(text) > 200 {
		return text[:200]
	}
	return text
}
