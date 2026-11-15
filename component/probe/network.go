package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
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

// network follows what the checks of the local network found. While it seems down, the
// automatic tests are throttled: one at a time, in rounds.
type network struct {
	state    NetworkState
	since    time.Time
	start    time.Time // of the coverage: the first failure that led to the check
	failures []failureNote
	success  time.Time // of the latest success: an adopted test, or new connection that received data
	checking bool
	// gen numbers the checks: a result of an earlier one, or of one that ran past a success,
	// changes nothing.
	gen        uint64
	checkStart time.Time
	// seen is when a success was last heard of, whenever what succeeded started
	seen  time.Time
	past  []interval
	timer *time.Timer

	// the throttle of the automatic tests while the network seems down
	round     []Key
	nextStart time.Time
	running   *execution
	retained  map[Key]*Registration // repairs dropped while they waited: one more chance each
}

// excused is how long of [since, until) the network did not seem normal, the current time
// included; the times older than the long window are forgotten.
func (n *network) excused(since, until time.Time) time.Duration {
	intervals := append([]interval(nil), n.past...)
	if n.state != NetworkNormal {
		intervals = append(intervals, interval{start: n.start, end: until})
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
	if n.state != NetworkNormal && !t.Before(n.start) {
		return true
	}
	for _, iv := range n.past {
		if iv.covers(t) {
			return true
		}
	}
	return false
}

// tcpFirstHop reports whether a node reaches its server over TCP, which a check of the local
// network can connect to without the protocol.
func tcpFirstHop(p C.Proxy) bool {
	// a transport over UDP the type does not tell
	if f, ok := adapterOf(p).(interface{ FirstHopTCP() bool }); ok && !f.FirstHopTCP() {
		return false
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
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "dns resolve failed") {
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(message, "refused")
}

// firstHop connects to the server of a node over TCP as the node would, without the protocol.
func firstHop(ctx context.Context, p C.Proxy) (time.Duration, error) {
	var options []dialer.Option
	if a, ok := adapterOf(p).(interface{ DialOptions() []dialer.Option }); ok {
		// the interface, routing mark and address family of the node; without fast open,
		// which connects only once data is written
		options = append(a.DialOptions(), dialer.WithTFO(false))
	} else {
		info := p.ProxyInfo()
		if info.Interface != "" {
			options = append(options, dialer.WithInterface(info.Interface))
		}
		if info.RoutingMark != 0 {
			options = append(options, dialer.WithRoutingMark(info.RoutingMark))
		}
	}
	started := time.Now()
	host, _, err := net.SplitHostPort(p.Addr())
	if err != nil {
		return 0, err
	}
	// resolved first, so that a failing resolver does not count as a host refusing; the dial
	// resolves again from the cache, with the address families and fallbacks of the node
	ips, err := resolver.LookupIPWithResolver(ctx, host, resolver.ProxyServerHostResolver)
	if err == nil && len(ips) == 0 {
		err = errors.New("no address")
	}
	if err != nil {
		return time.Since(started), fmt.Errorf("dns resolve failed: %w", err)
	}
	conn, err := dialer.DialContext(ctx, "tcp", p.Addr(), options...)
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
// start a check of the local network.
func (c *Coordinator) NoteDialFailure(target C.Proxy, at time.Time) {
	if target == nil || IsBuiltin(target) {
		return
	}
	c.mutex.Lock()
	c.noteFailureLocked(target, at)
	c.mutex.Unlock()
}

// NoteNewSuccess tells the coordinator that a connection through target dialed at a time
// received data: it ends a suspected network failure, and tells of the pool of the target.
func (c *Coordinator) NoteNewSuccess(target C.Proxy, dialed time.Time) {
	if target == nil || IsBuiltin(target) {
		return
	}
	c.mutex.Lock()
	c.noteSuccessLocked(dialed, true)
	c.poolSuccessLocked(target, dialed, false)
	c.mutex.Unlock()
	c.kick()
}

func (c *Coordinator) noteFailureLocked(target C.Proxy, at time.Time) {
	n := c.network
	now := c.now()
	kept := n.failures[:0]
	for _, f := range n.failures {
		if now.Sub(f.at) < LocalWindow {
			kept = append(kept, f)
		}
	}
	clearAfter(n.failures, len(kept))
	n.failures = append(kept, failureNote{target: target, at: at})
	if n.state != NetworkNormal || n.checking {
		return
	}
	distinct := map[C.Proxy]bool{}
	first := at
	for _, f := range n.failures {
		distinct[f.target] = true
		if f.at.Before(first) {
			first = f.at
		}
	}
	if len(distinct) < LocalFailures || !n.seen.Before(first) {
		return
	}
	c.startCheckLocked(first, "failures")
}

// noteSuccessLocked takes a success that started at: a test, or a connection dialed then that
// received data. A test tells of the network when it ends; a connection only if it is new: one
// dialed before the network seemed down, or long before, receiving late data tells nothing.
func (c *Coordinator) noteSuccessLocked(at time.Time, connection bool) {
	n := c.network
	now := c.now()
	if at.After(n.success) {
		n.success = at
	}
	if !connection || now.Sub(at) <= LocalWindow {
		n.seen = now
	}
	if n.state == NetworkDown && (!connection || !at.Before(n.since)) {
		c.endNetworkLocked("success")
	}
}

// checkTargetsLocked picks the nodes a check connects to: recently found alive, over TCP and
// not through a dialer proxy, each of another pool.
func (c *Coordinator) checkTargetsLocked() []C.Proxy {
	type found struct {
		p  C.Proxy
		at time.Time
	}
	var alive []found
	for p, at := range c.lastAlive {
		if IsBuiltin(p) || !tcpFirstHop(p) || p.ProxyInfo().DialerProxy != "" {
			continue
		}
		if _, isGroup := p.Adapter().(groupView); isGroup {
			continue
		}
		alive = append(alive, found{p: p, at: at})
	}
	sort.Slice(alive, func(i, j int) bool {
		if !alive[i].at.Equal(alive[j].at) {
			return alive[i].at.After(alive[j].at)
		}
		return c.objectIDLocked(alive[i].p) < c.objectIDLocked(alive[j].p)
	})
	var targets []C.Proxy
	pools := map[string]bool{}
	for _, f := range alive {
		pool := poolOf(f.p)
		if pool == "" {
			pool = "\x00" + f.p.Name() // a node of no pool is a pool of its own
		}
		if pools[pool] {
			continue
		}
		pools[pool] = true
		targets = append(targets, f.p)
		if len(targets) == LocalTargets {
			break
		}
	}
	return targets
}

type hopResult struct {
	target  C.Proxy
	elapsed time.Duration
	err     error
}

// startCheckLocked checks the local network in the background. first is the first failure
// that led to it, where the coverage of a failure starts.
func (c *Coordinator) startCheckLocked(first time.Time, reason string) {
	n := c.network
	targets := c.checkTargetsLocked()
	if len(targets) == 0 {
		c.records.appendPerformance(map[string]any{"event": "networkCheck", "reason": reason, "result": "noTargets"})
		// nothing to tell the network from the proxies by: taken as the proxies' failures
		c.endNetworkLocked("noTargets")
		return
	}
	n.checking = true
	n.gen++
	gen := n.gen
	n.checkStart = c.now()
	check := c.firstHop
	go func() {
		results := make([]hopResult, len(targets))
		done := make(chan struct{}, len(targets))
		for i, target := range targets {
			go func(i int, target C.Proxy) {
				ctx, cancel := context.WithTimeout(context.Background(), LocalCheckTimeout)
				elapsed, err := check(ctx, target)
				cancel()
				results[i] = hopResult{target: target, elapsed: elapsed, err: err}
				done <- struct{}{}
			}(i, target)
		}
		for range targets {
			<-done
		}
		c.mutex.Lock()
		c.checkedLocked(gen, first, reason, results)
		c.mutex.Unlock()
		c.kick()
	}()
}

func (c *Coordinator) checkedLocked(gen uint64, first time.Time, reason string, results []hopResult) {
	n := c.network
	if gen != n.gen {
		return
	}
	n.checking = false
	if n.seen.After(n.checkStart) {
		// something worked while the check ran: its failures tell of the past. A success that
		// tells the network is up ended a down one already; otherwise it is checked again.
		c.records.appendPerformance(map[string]any{"event": "networkCheck", "reason": reason, "result": "superseded", "success": formatTime(n.seen)})
		c.armNetworkLocked()
		return
	}
	failed, slow := 0, 0
	var list []any
	for _, r := range results {
		entry := map[string]any{
			"target":    map[string]any{"name": r.target.Name(), "object": c.objects[r.target], "addr": r.target.Addr()},
			"transport": "tcp",
			"elapsed":   r.elapsed.Milliseconds(),
			"ok":        r.err == nil,
		}
		if r.err != nil {
			failed++
			entry["error"] = shortText(r.err.Error())
		} else if r.elapsed > FastDial {
			slow++
		}
		list = append(list, entry)
	}
	state := NetworkNormal
	switch {
	case failed == len(results):
		state = NetworkDown
	case failed > 0 || slow == len(results):
		state = NetworkPoor
	}
	c.records.appendPerformance(map[string]any{
		"event": "networkCheck", "reason": reason, "checks": list, "result": state, "before": n.state,
	})
	switch {
	case state == n.state:
	case state == NetworkNormal:
		c.endNetworkLocked("check")
		return
	case n.state == NetworkNormal:
		n.state, n.since, n.start = state, c.now(), first
		n.round, n.nextStart, n.running = nil, time.Time{}, nil
		c.recordNetworkLocked("check")
		c.deliverNetworkLocked()
		// outages of pools suspected from failures the local network explains are no
		// outages of the pools
		for _, o := range c.outages.active {
			if !o.first.Before(first) {
				c.concludeOutageLocked(o, OutageUndecided, map[string]any{"network": state})
			}
		}
	default:
		n.state, n.since = state, c.now()
		c.recordNetworkLocked("check")
		c.deliverNetworkLocked()
	}
	c.armNetworkLocked()
}

// armNetworkLocked checks the network again while it does not seem normal.
func (c *Coordinator) armNetworkLocked() {
	n := c.network
	if n.timer != nil {
		n.timer.Stop()
		n.timer = nil
	}
	if n.state == NetworkNormal {
		return
	}
	every := DownInterval
	if n.state == NetworkPoor {
		every = PoorInterval
	}
	n.timer = time.AfterFunc(every, func() {
		c.mutex.Lock()
		if n.state != NetworkNormal && !n.checking {
			c.startCheckLocked(n.start, "recheck")
		}
		c.mutex.Unlock()
	})
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

// endNetworkLocked ends a time the network did not seem normal. The evidence stays; the
// members found dead meanwhile are tested again without waiting for a backoff, and their
// dead time leaves it out.
func (c *Coordinator) endNetworkLocked(reason string) {
	n := c.network
	if n.state == NetworkNormal {
		return
	}
	now := c.now()
	wasDown := n.state == NetworkDown
	iv := interval{start: n.start, end: now}
	n.past = append(n.past, iv)
	kept := n.past[:0]
	for _, p := range n.past {
		if now.Sub(p.end) < LongWindow {
			kept = append(kept, p)
		}
	}
	n.past = kept
	for _, ev := range c.evidence {
		if ev.State != Dead || ev.DeadSince.IsZero() {
			continue
		}
		if wasDown && !ev.DeadSince.Before(iv.start) {
			ev.Failures = 0
			ev.NextAllowed = now
		}
	}
	n.state, n.since = NetworkNormal, now
	n.round, n.running, n.retained = nil, nil, nil
	n.gen++ // a check still running changes nothing
	n.checking = false
	c.recordNetworkLocked(reason)
	c.deliverNetworkLocked()
	c.armNetworkLocked()
	c.attributeDueLocked()
}

// Network returns the state of the local network and since when.
func (c *Coordinator) Network() (NetworkState, time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.network.state, c.network.since
}

func shortText(text string) string {
	if len(text) > 200 {
		return text[:200]
	}
	return text
}
