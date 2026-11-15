package probe

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/iface"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func sample(elapsed time.Duration) hopSample {
	return hopSample{elapsed: elapsed}
}

func timedOut() hopSample {
	return hopSample{elapsed: LocalCheckTimeout, err: errors.New("i/o timeout")}
}

func entry(samples ...hopSample) []hopSample {
	return samples
}

func TestTheVerdictOfACheck(t *testing.T) {
	ms := time.Millisecond
	clean := entry(sample(45*ms), sample(46*ms), sample(45*ms), sample(47*ms))
	lossy := entry(sample(45*ms), sample(1045*ms), sample(45*ms), sample(1045*ms))
	once := entry(sample(45*ms), sample(1045*ms), sample(45*ms), sample(45*ms))
	twice := entry(sample(45*ms), sample(1045*ms), sample(1045*ms), sample(45*ms))
	out := entry(timedOut(), timedOut(), timedOut(), timedOut())
	slow := entry(sample(1700*ms), sample(1700*ms), sample(1700*ms), sample(1700*ms))
	later := func(extra time.Duration) []hopSample {
		return entry(sample(45*ms), sample(45*ms+extra), sample(45*ms), sample(45*ms))
	}
	local := hopSample{elapsed: ms, err: errors.New("connect: network is unreachable")}
	// the first connection failed locally, the ones after it got through
	outFirst := entry(local, sample(45*ms), sample(45*ms), sample(45*ms))

	for _, c := range []struct {
		name    string
		samples [][]hopSample
		out     bool
		result  string
		clean   bool
	}{
		{"no entry", nil, false, checkInsufficient, false},
		{"one entry tells nothing", [][]hopSample{lossy}, false, checkInsufficient, false},
		{"one entry out tells nothing", [][]hopSample{out}, false, checkInsufficient, false},
		{"one of two reached tells nothing", [][]hopSample{clean, out}, false, checkInsufficient, false},
		{"two entries out", [][]hopSample{out, out}, false, "down", false},
		{"two of three out", [][]hopSample{out, out, clean}, false, "poor", false},
		{"one of three out is the proxy's", [][]hopSample{out, clean, clean}, false, "normal", true},
		{"scattered loss", [][]hopSample{lossy, lossy, clean}, false, "poor", false},
		{"one entry losing is the proxy's", [][]hopSample{twice, clean, clean}, false, "normal", false},
		{"one late connection is clean", [][]hopSample{once, clean, clean}, false, "normal", true},
		{"all slow", [][]hopSample{slow, slow, slow}, false, "poor", false},
		{"one slow", [][]hopSample{slow, clean, clean}, false, "normal", true},
		{"all clean", [][]hopSample{clean, clean, clean}, false, "normal", true},
		{"699 ms later is no bad sample", [][]hopSample{later(699 * ms), later(699 * ms)}, false, "normal", true},
		{"700 ms later is a bad sample", [][]hopSample{later(700 * ms), later(700 * ms)}, false, "poor", false},
		{"701 ms later is a bad sample", [][]hopSample{later(701 * ms), later(701 * ms)}, false, "poor", false},
		{"every first connection failed locally", [][]hopSample{outFirst, outFirst}, true, "down", false},
	} {
		v := judge(c.samples, c.out)
		require.Equal(t, c.result, v.result, c.name)
		require.Equal(t, c.clean, v.clean, c.name)
	}
}

// lossySample is a connection over a network that loses each SYN round trip with probability
// loss: retransmitted after one second, then two, until the timeout.
func lossySample(r *rand.Rand, loss float64) hopSample {
	const rtt = 45 * time.Millisecond
	// tries at 0, 1 s and 3 s: the last is past the timeout
	for at := time.Duration(0); at < LocalCheckTimeout; at = 2*at + time.Second {
		if r.Float64() >= loss {
			return sample(at + rtt)
		}
	}
	return timedOut()
}

func poorShare(entries int, loss float64, rounds int) float64 {
	r := rand.New(rand.NewSource(1))
	poor := 0
	for i := 0; i < rounds; i++ {
		samples := make([][]hopSample, entries)
		for j := range samples {
			for k := 0; k < LocalSamples; k++ {
				samples[j] = append(samples[j], lossySample(r, loss))
			}
		}
		if judge(samples, false).result == "poor" {
			poor++
		}
	}
	return float64(poor) / float64(rounds)
}

func TestTheVerdictTellsScatteredLossFromANormalNetwork(t *testing.T) {
	require.GreaterOrEqual(t, poorShare(3, 0.30, 2000), 0.70, "three entries losing 30%")
	require.LessOrEqual(t, poorShare(3, 0.01, 2000), 0.005, "three entries losing 1%")
	require.Zero(t, poorShare(1, 0.45, 500), "one entry tells nothing")
}

func TestTheEntriesOfACheckAreAsIndependentAsTheyCanBe(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	// the six found alive last are all of pool p
	var nodes []C.Proxy
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5", "p6"} {
		nodes = append(nodes, pooledNode(name, "p", ""))
	}
	q, r, gone := pooledNode("q", "q", ""), pooledNode("r", "r", ""), pooledNode("gone", "x", "")
	g := newRecorder("g", append(nodes, q, r, gone)...)
	reg := activeRegistration(c, g)
	for _, p := range []C.Proxy{q, r, gone} {
		runner.set(p, true)
	}
	testOnce(t, reg, g, 1, q, r, gone)
	clock.Advance(time.Second)
	for _, p := range nodes {
		runner.set(p, true)
	}
	testOnce(t, reg, g, 2, nodes...)
	c.exclusions.Replace([]ExclusionEntry{{Pool: "x"}})

	c.mutex.Lock()
	candidates := c.checkCandidatesLocked()
	c.mutex.Unlock()
	var names []string
	for _, cand := range candidates {
		names = append(names, cand.p.Name())
	}
	require.Len(t, names, LocalCandidates)
	require.Contains(t, names, "q", "the pools take turns")
	require.Contains(t, names, "r")
	require.NotContains(t, names, "gone", "excluded")

	// q shares its address with the node of p taken first, at another port: one entry, counted
	// once
	shared := netip.AddrPortFrom(entryOf(candidates[0].p).Addr(), 8443)
	resolve := func(_ context.Context, p C.Proxy) (netip.AddrPort, error) {
		switch p {
		case q:
			return shared, nil
		case r:
			return netip.AddrPort{}, errors.New("no address")
		}
		return entryOf(p), nil
	}
	targets, tried := selectTargets(candidates, resolve)
	require.Len(t, targets, 1, "p counts once, q shares its address, r does not resolve")
	skipped := map[string]string{}
	for _, raw := range tried {
		e := raw.(map[string]any)
		if reason, ok := e["skipped"].(string); ok {
			skipped[e["name"].(string)] = reason
		}
	}
	require.Equal(t, "address", skipped["q"])
	require.Equal(t, "resolve", skipped["r"])
}

func networkChecks(c *Coordinator) int {
	n := 0
	for _, raw := range c.Records().Events(0, 10000).Events {
		if strings.Contains(string(raw), `"event":"networkCheck"`) {
			n++
		}
	}
	return n
}

func networkReasons(c *Coordinator) []string {
	var reasons []string
	for _, raw := range c.Records().Events(0, 10000).Events {
		text := string(raw)
		if !strings.Contains(text, `"event":"network"`) {
			continue
		}
		for _, reason := range []string{"recovered", "unconfirmed", "idle", "noTargets", "success", "check", "extended"} {
			if strings.Contains(text, `"reason":"`+reason+`"`) {
				reasons = append(reasons, reason)
			}
		}
	}
	return reasons
}

// twoEntries makes a and b alive, the entries of the checks.
func twoEntries(t *testing.T, c *Coordinator, runner *fakeRunner, extra ...C.Proxy) (*Registration, *recorder, C.Proxy, C.Proxy) {
	t.Helper()
	a, b := node("a"), node("b")
	g := newRecorder("g", append([]C.Proxy{a, b}, extra...)...)
	reg := activeRegistration(c, g)
	runner.set(a, true)
	runner.set(b, true)
	testOnce(t, reg, g, 1, a, b)
	return reg, g, a, b
}

func TestASingleNewFailureCallsForACheck(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	setHops(c, fastHop)
	x, y := node("x"), node("y")
	reg, g, _, _ := twoEntries(t, c, runner, x, y)
	require.Zero(t, networkChecks(c), "successes call for no check")
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x)
	settle(t, c)
	require.Equal(t, 1, networkChecks(c), "one member failing is enough")
	require.Equal(t, NetworkNormal, networkState(c))

	// another failure within the interval waits for it
	clock.Advance(10 * time.Second)
	testOnce(t, reg, g, 3, y)
	settle(t, c)
	require.Equal(t, 1, networkChecks(c))
	clock.Advance(CheckInterval)
	recheck(t, c)
	require.Equal(t, 2, networkChecks(c), "the failure waiting is checked once the interval is over")
	// a test failing again tells nothing new
	clock.Advance(BackoffMax)
	testOnce(t, reg, g, 4, x)
	recheck(t, c)
	require.Equal(t, 2, networkChecks(c))
}

// switchHop answers as lossy or clean entries, as told.
type switchHop struct {
	mutex sync.Mutex
	mode  string // lossy, clean, half (one entry out), out
	count map[C.Proxy]int
	first C.Proxy
}

func (s *switchHop) set(mode string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.mode = mode
}

func (s *switchHop) dial(p C.Proxy) (time.Duration, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.count == nil {
		s.count = map[C.Proxy]int{}
	}
	if s.first == nil {
		s.first = p
	}
	s.count[p]++
	switch s.mode {
	case "lossy":
		if s.count[p]%2 == 0 {
			return 1045 * time.Millisecond, nil
		}
	case "half":
		if p == s.first {
			return unreachableHop(p)
		}
	case "out":
		return unreachableHop(p)
	}
	return 45 * time.Millisecond, nil
}

func poorNetwork(t *testing.T) (*Coordinator, *fakeClock, *switchHop, C.Proxy) {
	t.Helper()
	c, runner, clock := newClockedCoordinator(t)
	hops := &switchHop{mode: "lossy"}
	setHops(c, hops.dial)
	x := node("x")
	reg, g, _, _ := twoEntries(t, c, runner, x)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x)
	settle(t, c)
	require.Equal(t, NetworkPoor, networkState(c))
	return c, clock, hops, x
}

func TestAPoorNetworkEndsAfterCleanChecksInARow(t *testing.T) {
	c, clock, hops, x := poorNetwork(t)
	hops.set("clean")
	again := func() {
		clock.Advance(CheckInterval)
		c.NoteDialFailure(x, clock.Now())
		recheck(t, c)
	}
	again()
	again()
	require.Equal(t, NetworkPoor, networkState(c), "two clean checks are not enough")
	hops.set("half")
	again()
	hops.set("clean")
	again()
	again()
	require.Equal(t, NetworkPoor, networkState(c), "a check that tells nothing breaks the row")
	again()
	require.Equal(t, NetworkNormal, networkState(c))
	require.Contains(t, networkReasons(c), "recovered")
}

func TestANetworkNotConfirmedAgainEnds(t *testing.T) {
	c, clock, hops, x := poorNetwork(t)
	hops.set("half")
	for i := 0; i < 9; i++ {
		clock.Advance(CheckInterval)
		c.NoteDialFailure(x, clock.Now())
		recheck(t, c)
		require.Equal(t, NetworkPoor, networkState(c))
	}
	clock.Advance(LocalConfirm - 9*CheckInterval)
	require.Equal(t, NetworkNormal, networkState(c))
	require.Contains(t, networkReasons(c), "unconfirmed")
}

func TestANetworkNothingUsesEnds(t *testing.T) {
	c, clock, _, _ := poorNetwork(t)
	for i := 0; i < 4; i++ {
		clock.Advance(2 * time.Minute)
		recheck(t, c)
		require.Equal(t, NetworkPoor, networkState(c), "the checks confirm it")
	}
	clock.Advance(2 * time.Minute)
	require.Equal(t, NetworkNormal, networkState(c))
	require.Contains(t, networkReasons(c), "idle")
}

func TestASuccessEndingTheNetworkDownLetsTheCheckRunningGoOn(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	var mutex sync.Mutex
	var gate chan struct{}
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		mutex.Lock()
		wait := gate
		mutex.Unlock()
		if wait != nil {
			<-wait
		}
		return unreachableHop(p)
	})
	x, y, z := node("x"), node("y"), node("z")
	reg, g, a, _ := twoEntries(t, c, runner, x, y, z)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	settle(t, c)
	require.Equal(t, NetworkDown, networkState(c))

	// a check is running when a new connection receives data
	mutex.Lock()
	gate = make(chan struct{})
	mutex.Unlock()
	clock.Advance(DownInterval)
	c.kick()
	c.mutex.Lock()
	require.True(t, c.network.checking)
	c.mutex.Unlock()
	c.NoteNewSuccess(a, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c), "the throttle is lifted at once")
	close(gate)
	settle(t, c)
	require.Equal(t, NetworkPoor, networkState(c), "its entries all out while something works")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.Equal(t, c.network.lastEnd(), c.network.start, "the coverage goes on from where it ended")
}

func TestAnOutageAWitnessOfWhichTheNetworkExplainsDoesNotCount(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	hops := &switchHop{mode: "clean"}
	setHops(c, hops.dial)
	a, b, w := pooledNode("pa", "jk", ""), pooledNode("pb", "jk", ""), pooledNode("pw", "jk", "")
	reg, g, alive, _ := twoEntries(t, c, runner, a, b, w)
	clock.Advance(time.Second)
	// a and b fail: the pool seems out, and the check finds the network normal
	testOnce(t, reg, g, 2, a, b)
	settle(t, c)
	require.Len(t, c.Outages(), 1)
	require.Equal(t, NetworkNormal, networkState(c))

	// w fails while the network is out: a witness of the outage the check explains
	clock.Advance(CheckInterval)
	hops.set("out")
	testOnce(t, reg, g, 3, w)
	settle(t, c)
	require.Equal(t, NetworkDown, networkState(c))
	require.Len(t, c.Outages(), 1, "it began before the network was out")
	// noted on the outage, which keeps it whatever is forgotten before it counts
	c.mutex.Lock()
	_, covered := c.outages.active[poolKey{"jk", testCond}].covered[Key{w, testCond}]
	c.mutex.Unlock()
	require.True(t, covered)

	clock.Advance(time.Second)
	c.NoteNewSuccess(alive, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c))
	runner.set(a, true)
	testOnce(t, reg, g, 4, a)
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageEstablished }))

	// the failure of a waits for the outage to be attributed
	var exec uint64
	for _, done := range g.completions() {
		if done.Key.Target == a && !done.Result.Satisfied {
			exec = done.Exec
		}
	}
	started, pools, unknown, ok := c.FailureRoute(Key{a, testCond}, exec)
	require.True(t, ok)
	pool, at, waiting := c.Waiting(started, pools, testCond, unknown)
	require.True(t, waiting)
	require.Equal(t, "jk", pool)
	require.False(t, at.IsZero(), "established, it is attributed at a known time")

	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.Committed && !o.Counted }))
	outcomes := map[string]string{}
	for _, at := range g.attributed() {
		outcomes[at.Key.Target.Name()] = at.Outcome
	}
	require.Equal(t, map[string]string{"pa": OutcomeMember, "pb": OutcomeMember, "pw": OutcomeLocal}, outcomes)
}

func TestANodeDeadBeforeAnOutageTellsNothingOfIt(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	u, a, b := pooledNode("u", "jk", ""), pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", u, a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, u)
	clock.Advance(OutageWindow + time.Minute)
	testOnce(t, reg, g, 2, a, b)
	require.Len(t, c.Outages(), 1)

	runner.set(u, true)
	testOnce(t, reg, g, 3, u)
	require.Len(t, c.Outages(), 1, "u was dead before the outage: its coming back tells nothing")

	runner.set(a, true)
	clock.Advance(BackoffFirst)
	testOnce(t, reg, g, 4, a)
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageEstablished }))
}

func TestATestStartedBeforeAnOutageIsNoWitnessOfIt(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	a, b, w := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("w", "jk", "")
	g := newRecorder("g", a, b, w)
	reg := activeRegistration(c, g)
	hold := runner.holdOn(w)
	started := clock.Now()
	verify := func(p C.Proxy, at time.Time) Need {
		return Need{Target: p, Cond: testCond, Class: ClassVerify, Source: SourceVerify, NotBefore: at}
	}
	reg.Submit(1, []Need{verify(w, started)})
	require.Eventually(t, func() bool { return runner.count(w) == 1 }, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	before := len(g.completions())
	reg.Submit(2, []Need{verify(w, started), verify(a, clock.Now()), verify(b, clock.Now())})
	require.Eventually(t, func() bool { return len(g.completions()) >= before+2 }, time.Second, time.Millisecond)
	require.Len(t, c.Outages(), 1)

	// w fails late, its test having started before the outage
	close(hold)
	require.Eventually(t, func() bool {
		for _, done := range g.completions() {
			if done.Key.Target == w {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	o := c.outages.active[poolKey{"jk", testCond}]
	require.NotNil(t, o)
	_, dead := o.dead[Key{w, testCond}]
	_, witness := o.witness[Key{w, testCond}]
	require.True(t, dead, "it is tested again with the others")
	require.False(t, witness)
}

func TestAFailureTheNetworkExplainsSuggestsNoOutageLater(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	hops := &switchHop{mode: "out"}
	setHops(c, hops.dial)
	a, b := pooledNode("pa", "jk", ""), pooledNode("pb", "jk", "")
	reg, g, alive, _ := twoEntries(t, c, runner, a, b)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, a)
	settle(t, c)
	require.Equal(t, NetworkDown, networkState(c))
	hops.set("clean")
	clock.Advance(time.Second)
	c.NoteNewSuccess(alive, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c))

	// b fails for real a minute later: with a, whose failure the network explains, no outage
	clock.Advance(time.Minute)
	testOnce(t, reg, g, 3, b)
	settle(t, c)
	require.Empty(t, c.Outages())
	clock.Advance(AttributionDelay)
	c.attributeNow()
	outcomes := map[string]string{}
	for _, at := range g.attributed() {
		outcomes[at.Key.Target.Name()] = at.Outcome
	}
	require.Equal(t, map[string]string{"pa": OutcomeLocal, "pb": OutcomeMember}, outcomes)
}

func TestTheLocalStackFailingIsALocalError(t *testing.T) {
	require.True(t, localError(fmt.Errorf("dial: %w", iface.ErrIfaceNotFound)))
	require.True(t, localError(fmt.Errorf("dial: %w", iface.ErrAddrNotFound)))
	require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ENETUNREACH)}))
	require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("setsockopt", syscall.ENODEV)}))
	require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ENETDOWN)}))
	if runtime.GOOS == "windows" {
		require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connectex", syscall.Errno(10051))}))
		require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connectex", syscall.Errno(10049))}))
		require.True(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connectex", syscall.Errno(10050))}))
	}
	require.False(t, localError(errors.New("i/o timeout")))
	require.False(t, localError(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNRESET)}))
}

func TestOneTestCallsForOneCheck(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	setHops(c, fastHop)
	x := node("x")
	twoEntries(t, c, runner, x)
	c.mutex.Lock()
	// a test that turns a third member dead, and is found under another condition
	c.demandLocked(clock.Now(), 7, "failure", "down")
	c.demandLocked(clock.Now(), 7, "failure")
	waiting := !c.network.pending.empty()
	c.mutex.Unlock()
	require.False(t, waiting, "the check running explains it")
	settle(t, c)
	// another group reports the degradation the same test found
	c.NoteDegradation(x, 7, clock.Now())
	clock.Advance(CheckInterval)
	recheck(t, c)
	require.Equal(t, 1, networkChecks(c))
	// another test does call for one
	c.NoteDegradation(x, 8, clock.Now())
	settle(t, c)
	require.Equal(t, 2, networkChecks(c))
}

func TestAFailureThatStartedLongAgoSuggestsNoNetworkDown(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	x, y, z := node("x"), node("y"), node("z")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.noteFailureLocked(x, clock.Now())
	c.noteFailureLocked(y, clock.Now())
	_, down := c.noteFailureLocked(z, clock.Now().Add(-5*time.Minute))
	require.False(t, down, "a test that started five minutes ago, ending now")
	first, down := c.noteFailureLocked(z, clock.Now().Add(-10*time.Second))
	require.True(t, down)
	require.Equal(t, clock.Now().Add(-10*time.Second), first)
}

func TestOnlyFailuresAfterTheLastSuccessSuggestTheNetworkDown(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, cc, d := node("a"), node("b"), node("c"), node("d")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.noteFailureLocked(a, clock.Now())
	clock.Advance(time.Second)
	c.noteSuccessLocked(clock.Now(), false)
	early := clock.Now() // started before the success was heard, ending after it
	clock.Advance(time.Second)
	c.noteFailureLocked(b, clock.Now())
	from := clock.Now()
	c.noteFailureLocked(cc, clock.Now())
	_, down := c.noteFailureLocked(a, early)
	require.False(t, down)
	first, down := c.noteFailureLocked(d, clock.Now())
	require.True(t, down, "three members failed since the success")
	require.Equal(t, from, first)
	_, down = c.noteFailureLocked(node("e"), early)
	require.False(t, down, "a failure that started before the success calls for no check again")
}

func TestTheAnomaliesAbsorbedByANetworkNotNormalAreExplained(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	gate := make(chan struct{})
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		<-gate
		return unreachableHop(p)
	})
	x := node("x")
	_, _, alive, _ := twoEntries(t, c, runner, x)
	clock.Advance(time.Second)
	c.NoteDegradation(x, 8, clock.Now())
	require.Eventually(t, func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.network.checking
	}, time.Second, time.Millisecond)
	// another test is found degraded while the check runs: it waits for the next
	c.NoteDegradation(x, 9, clock.Now())
	close(gate)
	settle(t, c)
	require.Equal(t, NetworkDown, networkState(c))
	clock.Advance(time.Second)
	c.NoteNewSuccess(alive, clock.Now())
	require.Equal(t, NetworkNormal, networkState(c))
	// another group reports what the check explained
	c.NoteDegradation(x, 9, clock.Now())
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.True(t, c.network.pending.empty())
}

func TestFirstDataHeardAtOnceTellsTheNetworkIsNotDown(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	release := make(chan struct{})
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		<-release
		return unreachableHop(p)
	})
	x, y, z := node("x"), node("y"), node("z")
	reg, g, _, _ := twoEntries(t, c, runner, x, y, z)
	clock.Advance(time.Second)
	testOnce(t, reg, g, 2, x, y, z)
	require.Eventually(t, func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.network.checking
	}, time.Second, time.Millisecond)
	clock.Advance(time.Second)
	// a connection dialed during the check receives data; its group hands it on only later
	c.NoteFirstData(clock.Now().UnixNano())
	close(release)
	settle(t, c)
	require.Equal(t, NetworkPoor, networkState(c))
}

func TestALateFailureWithinAnOutageNotAttributedYetWitnessesIt(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, w := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("w", "jk", "")
	start := clock.Now()
	c.mutex.Lock()
	c.poolFailureLocked(Key{a, testCond}, start)
	c.poolFailureLocked(Key{b, testCond}, start.Add(time.Second))
	o := c.outages.active[poolKey{"jk", testCond}]
	require.NotNil(t, o)
	// the network was out a while in it
	c.network.past = []interval{{start: start.Add(2 * time.Second), end: start.Add(time.Minute)}}
	clock.Advance(time.Minute)
	c.concludeOutageLocked(o, OutageEstablished, nil)
	// w failed then, its result coming once the outage is over
	c.poolFailureLocked(Key{w, testCond}, start.Add(3*time.Second))
	_, covered := o.covered[Key{w, testCond}]
	c.mutex.Unlock()
	require.True(t, covered)
	clock.Advance(AttributionDelay)
	c.attributeNow()
	require.Equal(t, Counts{}, c.PoolCounts("jk", testCond))
}

func TestANodeBackEndsItsRecentFailure(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	clock.Advance(time.Minute)
	c.poolAliveLocked(Key{a, testCond}, clock.Now(), 1)
	clock.Advance(time.Minute)
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	require.Empty(t, c.outages.active, "a failed and came back before b failed")
}

func TestWhatAFormerNodeTellsDependsOnWhenItsTestStarted(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	u, a, b := pooledNode("u", "jk", ""), pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	uKey := Key{u, testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.evidence[uKey] = &Evidence{State: Dead, DeadSince: clock.Now(), NextAllowed: clock.Now().Add(time.Hour)}
	clock.Advance(OutageWindow + time.Minute)
	first := clock.Now()
	c.poolFailureLocked(Key{a, testCond}, first)
	c.poolFailureLocked(Key{b, testCond}, first)
	o := c.outages.active[poolKey{"jk", testCond}]
	require.True(t, o.former[uKey])

	// a success of u its evidence does not take keeps it in the recovery tests
	clock.Advance(time.Second)
	between := clock.Now()
	c.poolAliveLocked(uKey, between, 1)
	_, dead := o.dead[uKey]
	require.True(t, dead)
	require.Same(t, o, c.outages.active[poolKey{"jk", testCond}])

	// u fails again in the outage: a success that started before tells nothing still, nor
	// does a connection dialed before, which leaves it in the recovery tests
	clock.Advance(time.Second)
	c.poolFailureLocked(uKey, clock.Now())
	c.poolAliveLocked(uKey, between, 2)
	c.poolSuccessLocked(u, between, false)
	require.Same(t, o, c.outages.active[poolKey{"jk", testCond}], "no disproof")
	_, dead = o.dead[uKey]
	require.True(t, dead)

	// a connection dialed since stands for the outage, whose end clears the backoff of u
	clock.Advance(time.Second)
	c.poolSuccessLocked(u, clock.Now(), false)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageEstablished, o.state)
	require.Equal(t, clock.Now(), c.evidence[uKey].NextAllowed)
}

func TestASuccessOfThePoolHidesOnlyTheFailuresBeforeIt(t *testing.T) {
	for _, connection := range []bool{false, true} {
		c, _, clock := newClockedCoordinator(t)
		a, h, b, cc := pooledNode("a", "jk", ""), pooledNode("h", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
		c.mutex.Lock()
		c.evidence[Key{a, testCond}] = &Evidence{State: Dead, DeadSince: clock.Now()}
		c.poolFailureLocked(Key{a, testCond}, clock.Now())
		clock.Advance(time.Minute)
		if connection {
			c.poolSuccessLocked(h, clock.Now(), false)
		} else {
			c.evidence[Key{h, testCond}] = &Evidence{State: Alive, Started: clock.Now()}
			c.poolAliveLocked(Key{h, testCond}, clock.Now(), 1)
		}
		clock.Advance(2 * time.Minute)
		c.poolFailureLocked(Key{b, testCond}, clock.Now())
		clock.Advance(time.Minute)
		c.poolFailureLocked(Key{cc, testCond}, clock.Now())
		o := c.outages.active[poolKey{"jk", testCond}]
		require.NotNil(t, o, "connection %v", connection)
		require.Len(t, o.witness, 2)
		require.Nil(t, o.witness[Key{a, testCond}])
		require.True(t, o.former[Key{a, testCond}], "a dead since before takes part")
		c.mutex.Unlock()
	}
}

func TestAFailureWithinAnOutageConcludedSuggestsNoOther(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, d, e := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("d", "jk", ""), pooledNode("e", "jk", "")
	pk := poolKey{"jk", testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	start := clock.Now()
	c.poolFailureLocked(Key{a, testCond}, start)
	c.poolFailureLocked(Key{b, testCond}, start)
	clock.Advance(time.Minute)
	c.concludeOutageLocked(c.outages.active[pk], OutageDisproved, nil)
	// d failed before it was disproved, its result coming after; e fails after
	c.poolFailureLocked(Key{d, testCond}, start.Add(30*time.Second))
	clock.Advance(time.Minute)
	c.poolFailureLocked(Key{e, testCond}, clock.Now())
	require.Empty(t, c.outages.active)

	// in an outage established, it witnesses it, and is one that failed before in the next
	c, _, clock = newClockedCoordinator(t)
	f := pooledNode("f", "jk", "")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	start = clock.Now()
	c.poolFailureLocked(Key{a, testCond}, start)
	c.poolFailureLocked(Key{b, testCond}, start)
	clock.Advance(time.Minute)
	first := c.outages.active[pk]
	c.concludeOutageLocked(first, OutageEstablished, nil)
	clock.Advance(time.Second)
	c.poolFailureLocked(Key{d, testCond}, clock.Now())
	c.poolFailureLocked(Key{e, testCond}, clock.Now())
	second := c.outages.active[pk]
	require.NotNil(t, second)
	c.poolFailureLocked(Key{f, testCond}, start.Add(30*time.Second))
	require.NotNil(t, first.witness[Key{f, testCond}])
	require.True(t, second.former[Key{f, testCond}])
	_, dead := second.dead[Key{f, testCond}]
	require.True(t, dead)
}

func TestWhatASuccessTellsOfAnOutageIsJudgedOnceTheEarlierTestsAreIn(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, cc := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	pk := poolKey{"jk", testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[pk]
	// a test of c starts, then a connection through c receives data
	clock.Advance(time.Second)
	running := &execution{id: 60, key: Key{cc, testCond}, class: ClassVerify, started: clock.Now()}
	c.inflight[running] = struct{}{}
	clock.Advance(time.Second)
	c.poolSuccessLocked(cc, clock.Now(), false)
	require.Same(t, o, c.outages.active[pk], "it waits for the test of c")
	// the test finds c dead: c witnessed the outage, and its connection after stands for it
	delete(c.inflight, running)
	c.poolFailureLocked(Key{cc, testCond}, running.started)
	c.settleVerdictLocked(o)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageEstablished, o.state)
}

func TestASuccessUnderALongerTimeoutTellsOfThePool(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, cc := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	longer := testCond
	longer.Timeout = 2 * testTimeout(testCond)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[poolKey{"jk", testCond}]
	clock.Advance(time.Second)
	exec := &execution{id: 70, key: Key{cc, longer}, class: ClassVerify, started: clock.Now()}
	c.poolUnderLocked(exec, C.ProbeResult{Satisfied: true, Elapsed: time.Second}, false)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageDisproved, o.state)
}

func TestANewFailureUnderALongerTimeoutWitnessesTheOutageOfThePool(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, h := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("h", "jk", "")
	longer := testCond
	longer.Timeout = 2 * testTimeout(testCond)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[poolKey{"jk", testCond}]
	// h, read under the longer timeout only, fails at once, then works
	clock.Advance(time.Second)
	failed := &execution{id: 80, key: Key{h, longer}, class: ClassVerify, started: clock.Now()}
	c.poolUnderLocked(failed, C.ProbeResult{Elapsed: time.Millisecond, ErrorClass: C.ProbeErrorDial}, true)
	require.NotNil(t, o.witness[Key{h, testCond}])
	clock.Advance(time.Second)
	back := &execution{id: 81, key: Key{h, longer}, class: ClassVerify, started: clock.Now()}
	c.poolUnderLocked(back, C.ProbeResult{Satisfied: true, Elapsed: time.Millisecond}, false)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageEstablished, o.state)
}

func TestTheLastSuccessOfAPoolStaysWhenItsNodeFailsAfter(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, cc := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("c", "jk", "")
	pk := poolKey{"jk", testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.evidence[Key{a, testCond}] = &Evidence{State: Dead, DeadSince: clock.Now()}
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	clock.Advance(2 * time.Minute)
	c.poolAliveLocked(Key{b, testCond}, clock.Now(), 1)
	// b fails later: what it found when it worked stays
	clock.Advance(2 * time.Minute)
	c.evidence[Key{b, testCond}] = &Evidence{State: Dead, DeadSince: clock.Now()}
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	require.Empty(t, c.outages.active, "a failed before the pool worked")
	clock.Advance(time.Minute)
	c.poolFailureLocked(Key{cc, testCond}, clock.Now())
	o := c.outages.active[pk]
	require.NotNil(t, o)
	require.Len(t, o.witness, 2)
	require.True(t, o.former[Key{a, testCond}])
}

func TestASuccessThatTellsNothingIsLetGo(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, w := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("w", "jk", "")
	pk := poolKey{"jk", testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// a test of w, unknown then, starts before a and b fail
	running := &execution{id: 90, key: Key{w, testCond}, class: ClassVerify, started: clock.Now()}
	c.inflight[running] = struct{}{}
	clock.Advance(time.Second)
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[pk]
	// a connection through w works: whether it disproves the outage waits for that test
	clock.Advance(time.Second)
	c.poolSuccessLocked(w, clock.Now(), false)
	require.Same(t, o, c.outages.active[pk])
	// w was dead before the outage: its connection tells nothing, and is let go
	delete(c.inflight, running)
	c.evidence[Key{w, testCond}] = &Evidence{State: Dead, DeadSince: running.started}
	c.poolFailureLocked(Key{w, testCond}, running.started)
	c.settleVerdictLocked(o)
	require.Same(t, o, c.outages.active[pk])
	require.Empty(t, o.verdicts)
	// with its nodes gone and nothing waiting, it cannot be concluded
	c.pruneOutagesLocked(map[Key]struct{}{})
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageUndecided, o.state)
}

func TestAnOutageLeftWithASuccessWaitingEndsOnceItTellsNothing(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, w := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("w", "jk", "")
	wKey := Key{w, testCond}
	pk := poolKey{"jk", testCond}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	running := &execution{id: 95, key: wKey, class: ClassVerify, started: clock.Now()}
	c.inflight[running] = struct{}{}
	clock.Advance(time.Second)
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[pk]
	clock.Advance(time.Second)
	c.poolSuccessLocked(w, clock.Now(), false)
	// a and b are removed while the success through w waits for the test of w
	c.pruneOutagesLocked(map[Key]struct{}{wKey: {}})
	require.Same(t, o, c.outages.active[pk])
	// w was dead before: its connection tells nothing, takes it out of the recovery tests, and
	// leaves nothing to conclude the outage
	delete(c.inflight, running)
	c.evidence[wKey] = &Evidence{State: Dead, DeadSince: running.started}
	c.poolFailureLocked(wKey, running.started)
	c.settleVerdictLocked(o)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageUndecided, o.state)
	_, dead := o.dead[wKey]
	require.False(t, dead)
}

func TestAPoolThatMayStillCountWaitsWhateverTheOrderOfTheRoute(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	start := clock.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.outages.past = append(c.outages.past, &outage{key: poolKey{"A", testCond}, state: OutageUndecided, first: start, end: start.Add(time.Hour)})
	c.outages.active[poolKey{"B", testCond}] = &outage{key: poolKey{"B", testCond}, state: OutageSuspected, first: start}
	at := start.Add(time.Minute)
	for _, pools := range [][]string{{"A", "B"}, {"B", "A"}} {
		a := c.attributionLocked(at, pools, testCond, false)
		require.True(t, a.Wait, "%v", pools)
		require.Equal(t, "B", a.Pool)
	}
}

func TestAMemberFailingOverAndOverIsNotedOnceASecond(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	x := node("x")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i := 0; i < 1000; i++ {
		c.noteFailureLocked(x, clock.Now())
	}
	require.Len(t, c.network.failures, 1)
}

func TestAPoolWhoseOutageCountedExplainsAFailureWhateverTheOrderOfTheRoute(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	start := clock.Now()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.outages.active[poolKey{"A", testCond}] = &outage{key: poolKey{"A", testCond}, state: OutageSuspected, first: start}
	c.outages.past = append(c.outages.past, &outage{key: poolKey{"B", testCond}, state: OutageEstablished, first: start, end: start.Add(time.Hour), committed: true, counted: true})
	at := start.Add(time.Minute)
	for _, pools := range [][]string{{"A", "B"}, {"B", "A"}} {
		a := c.attributionLocked(at, pools, testCond, false)
		require.Equal(t, OutcomePool, a.Outcome, "%v", pools)
		require.Equal(t, "B", a.Pool)
	}
}

func TestATestExplainedWhileTheNetworkIsNotNormalCallsForNoCheckAfter(t *testing.T) {
	c, clock, _, _ := poorNetwork(t)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.demandLocked(clock.Now(), 9, "degradation")
	c.endNetworkLocked("recovered", clock.Now())
	c.demandLocked(clock.Now(), 9, "degradation")
	require.True(t, c.network.pending.empty())
}

func TestTheSamplerTellsEveryFirstConnectionFailedLocally(t *testing.T) {
	var mutex sync.Mutex
	calls := map[C.Proxy]int{}
	dial := func(_ context.Context, p C.Proxy, _ netip.AddrPort) (time.Duration, error) {
		mutex.Lock()
		calls[p]++
		first := calls[p] == 1
		mutex.Unlock()
		if first {
			// the first connections fail late, once the later ones got through
			time.Sleep(50 * time.Millisecond)
			return time.Millisecond, errors.New("connect: network is unreachable")
		}
		return time.Millisecond, nil
	}
	x, y := node("x"), node("y")
	targets := []hopTarget{
		{hopCandidate: hopCandidate{p: x}, addr: entryOf(x)},
		{hopCandidate: hopCandidate{p: y}, addr: entryOf(y)},
	}
	samples, out := sampleTargets(targets, dial, time.Millisecond)
	require.True(t, out)
	require.Equal(t, NetworkDown.String(), judge(samples, out).result)
}

func TestAnOutageWaitsForATestOfItsPoolUnderAnotherTimeout(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b, h := pooledNode("a", "jk", ""), pooledNode("b", "jk", ""), pooledNode("h", "jk", "")
	longer := testCond
	longer.Timeout = 3 * testTimeout(testCond)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[poolKey{"jk", testCond}]
	clock.Advance(time.Second)
	running := &execution{id: 50, key: Key{h, longer}, class: ClassVerify, started: clock.Now()}
	c.inflight[running] = struct{}{}
	clock.Advance(time.Second)
	c.offerVerdictLocked(o, Key{a, testCond}, clock.Now(), 51, nil)
	require.Same(t, o, c.outages.active[poolKey{"jk", testCond}], "the test of h may still disprove it")

	// its witnesses removed, it still waits for that test
	c.pruneOutagesLocked(map[Key]struct{}{{h, testCond}: {}})
	require.Same(t, o, c.outages.active[poolKey{"jk", testCond}])
	delete(c.inflight, running)
	c.settleVerdictLocked(o)
	require.Empty(t, c.outages.active)
	require.Equal(t, OutageEstablished, o.state)
}

func TestOnlyATestOfANodeDeadWhenItStartedStandsForAnOutage(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// a maintenance test of a, unknown then, still runs when a and b are found dead
	old := c.startLocked(Key{a, testCond}, ClassMaintain, clock.Now())
	require.False(t, old.recovery)
	c.inflight[old] = struct{}{}
	clock.Advance(time.Second)
	c.evidence[Key{a, testCond}] = &Evidence{State: Dead, DeadSince: clock.Now()}
	c.poolFailureLocked(Key{a, testCond}, clock.Now())
	c.poolFailureLocked(Key{b, testCond}, clock.Now())
	o := c.outages.active[poolKey{"jk", testCond}]
	require.Nil(t, c.recoveringLocked(o))
}

func TestNoCheckRunsWhileAConfigIsPublished(t *testing.T) {
	c, runner, clock := newClockedCoordinator(t)
	setHops(c, fastHop)
	twoEntries(t, c, runner)
	c.mutex.Lock()
	c.publishing = true
	c.demandLocked(clock.Now(), 0, "failure")
	checking := c.network.checking
	c.mutex.Unlock()
	require.False(t, checking)
	require.Zero(t, networkChecks(c))
	c.mutex.Lock()
	c.publishing = false
	c.mutex.Unlock()
	recheck(t, c)
	require.Equal(t, 1, networkChecks(c))
}

func TestNodesOfTheSameNameAndNoPoolAreEntriesOfTheirOwn(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	first, second := node("HK"), node("HK")
	c.mutex.Lock()
	c.lastAlive[first] = clock.Now()
	c.lastAlive[second] = clock.Now()
	candidates := c.checkCandidatesLocked()
	c.mutex.Unlock()
	require.Len(t, candidates, 2)
	addrs := map[C.Proxy]netip.AddrPort{
		first:  netip.MustParseAddrPort("10.0.0.1:443"),
		second: netip.MustParseAddrPort("10.0.0.2:443"),
	}
	targets, _ := selectTargets(candidates, func(_ context.Context, p C.Proxy) (netip.AddrPort, error) { return addrs[p], nil })
	require.Len(t, targets, 2)
}

func TestTrafficKeepsAPoorNetworkInUse(t *testing.T) {
	c, clock, _, _ := poorNetwork(t)
	for i := 0; i < 6; i++ {
		clock.Advance(2 * time.Minute)
		c.NoteTraffic(clock.Now().UnixNano())
		recheck(t, c)
		require.Equal(t, NetworkPoor, networkState(c), "a connection still downloading uses it")
	}
	// the traffic stops, the checks go on confirming it
	for i := 0; i < 4; i++ {
		clock.Advance(2 * time.Minute)
		recheck(t, c)
		require.Equal(t, NetworkPoor, networkState(c))
	}
	clock.Advance(2 * time.Minute)
	require.Equal(t, NetworkNormal, networkState(c))
	require.Contains(t, networkReasons(c), "idle")
}

func TestANetworkThatExpiredIsNotExtendedByALateCheck(t *testing.T) {
	c, clock, hops, x := poorNetwork(t)
	gate := make(chan struct{})
	setHops(c, func(p C.Proxy) (time.Duration, error) {
		<-gate
		return hops.dial(p)
	})
	clock.Advance(CheckInterval)
	c.NoteDialFailure(x, clock.Now())
	c.kick()
	require.Eventually(t, func() bool {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.network.checking
	}, time.Second, time.Millisecond)
	c.mutex.Lock()
	until := c.network.until()
	c.mutex.Unlock()
	// the check, finding the network poor again, ends after the network expired
	clock.Advance(LocalConfirm)
	state, _ := c.Network()
	require.Equal(t, NetworkNormal, state)
	c.NoteDialFailure(x, clock.Now())
	c.mutex.Lock()
	require.True(t, c.network.checking, "the check still running keeps its place")
	c.mutex.Unlock()
	close(gate)
	settle(t, c)
	c.mutex.Lock()
	require.Equal(t, until, c.network.lastEnd(), "it ended where it expired")
	c.mutex.Unlock()
	voided := false
	for _, raw := range c.Records().Events(0, 10000).Events {
		if strings.Contains(string(raw), `"result":"void"`) {
			voided = true
		}
	}
	require.True(t, voided)
}

func TestFailuresOutOfOrderSuggestAnOutageOnlyWithinTheWindow(t *testing.T) {
	for _, c := range []struct {
		gap     time.Duration
		suspect bool
	}{{9 * time.Minute, true}, {12 * time.Minute, false}} {
		for _, reversed := range []bool{false, true} {
			coord, _, clock := newClockedCoordinator(t)
			a, b := pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
			early, late := clock.Now().Add(-c.gap), clock.Now()
			coord.mutex.Lock()
			if reversed {
				coord.poolFailureLocked(Key{b, testCond}, late)
				coord.poolFailureLocked(Key{a, testCond}, early)
			} else {
				coord.poolFailureLocked(Key{a, testCond}, early)
				coord.poolFailureLocked(Key{b, testCond}, late)
			}
			suspected := len(coord.outages.active) == 1
			coord.mutex.Unlock()
			require.Equal(t, c.suspect, suspected, "%v apart, reversed %v", c.gap, reversed)
		}
	}
}

// outageWithAFormerNode makes u dead before a and b suggest an outage of their pool.
func outageWithAFormerNode(t *testing.T) (*Coordinator, *fakeRunner, *fakeClock, *Registration, *recorder, C.Proxy, C.Proxy, C.Proxy) {
	t.Helper()
	c, runner, clock := newClockedCoordinator(t)
	u, a, b := pooledNode("u", "jk", ""), pooledNode("a", "jk", ""), pooledNode("b", "jk", "")
	g := newRecorder("g", u, a, b)
	reg := activeRegistration(c, g)
	testOnce(t, reg, g, 1, u)
	clock.Advance(OutageWindow + time.Minute)
	testOnce(t, reg, g, 2, a, b)
	require.Len(t, c.Outages(), 1)
	return c, runner, clock, reg, g, u, a, b
}

func TestANodeBackFromBeforeAnOutageNeitherDisprovesItNorStandsForIt(t *testing.T) {
	c, runner, clock, reg, g, u, _, _ := outageWithAFormerNode(t)
	runner.set(u, true)
	testOnce(t, reg, g, 3, u)
	c.mutex.Lock()
	o := c.outages.active[poolKey{"jk", testCond}]
	failures := o.failures
	_, dead := o.dead[Key{u, testCond}]
	c.mutex.Unlock()
	require.False(t, dead, "back, it leaves the recovery tests")

	// u tested again as it stays in place: not a recovery, the outage does not back off
	clock.Advance(time.Minute)
	reg.Submit(4, []Need{{Target: u, Cond: testCond, Class: ClassMaintain, Rank: 40, Source: SourceRecover}})
	require.Eventually(t, func() bool { return runner.count(u) == 3 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return len(c.Outages()) == 1 && len(g.completions()) >= 4 }, time.Second, time.Millisecond)
	c.mutex.Lock()
	require.Equal(t, failures, o.failures)
	c.mutex.Unlock()
	require.Len(t, c.Outages(), 1, "found alive again, it tells nothing either")
}

func TestAnOutageLeftWithNoWitnessIsUndecided(t *testing.T) {
	c, _, _, _, g, u, _, _ := outageWithAFormerNode(t)
	c.mutex.Lock()
	c.pruneOutagesLocked(map[Key]struct{}{{u, testCond}: {}})
	c.mutex.Unlock()
	require.Empty(t, c.Outages())
	require.True(t, g.has(func(n Notice) bool { o, ok := n.(OutageChanged); return ok && o.State == OutageUndecided }))
}
