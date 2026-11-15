package outboundgroup

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

var dialFields = []string{"dials", "launched", dialNoMember, dialRejected, dialFailed, dialCanceled, dialUnobserved, dialFailedBeforeData, dialClosedSent, dialClosedUnsent, "unknown"}

// sealDials lets the group handle what it heard, removes it as a reload without it does, and
// returns the dials it summed up.
func (ot *odTest) sealDials(f *Fallback) map[string]int {
	ot.t.Helper()
	ot.idle(f)
	f.od.retire(nil)
	migrateBindings(f.od, nil)
	return dialTotals(ot.t, ot.groupEvents("dials"))
}

func dialTotals(t *testing.T, events []map[string]any) map[string]int {
	totals := map[string]int{}
	for _, e := range events {
		require.Equal(t, "tcp", e["transport"])
		for _, field := range dialFields {
			totals[field] += int(e[field].(float64))
		}
		for wait, count := range e["firstData"].(map[string]any) {
			totals[wait] += int(count.(float64))
		}
	}
	return totals
}

// requireDials checks every count, the ones not wanted being zero.
func requireDials(t *testing.T, totals map[string]int, want map[string]int) {
	t.Helper()
	for field, count := range totals {
		require.Equal(t, want[field], count, field)
	}
	for field, count := range want {
		require.Equal(t, count, totals[field], field)
	}
}

func TestOnDemandCountsTheDialsOfTheGroupByHowTheyEnd(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	// closed before any data, once something was sent and once not
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	_, err = conn.Write([]byte("hi"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	// a dial a probe makes is none of the group's
	conn, err = f.DialContext(C.WithProbe(context.Background()), metadata)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 3, "within1s": 1, dialClosedSent: 1, dialClosedUnsent: 1})
}

func TestOnDemandCountsADialThatFailedOrWasCanceled(t *testing.T) {
	ot := newODTest(t)
	a := ot.gateNode("A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	a.set(0, true)
	_, err := f.DialContext(context.Background(), metadata)
	require.Error(t, err)
	a.set(time.Second, false)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err = f.DialContext(ctx, metadata)
	require.Error(t, err)
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 2, dialFailed: 1, dialCanceled: 1})
}

func TestOnDemandCountsAFailureBeforeTheFirstData(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	conn, err := f.DialContext(context.Background(), refuser(t))
	require.NoError(t, err)
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	require.NoError(t, conn.Close())
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 1, dialFailedBeforeData: 1})
}

// refuser listens on the loopback and closes what it accepts.
func refuser(t *testing.T) *C.Metadata {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	addr := listener.Addr().(*net.TCPAddr)
	metadata := &C.Metadata{NetWork: C.TCP, DstPort: uint16(addr.Port)}
	require.NoError(t, metadata.SetRemoteAddress(addr.String()))
	return metadata
}

func TestOnDemandTellsANoMemberFromAChosenReject(t *testing.T) {
	ot := newODTest(t)
	lone := ot.group("lone", "A")
	ending := ot.group("ending", "B", "REJECT")
	ot.coord.Manual(context.Background(), ot.proxies["A"], lone.od.cond, "test")
	ot.coord.Manual(context.Background(), ot.proxies["B"], ending.od.cond, "test")
	ot.eventually(func() bool { return ot.current(ending) == "REJECT" }, "the REJECT it ends with is chosen")
	ot.eventually(func() bool { return ot.route(lone) == "" || ot.route(lone) == "REJECT" }, "nothing is left to choose")
	metadata := echo(t)
	_, err := lone.DialContext(context.Background(), metadata)
	require.ErrorIs(t, err, errNoAvailable)
	conn, err := ending.DialContext(context.Background(), metadata)
	require.NoError(t, err, "a REJECT member hands out a connection")
	_ = conn.Close()
	ot.idle(lone)
	lone.od.retire(nil)
	migrateBindings(lone.od, nil)
	ending.od.retire(nil)
	migrateBindings(ending.od, nil)
	byGroup := map[string][]map[string]any{}
	for _, e := range ot.groupEvents("dials") {
		byGroup[e["group"].(string)] = append(byGroup[e["group"].(string)], e)
	}
	requireDials(t, dialTotals(t, byGroup["lone"]), map[string]int{"dials": 1, dialNoMember: 1})
	requireDials(t, dialTotals(t, byGroup["ending"]), map[string]int{"dials": 1, dialRejected: 1})
}

func TestOnDemandCountsADialCarriedByADirectItEndsWith(t *testing.T) {
	ot := newODTest(t)
	ot.proxies["DIRECT"] = adapter.NewProxy(outbound.NewDirect())
	f := ot.group("g", "A", "DIRECT")
	ot.coord.Manual(context.Background(), ot.proxies["A"], f.od.cond, "test")
	ot.eventually(func() bool { return ot.current(f) == "DIRECT" }, "selects DIRECT")
	conn, err := f.DialContext(context.Background(), echo(t))
	require.NoError(t, err)
	use(t, conn)
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 1, dialUnobserved: 1})
}

func TestOnDemandCountsADialACandidateSavedOnce(t *testing.T) {
	ot := newODTest(t)
	a, _ := ot.gateNode("A"), ot.gateNode("B")
	a.set(0, true)
	ot.runner.set(true, "B")
	f := ot.group("g", "A", "B", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	conn, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err, "B carries the dial A failed")
	use(t, conn)
	// a caller dialing again counts again
	conn, err = f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	use(t, conn)
	totals := ot.sealDials(f)
	require.Equal(t, 2, totals["dials"])
	require.Equal(t, 2, totals["within1s"])
	require.GreaterOrEqual(t, totals["launched"], 1, "B was dialed beside or after A")
}

func TestOnDemandCountsADialInTheMinuteItStarted(t *testing.T) {
	ot, clock := newClockedODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	started := clock.Now()
	late, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	slow, err := f.DialContext(context.Background(), metadata)
	require.NoError(t, err)
	// the first data of slow comes five seconds on, in the minute after maybe
	clock.Advance(5 * time.Second)
	use(t, slow)
	ot.idle(f)
	// the minute is summed up two minutes after it ends; the first data of late comes after
	clock.Advance(4 * time.Minute)
	f.od.post(settleNote{done: make(chan struct{})})
	ot.eventually(func() bool { return len(ot.groupEvents("dials")) == 1 }, "the minute is summed up")
	use(t, late)
	ot.idle(f)
	events := ot.groupEvents("dials")
	require.Len(t, events, 1)
	require.Equal(t, formatTime(time.Unix(started.Unix()/60*60, 0)), events[0]["minute"])
	requireDials(t, dialTotals(t, events), map[string]int{"dials": 2, "later": 1, "unknown": 1})
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 2, "later": 1, "unknown": 1})
}

func TestOnDemandAReloadCountsTheDialsTheReplacedGroupTook(t *testing.T) {
	for _, members := range [][]string{{"A", "B", "REJECT"}, {"B", "REJECT"}} {
		ot := newODTest(t)
		ot.runner.set(true, "A", "B")
		old := ot.group("g", "A", "B", "REJECT")
		ot.eventually(func() bool { return ot.current(old) == "A" }, "selects A")
		conn, err := old.DialContext(context.Background(), echo(t))
		require.NoError(t, err)
		fresh := ot.build("g", members...)
		ot.replace(old, fresh)
		// the connection the old group handed out ends after the reload, kept member or not
		use(t, conn)
		totals := ot.sealDials(fresh)
		delete(totals, "launched") // B may be dialed beside A, not known alive yet
		requireDials(t, totals, map[string]int{"dials": 1, "within1s": 1})
	}
}

func TestOnDemandAGroupRemovedCountsNothingAfter(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	conn, err := f.DialContext(context.Background(), echo(t))
	require.NoError(t, err)
	requireDials(t, ot.sealDials(f), map[string]int{"dials": 1, "unknown": 1})
	use(t, conn)
	time.Sleep(50 * time.Millisecond)
	requireDials(t, dialTotals(t, ot.groupEvents("dials")), map[string]int{"dials": 1, "unknown": 1})
}

func TestOnDemandCountsConcurrentDialsOnce(t *testing.T) {
	ot := newODTest(t)
	ot.runner.set(true, "A")
	f := ot.group("g", "A", "REJECT")
	ot.eventually(func() bool { return ot.current(f) == "A" }, "selects A")
	metadata := echo(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := f.DialContext(context.Background(), metadata)
			if err != nil {
				return
			}
			// the first data, a read and the close race to end the dial
			go func() { _ = conn.Close() }()
			_, _ = conn.Write([]byte("hi"))
			_, _ = conn.Read(make([]byte, 2))
			_ = conn.Close()
		}()
	}
	wg.Wait()
	totals := ot.sealDials(f)
	require.Equal(t, 20, totals["dials"])
	ended := totals["within1s"] + totals["within3s"] + totals["later"] + totals[dialFailedBeforeData] + totals[dialClosedSent] + totals[dialClosedUnsent] + totals[dialFailed]
	require.Equal(t, 20, ended, "each dial ends once")
	require.Zero(t, totals["unknown"])
}
