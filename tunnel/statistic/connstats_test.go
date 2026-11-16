package statistic

import (
	"context"
	"encoding/json"
	"math"
	"net/netip"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// useTestConnStats points the trackers at connection stats and closed connections with a
// controllable clock, counting from the start.
func useTestConnStats(t *testing.T) (*ConnStats, *ClosedConns, *fakeClock) {
	usage, clock := useTestUsage(t)
	stats := newConnStats(clock.now, usage.instance, usage.startedAt)
	stats.SetEnabled(true)
	closed := newClosedConns(clock.now, usage.instance, usage.startedAt)
	savedStats, savedClosed := DefaultConnStats, DefaultClosed
	DefaultConnStats, DefaultClosed = stats, closed
	t.Cleanup(func() { DefaultConnStats, DefaultClosed = savedStats, savedClosed })
	return stats, closed, clock
}

func testMetadata(host string) *C.Metadata {
	return &C.Metadata{
		NetWork:     C.TCP,
		Type:        C.TUN,
		SrcIP:       netip.MustParseAddr("198.18.0.1"),
		DstIP:       netip.MustParseAddr("203.0.113.9"),
		Host:        host,
		Process:     "chrome.exe",
		ProcessPath: `C:\Chrome\chrome.exe`,
	}
}

type testRule struct{ payload string }

func (r testRule) RuleType() C.RuleType                                { return C.DomainSuffix }
func (r testRule) Match(*C.Metadata, C.RuleMatchHelper) (bool, string) { return true, "" }
func (r testRule) Adapter() string                                     { return "" }
func (r testRule) Payload() string                                     { return r.payload }
func (r testRule) ProviderNames() []string                             { return nil }

func newTestTracker(host string, options ...TrackerOption) *tcpTracker {
	conn := outboundConn("JP 01", "sub-a", C.Vmess, "jp1.example.com:443")
	return NewTCPTracker(conn, &Manager{}, testMetadata(host), testRule{"example.com"}, 0, 0, false, options...)
}

func connEntries(t *testing.T, stats *ConnStats) []ConnStatsEntry {
	t.Helper()
	return stats.Snapshot(0, 1<<62, false, 0).Entries
}

func TestConnStatsCountKeysAndOpenedConnections(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	first := newTestTracker("www.example.com")
	second := newTestTracker("www.example.com")
	_, _ = first.Read(make([]byte, 100))
	_, _ = second.Write(make([]byte, 10))
	_, _ = first.Read(make([]byte, 5))

	entries := connEntries(t, stats)
	require.Len(t, entries, 1)
	assert.Equal(t, ConnStatsEntry{
		InType: "Tun", SrcIP: "198.18.0.1", Process: `C:\Chrome\chrome.exe`,
		Target: "www.example.com", Network: "tcp", Rule: "DomainSuffix", RulePayload: "example.com",
		Provider: "sub-a", Chain: []string{"JP 01", "group"},
		Buckets: []ConnStatsBucket{{Hour: testHour, Up: 10, Down: 105, Conns: 2}},
	}, entries[0])
}

func TestConnStatsTargetFallsBackToTheSniffedHostAndTheIP(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	sniffed := testMetadata("")
	sniffed.SniffHost = "sniffed.example.com"
	bare := testMetadata("")
	bare.ProcessPath = ""
	for _, metadata := range []*C.Metadata{sniffed, bare} {
		NewTCPTracker(outboundConn("JP 01", "", C.Vmess, "a:1"), &Manager{}, metadata, nil, 0, 0, false)
	}

	targets := map[string]ConnStatsEntry{}
	for _, entry := range connEntries(t, stats) {
		targets[entry.Target] = entry
	}
	require.Len(t, targets, 2)
	assert.False(t, targets["sniffed.example.com"].TargetIsIP)
	assert.True(t, targets["203.0.113.9"].TargetIsIP)
	assert.Equal(t, "chrome.exe", targets["203.0.113.9"].Process)
}

func TestConnStatsLeaveOutRelaysAndCarriers(t *testing.T) {
	stats, closed, _ := useTestConnStats(t)
	relay := newTestTracker("relay.example.com", AsUnderlying(context.Background()))
	carrier := newTestTracker("carrier.example.com", WithoutUsage())
	_, _ = relay.Read(make([]byte, 10))
	_, _ = carrier.Read(make([]byte, 10))
	_ = relay.Close()
	_ = carrier.Close()

	assert.Empty(t, connEntries(t, stats))
	assert.Empty(t, closed.Snapshot(0, 100).Records)
}

func TestConnStatsCountOpenConnectionsOnceEnabled(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	stats.SetEnabled(false)
	tracker := newTestTracker("www.example.com")
	_, _ = tracker.Read(make([]byte, 100))
	assert.Empty(t, connEntries(t, stats))

	stats.SetEnabled(true)
	_, _ = tracker.Read(make([]byte, 7))
	stats.SetEnabled(false)
	_, _ = tracker.Read(make([]byte, 1000))
	stats.SetEnabled(true)
	_, _ = tracker.Write(make([]byte, 3))

	entries := connEntries(t, stats)
	require.Len(t, entries, 1)
	assert.Equal(t, []ConnStatsBucket{{Hour: testHour, Up: 3, Down: 7, Conns: 0}}, entries[0].Buckets)
	assert.Equal(t, int64(1), tracker.usage.conn.entry.Load().refs)

	_ = tracker.Close()
	assert.Equal(t, int64(0), tracker.usage.conn.entry.Load().refs)
}

func TestConnStatsCountTrafficAfterTheTrackerLeft(t *testing.T) {
	stats, _, clock := useTestConnStats(t)
	tracker := newTestTracker("www.example.com")
	_ = tracker.Close()
	later := testHour + ConnStatsRetainHours + 2
	clock.setHour(later)
	stats.tick()
	require.Empty(t, stats.entries, "the idle entry is removed")

	tracker.usage.add(0, 9)
	entries := stats.Snapshot(later, later, false, 0).Entries
	require.Len(t, entries, 1)
	assert.Equal(t, []ConnStatsBucket{{Hour: later, Down: 9}}, entries[0].Buckets)
	assert.Equal(t, int64(0), stats.entries[connStatsKeyOf(tracker.TrackerInfo)].refs)
}

func TestConnStatsCountingOfHours(t *testing.T) {
	stats, _, clock := useTestConnStats(t)
	clock.setHour(testHour + 1)
	stats.tick()
	stats.SetEnabled(false)
	stats.SetEnabled(true)
	clock.setHour(testHour + 2)
	stats.tick()
	stats.SetEnabled(false)
	clock.setHour(testHour + 4)
	stats.tick()

	counting := stats.Snapshot(0, 1<<62, false, 0).Counting
	assert.Equal(t, [][2]int64{{testHour, testHour}}, counting.On)
	assert.Equal(t, [][2]int64{{testHour + 1, testHour + 2}}, counting.Partial)
}

func TestConnStatsSwitchAfterTheHourPassed(t *testing.T) {
	stats, _, clock := useTestConnStats(t)
	stats.SetEnabled(false)
	ticks := stats.ticks.Load()
	// the hour changed, and the switch comes before the next tick
	clock.setHour(testHour + 3)
	stats.SetEnabled(true)
	stats.tick()
	assert.Equal(t, ticks+1, stats.ticks.Load(), "a switch is no tick")

	counting := stats.Snapshot(0, 1<<62, false, 0).Counting
	assert.Empty(t, counting.On)
	assert.Equal(t, [][2]int64{{testHour, testHour}, {testHour + 3, testHour + 3}}, counting.Partial)
}

func TestConnStatsCountingWaitsForTheConfig(t *testing.T) {
	usage, clock := useTestUsage(t)
	stats := newConnStats(clock.now, usage.instance, usage.startedAt)
	stats.SetEnabled(true)
	counting := stats.Snapshot(0, 1<<62, false, 0).Counting
	assert.Equal(t, [][2]int64{{testHour, testHour}}, counting.On, "the hour before the first config is not a pause")
	assert.Empty(t, counting.Partial)
}

func TestConnStatsIncrementalSnapshot(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	quiet := newTestTracker("quiet.example.com")
	busy := newTestTracker("busy.example.com")
	_, _ = quiet.Read(make([]byte, 1))
	stats.tick()
	stats.tick()

	first := stats.Snapshot(0, 1<<62, false, 0)
	require.Len(t, first.Entries, 2)
	_, _ = busy.Read(make([]byte, 5))

	next := stats.Snapshot(0, 1<<62, true, first.Tick-1)
	require.Len(t, next.Entries, 1)
	assert.Equal(t, "busy.example.com", next.Entries[0].Target)
	assert.Equal(t, int64(5), next.Entries[0].Buckets[0].Down)
}

func TestConnStatsOverflow(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	stats.maxEntries = 1
	newTestTracker("a.example.com")
	tracker := newTestTracker("b.example.com")
	_, _ = tracker.Read(make([]byte, 4))

	snapshot := stats.Snapshot(0, 1<<62, false, 0)
	assert.True(t, snapshot.Overflow)
	targets := map[string]int64{}
	for _, entry := range snapshot.Entries {
		targets[entry.Target] = entry.Buckets[0].Down
	}
	assert.Equal(t, map[string]int64{"a.example.com": 0, ConnStatsOverflowName: 4}, targets)
}

func TestConnStatsExpire(t *testing.T) {
	stats, _, clock := useTestConnStats(t)
	tracker := newTestTracker("www.example.com")
	_ = tracker.Close()
	clock.setHour(testHour + ConnStatsRetainHours + 1)
	stats.tick()

	snapshot := stats.Snapshot(0, 1<<62, false, 0)
	assert.Equal(t, testHour+1, snapshot.RetainedSince)
	assert.Empty(t, snapshot.Entries)
	assert.Empty(t, stats.entries)
}

func TestClosedConnectionsRecordTheFinalTotals(t *testing.T) {
	_, closed, _ := useTestConnStats(t)
	tracker := newTestTracker("www.example.com")
	_, _ = tracker.Read(make([]byte, 100))
	_ = tracker.Close()
	_ = tracker.Close()

	first := closed.Snapshot(0, 100)
	require.Len(t, first.Records, 1)
	assert.Equal(t, int64(1), first.Records[0].Seq)
	assert.Equal(t, int64(100), first.Records[0].DownloadTotal.Load())

	// A read in flight at the close returns afterwards: the record moves to the end.
	_, _ = tracker.Read(make([]byte, 50))
	again := closed.Snapshot(first.Latest, 100)
	require.Len(t, again.Records, 1)
	assert.Equal(t, int64(2), again.Records[0].Seq)
	assert.Equal(t, int64(150), again.Records[0].DownloadTotal.Load())
	assert.Len(t, closed.Snapshot(0, 100).Records, 1, "one record per connection")

	encoded, err := json.Marshal(again.Records[0])
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"download":150`)
	assert.Contains(t, string(encoded), `"seq":2`)
	assert.Contains(t, string(encoded), `"closedAt"`)
}

func TestClosedConnectionsEviction(t *testing.T) {
	_, closed, clock := useTestConnStats(t)
	closed.maxRecords = 2
	for i := 0; i < 3; i++ {
		_ = newTestTracker("www.example.com").Close()
	}
	snapshot := closed.Snapshot(0, 100)
	assert.Equal(t, int64(2), snapshot.Oldest)
	assert.Equal(t, int64(3), snapshot.Latest)
	assert.True(t, snapshot.Truncated)
	require.NotNil(t, snapshot.EvictedUntil)
	assert.Len(t, snapshot.Records, 2)
	assert.False(t, closed.Snapshot(1, 100).Truncated)

	clock.unix.Add(int64(ClosedRetain/time.Second) + 1)
	snapshot = closed.Snapshot(3, 100)
	assert.Empty(t, snapshot.Records)
	assert.Equal(t, int64(0), snapshot.Oldest)
	assert.False(t, snapshot.Truncated)
	assert.True(t, closed.Snapshot(2, 100).Truncated)
}

func TestClosedConnectionsPage(t *testing.T) {
	_, closed, _ := useTestConnStats(t)
	for i := 0; i < 5; i++ {
		_ = newTestTracker("www.example.com").Close()
	}
	page := closed.Snapshot(1, 2)
	require.Len(t, page.Records, 2)
	assert.Equal(t, []int64{2, 3}, []int64{page.Records[0].Seq, page.Records[1].Seq})
	assert.Equal(t, int64(5), page.Latest)
}

func TestConnStatsRace(t *testing.T) {
	stats, closed, _ := useTestConnStats(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				tracker := newTestTracker("www.example.com")
				_, _ = tracker.Read(make([]byte, 1))
				if j%3 == i%3 {
					stats.SetEnabled(j%2 == 0)
				}
				_ = tracker.Close()
				_, _ = tracker.Write(make([]byte, 1))
			}
		}(i)
	}
	for i := 0; i < 50; i++ {
		stats.Snapshot(0, 1<<62, false, 0)
		closed.Snapshot(0, 10)
		stats.tick()
	}
	wg.Wait()
	assert.LessOrEqual(t, len(closed.Snapshot(0, ClosedMaxRecords).Records), ClosedMaxRecords)
}

func TestClosedConnectionsExpireRenewedRecordsByTheirCloseTime(t *testing.T) {
	_, closed, clock := useTestConnStats(t)
	closed.maxRecords = 2
	a := newTestTracker("a.example.com")
	_ = a.Close()
	clock.unix.Add(1)
	_ = newTestTracker("b.example.com").Close()
	// a read in flight at the close of a comes back: a moves behind b
	_, _ = a.Read(make([]byte, 1))

	clock.unix.Add(int64(ClosedRetain / time.Second))
	snapshot := closed.Snapshot(0, 100)
	require.Len(t, snapshot.Records, 1, "a expired though b, closed later, comes first")
	assert.Equal(t, "b.example.com", snapshot.Records[0].Metadata.Host)

	_ = newTestTracker("c.example.com").Close()
	snapshot = closed.Snapshot(0, 100)
	assert.Len(t, snapshot.Records, 2, "b and c fit once a is gone")
	assert.Nil(t, snapshot.EvictedUntil, "nothing was dropped for room")
	assert.True(t, snapshot.Truncated)
}

func TestClosedConnectionsAfterTheLatest(t *testing.T) {
	_, closed, _ := useTestConnStats(t)
	_ = newTestTracker("www.example.com").Close()
	assert.Empty(t, closed.Snapshot(math.MaxInt64, 100).Records)
	assert.Empty(t, closed.Snapshot(1, 100).Records)
	assert.Len(t, closed.Snapshot(0, 100).Records, 1)
}

func TestConnStatsCountNothingOnceStopped(t *testing.T) {
	stats, _, _ := useTestConnStats(t)
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				tracker := newTestTracker("www.example.com")
				_, _ = tracker.Read(make([]byte, 1))
				_ = tracker.Close()
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	stats.SetEnabled(false)
	total := func() (sum int64) {
		for _, entry := range stats.Snapshot(0, 1<<62, false, 0).Entries {
			for _, bucket := range entry.Buckets {
				sum += bucket.Down + bucket.Conns
			}
		}
		return sum
	}
	stopped := total()
	time.Sleep(10 * time.Millisecond)
	close(done)
	wg.Wait()
	assert.False(t, stats.Snapshot(0, 0, false, 0).Enabled)
	assert.Equal(t, stopped, total(), "nothing is counted after the stop returned")
}
