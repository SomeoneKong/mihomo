package statistic

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHour = int64(493000)

type fakeClock struct {
	unix atomic.Int64
}

func newFakeClock(hour int64) *fakeClock {
	c := &fakeClock{}
	c.unix.Store(hour*3600 + 60)
	return c
}

func (c *fakeClock) now() time.Time { return time.Unix(c.unix.Load(), 0) }

func (c *fakeClock) setHour(hour int64) { c.unix.Store(hour*3600 + 60) }

// useTestUsage points the trackers at a usage with a controllable clock.
func useTestUsage(t *testing.T) (*Usage, *fakeClock) {
	clock := newFakeClock(testHour)
	usage := newUsage(clock.now)
	saved := DefaultUsage
	DefaultUsage = usage
	t.Cleanup(func() { DefaultUsage = saved })
	return usage, clock
}

// countingConn is an outbound connection that moves a fixed number of bytes per call.
type countingConn struct {
	fakeConn
	adapterType C.AdapterType
	addr        string
	closed      int
}

func (c *countingConn) OutboundIdentity() (C.AdapterType, string) { return c.adapterType, c.addr }

func (c *countingConn) Read(b []byte) (int, error) { return len(b), nil }

func (c *countingConn) Write(b []byte) (int, error) { return len(b), nil }

func (c *countingConn) ReadBuffer(buffer *buf.Buffer) error {
	buffer.Extend(7)
	return nil
}

func (c *countingConn) WriteBuffer(buffer *buf.Buffer) error {
	buffer.Release()
	return nil
}

func (c *countingConn) Close() error {
	c.closed++
	return nil
}

// wrappedConn hides the outbound connection the way loopback detection and callbacks do.
type wrappedConn struct {
	C.Conn
}

func (c *wrappedConn) Upstream() any { return c.Conn }

func outboundConn(name, provider string, adapterType C.AdapterType, addr string) *countingConn {
	return &countingConn{
		fakeConn:    fakeConn{chain: C.Chain{name, "group"}, providerChain: C.Chain{provider, ""}},
		adapterType: adapterType,
		addr:        addr,
	}
}

func onlyEntry(t *testing.T, snapshot *UsageSnapshot) UsageEntry {
	t.Helper()
	require.Len(t, snapshot.Entries, 1)
	return snapshot.Entries[0]
}

func TestUsageCountsEveryPath(t *testing.T) {
	usage, _ := useTestUsage(t)
	conn := outboundConn("HK 01", "sub-a", C.Vmess, "hk1.example.com:443")
	tracker := NewTCPTracker(&wrappedConn{conn}, &Manager{}, &C.Metadata{}, nil, 3, 5, false)

	_, _ = tracker.Read(make([]byte, 100))
	_, _ = tracker.Write(make([]byte, 10))
	_ = tracker.ReadBuffer(buf.NewSize(64))
	wb := buf.NewSize(64)
	wb.Extend(11)
	_ = tracker.WriteBuffer(wb)
	_, readCounters := tracker.UnwrapReader()
	readCounters[0](1000)
	_, writeCounters := tracker.UnwrapWriter()
	writeCounters[0](2000)

	entry := onlyEntry(t, usage.Snapshot(0, testHour))
	assert.Equal(t, UsageEntry{
		Provider: "sub-a", Name: "HK 01", Type: "Vmess", Addr: "hk1.example.com:443",
		Buckets: []UsageBucket{{Hour: testHour, Up: 3 + 10 + 11 + 2000, Down: 5 + 100 + 7 + 1000}},
	}, entry)
	assert.Equal(t, tracker.UploadTotal.Load(), entry.Buckets[0].Up)
	assert.Equal(t, tracker.DownloadTotal.Load(), entry.Buckets[0].Down)
}

func TestUsageCreditsTheFirstOutboundMet(t *testing.T) {
	usage, _ := useTestUsage(t)
	relay := outboundConn("relay", "", C.Socks5, "relay.example.com:1080")
	relayTracker := NewTCPTracker(relay, &Manager{}, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(context.Background()))
	// The exit proxy's connection runs over the relay's tracker.
	exit := outboundConn("exit", "sub", C.Trojan, "exit.example.com:443")
	exit.fakeConn.Conn = relayTracker
	exitTracker := NewTCPTracker(&wrappedConn{exit}, &Manager{}, &C.Metadata{}, nil, 0, 0, true)

	_, _ = relayTracker.Write(make([]byte, 50))
	_, _ = exitTracker.Write(make([]byte, 40))

	snapshot := usage.Snapshot(0, testHour)
	require.Len(t, snapshot.Entries, 2)
	assert.Equal(t, UsageEntry{
		Name: "relay", Type: "Socks5", Addr: "relay.example.com:1080",
		Buckets: []UsageBucket{{Hour: testHour, Up: 50}},
	}, snapshot.Entries[0])
	assert.Equal(t, UsageEntry{
		Provider: "sub", Name: "exit", Type: "Trojan", Addr: "exit.example.com:443",
		Buckets: []UsageBucket{{Hour: testHour, Up: 40}},
	}, snapshot.Entries[1])
}

func TestUsageKeepsSameNamedProxiesApart(t *testing.T) {
	usage, _ := useTestUsage(t)
	inline := NewTCPTracker(outboundConn("HK", "", C.Vmess, "a.example.com:443"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)
	fromProvider := NewTCPTracker(outboundConn("HK", "sub", C.Vmess, "b.example.com:443"), &Manager{}, &C.Metadata{}, nil, 2, 0, false)
	defer inline.Close()
	defer fromProvider.Close()

	snapshot := usage.Snapshot(0, testHour)
	require.Len(t, snapshot.Entries, 2)
	assert.Equal(t, "", snapshot.Entries[0].Provider)
	assert.Equal(t, int64(1), snapshot.Entries[0].Buckets[0].Up)
	assert.Equal(t, "sub", snapshot.Entries[1].Provider)
	assert.Equal(t, int64(2), snapshot.Entries[1].Buckets[0].Up)
}

func TestUsageMarksConnectionsWithoutIdentity(t *testing.T) {
	usage, _ := useTestUsage(t)
	conn := &fakeConn{chain: C.Chain{"odd"}, providerChain: C.Chain{"sub"}}
	NewTCPTracker(conn, &Manager{}, &C.Metadata{}, nil, 9, 0, false)

	entry := onlyEntry(t, usage.Snapshot(0, testHour))
	assert.True(t, entry.Incomplete)
	assert.Equal(t, "odd", entry.Name)
	assert.Equal(t, "", entry.Type)
}

func TestUsageSkipsTrackersWithoutUsage(t *testing.T) {
	usage, _ := useTestUsage(t)
	conn := outboundConn("mux", "", C.Vmess, "a.example.com:443")
	tracker := NewTCPTracker(conn, &Manager{}, &C.Metadata{}, nil, 10, 10, false, AsUnderlying(context.Background()), WithoutUsage())
	_, _ = tracker.Write(make([]byte, 10))
	_ = tracker.Close()

	assert.Empty(t, usage.Snapshot(0, testHour).Entries)
}

type countingPacketConn struct {
	C.PacketConn
	chain C.Chain
}

func (c *countingPacketConn) Chains() C.Chain { return c.chain }

func (c *countingPacketConn) ProviderChains() C.Chain { return C.Chain{""} }

func (c *countingPacketConn) RemoteDestination() string { return "" }

func (c *countingPacketConn) OutboundIdentity() (C.AdapterType, string) {
	return C.Hysteria2, "hy.example.com:443"
}

func (c *countingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) { return len(b), nil }

func TestUsageCountsUDP(t *testing.T) {
	usage, _ := useTestUsage(t)
	tracker := NewUDPTracker(&countingPacketConn{chain: C.Chain{"hy"}}, &Manager{}, &C.Metadata{}, nil, 4, 6, false)
	_, _ = tracker.WriteTo(make([]byte, 20), nil)

	entry := onlyEntry(t, usage.Snapshot(0, testHour))
	assert.Equal(t, "Hysteria2", entry.Type)
	assert.Equal(t, []UsageBucket{{Hour: testHour, Up: 24, Down: 6}}, entry.Buckets)
}

func TestUsageSealsPastHours(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	_, _ = tracker.Write(make([]byte, 10))

	snapshot := usage.Snapshot(0, testHour+5)
	assert.Equal(t, testHour, snapshot.Hour)
	assert.Equal(t, testHour-1, snapshot.Settled)
	assert.Equal(t, testHour, snapshot.RetainedSince)

	clock.setHour(testHour + 1)
	usage.tick()
	_, _ = tracker.Write(make([]byte, 20))

	snapshot = usage.Snapshot(0, testHour+5)
	assert.Equal(t, testHour+1, snapshot.Hour)
	assert.Equal(t, testHour, snapshot.Settled)
	assert.Equal(t, []UsageBucket{{Hour: testHour, Up: 10}, {Hour: testHour + 1, Up: 20}}, onlyEntry(t, snapshot).Buckets)
}

func TestUsageWaitsForWritesInFlight(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)
	bucket := tracker.usage.entry.current.Load()

	// A writer that took the bucket but has not added yet.
	bucket.inflight.Add(1)
	clock.setHour(testHour + 1)
	done := make(chan struct{})
	go func() {
		usage.tick()
		close(done)
	}()

	require.Eventually(t, bucket.sealed.Load, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("settled before the write in flight finished")
	case <-time.After(20 * time.Millisecond):
	}
	assert.Equal(t, testHour-1, usage.settled.Load())

	bucket.up.Add(5)
	bucket.inflight.Add(-1)
	<-done
	assert.Equal(t, testHour, usage.settled.Load())

	// Writes arriving after the seal go to the current hour.
	_, _ = tracker.Write(make([]byte, 3))
	assert.Equal(t, []UsageBucket{{Hour: testHour, Up: 6}, {Hour: testHour + 1, Up: 3}},
		onlyEntry(t, usage.Snapshot(0, testHour+1)).Buckets)
}

func TestUsageSettledHoursAreFinal(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := make([]byte, 1)
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = tracker.Write(b)
				}
			}
		}()
	}

	for hour := testHour + 1; hour <= testHour+20; hour++ {
		clock.setHour(hour)
		usage.tick()
		settled := usage.Snapshot(0, hour)
		time.Sleep(time.Millisecond)
		again := usage.Snapshot(0, hour)
		for i, b := range onlyEntry(t, settled).Buckets {
			if b.Hour <= settled.Settled {
				assert.Equal(t, b, onlyEntry(t, again).Buckets[i], "hour %d changed after it settled", b.Hour)
			}
		}
	}
	close(stop)
	wg.Wait()

	var total int64
	for _, b := range onlyEntry(t, usage.Snapshot(0, testHour+20)).Buckets {
		total += b.Up
	}
	assert.Equal(t, tracker.UploadTotal.Load(), total)
}

func TestUsageHourNeverGoesBack(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	clock.setHour(testHour + 2)
	usage.tick()

	clock.setHour(testHour - 5)
	usage.tick()
	_, _ = tracker.Write(make([]byte, 8))

	snapshot := usage.Snapshot(0, testHour+2)
	assert.Equal(t, testHour+2, snapshot.Hour)
	assert.Equal(t, testHour+1, snapshot.Settled)
	assert.Equal(t, []UsageBucket{{Hour: testHour + 2, Up: 8}}, onlyEntry(t, snapshot).Buckets)
}

func TestUsageExpiresOldHoursAndIdleEntries(t *testing.T) {
	usage, clock := useTestUsage(t)
	open := NewTCPTracker(outboundConn("open", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)
	closed := NewTCPTracker(outboundConn("closed", "", C.Vmess, "b:1"), &Manager{}, &C.Metadata{}, nil, 2, 0, false)
	_ = closed.Close()

	clock.setHour(testHour + UsageRetainHours)
	usage.tick()
	snapshot := usage.Snapshot(0, testHour+UsageRetainHours)
	assert.Equal(t, testHour, snapshot.RetainedSince)
	assert.Len(t, snapshot.Entries, 2)

	clock.setHour(testHour + UsageRetainHours + 1)
	usage.tick()
	snapshot = usage.Snapshot(0, testHour+UsageRetainHours+1)
	assert.Equal(t, testHour+1, snapshot.RetainedSince)
	assert.Empty(t, snapshot.Entries)

	usage.mu.Lock()
	_, openKept := usage.entries[open.usage.entry.key]
	_, closedKept := usage.entries[closed.usage.entry.key]
	usage.mu.Unlock()
	assert.True(t, openKept, "an entry still referenced by a tracker is kept")
	assert.False(t, closedKept)

	// The open tracker keeps counting into its entry.
	_, _ = open.Write(make([]byte, 4))
	assert.Equal(t, []UsageBucket{{Hour: testHour + UsageRetainHours + 1, Up: 4}},
		onlyEntry(t, usage.Snapshot(0, testHour+UsageRetainHours+1)).Buckets)
}

func TestUsageReleasesOnce(t *testing.T) {
	usage, _ := useTestUsage(t)
	first := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	second := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	require.Same(t, first.usage.entry, second.usage.entry)

	_ = first.Close()
	_ = first.Close()
	usage.mu.Lock()
	defer usage.mu.Unlock()
	assert.Equal(t, int64(1), first.usage.entry.refs)
}

func TestUsageOverflow(t *testing.T) {
	usage, _ := useTestUsage(t)
	for i := 0; i < UsageMaxEntries; i++ {
		usage.acquire(UsageKey{Name: fmt.Sprint(i)})
	}
	tracker := NewTCPTracker(outboundConn("late", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 3, 0, false)
	_ = tracker.Close()

	snapshot := usage.Snapshot(0, testHour)
	assert.True(t, snapshot.Overflow)
	entry := onlyEntry(t, snapshot)
	assert.Equal(t, UsageOverflowName, entry.Name)
	assert.Equal(t, int64(3), entry.Buckets[0].Up)
}

func TestUsageSnapshotRange(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	for hour := testHour; hour < testHour+4; hour++ {
		clock.setHour(hour)
		usage.tick()
		if hour != testHour+2 {
			_, _ = tracker.Write(make([]byte, 1))
		}
	}

	assert.Equal(t, []UsageBucket{{Hour: testHour + 1, Up: 1}, {Hour: testHour + 3, Up: 1}},
		onlyEntry(t, usage.Snapshot(testHour+1, testHour+3)).Buckets)
	assert.Empty(t, usage.Snapshot(testHour+2, testHour+2).Entries)
}

func TestUsageExpiryWaitsForSnapshots(t *testing.T) {
	usage, clock := useTestUsage(t)
	NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)

	usage.snapshotMu.RLock()
	clock.setHour(testHour + UsageRetainHours + 1)
	done := make(chan struct{})
	go func() {
		usage.tick()
		close(done)
	}()
	require.Eventually(t, func() bool { return usage.settled.Load() == testHour+UsageRetainHours }, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("expired while a snapshot was being copied")
	case <-time.After(20 * time.Millisecond):
	}
	assert.Equal(t, testHour, usage.retainedSince.Load())
	usage.snapshotMu.RUnlock()
	<-done
	assert.Equal(t, testHour+1, usage.retainedSince.Load())
}

func TestUsageRecyclesIdleEntriesFromTheStart(t *testing.T) {
	usage, clock := useTestUsage(t)
	for i := 0; i < UsageMaxEntries; i++ {
		usage.release(usage.acquire(UsageKey{Name: fmt.Sprint(i)}))
	}
	clock.setHour(testHour + 2)
	usage.tick()
	usage.mu.Lock()
	left := len(usage.entries)
	usage.mu.Unlock()
	assert.Zero(t, left)

	NewTCPTracker(outboundConn("late", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 3, 0, false)
	snapshot := usage.Snapshot(0, testHour+2)
	assert.False(t, snapshot.Overflow)
	assert.Equal(t, "late", onlyEntry(t, snapshot).Name)
}

func TestUsageCountsWritesThatOutliveTheirEntry(t *testing.T) {
	usage, clock := useTestUsage(t)
	tracker := NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 0, 0, false)
	_ = tracker.Close()
	clock.setHour(testHour + 2)
	usage.tick()
	usage.mu.Lock()
	_, kept := usage.entries[tracker.usage.entry.key]
	usage.mu.Unlock()
	require.False(t, kept)

	// The tracker's I/O finished before its close and counts only now.
	tracker.usage.add(7, 0)
	assert.Equal(t, []UsageBucket{{Hour: testHour + 2, Up: 7}},
		onlyEntry(t, usage.Snapshot(0, testHour+2)).Buckets)
}
