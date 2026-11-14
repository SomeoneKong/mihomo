package statistic

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAdapter is an outbound that only tells its dialer proxy.
type fakeAdapter struct {
	C.ProxyAdapter
	dialer string
}

func (a *fakeAdapter) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{DialerProxy: a.dialer} }

// adaptedConn is an outbound connection that tells the adapter that made it.
type adaptedConn struct {
	*countingConn
	adapter C.ProxyAdapter
}

func (c *adaptedConn) OutboundAdapter() C.ProxyAdapter { return c.adapter }

func connMadeBy(adapter C.ProxyAdapter, chain ...string) *adaptedConn {
	conn := outboundConn(chain[0], "", C.Vless, chain[0]+":443")
	conn.chain = chain
	return &adaptedConn{countingConn: conn, adapter: adapter}
}

func TestUsageSplitsByKindAndRoute(t *testing.T) {
	usage, _ := useTestUsage(t)
	ctx, recorder := WithDialerRecorder(context.Background())
	relay := NewTCPTracker(connMadeBy(&fakeAdapter{}, "JP 01", "JpDialer"), &Manager{}, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(ctx))
	exit := NewTCPTracker(connMadeBy(&fakeAdapter{dialer: "JpDialer"}, "us@JpDialer", "MyProxy", "DnsProxy"), &Manager{}, &C.Metadata{}, nil, 0, 0, true, WithDialedLayers(recorder.Layers()))
	_, _ = relay.Write(make([]byte, 50))
	_, _ = exit.Write(make([]byte, 40))

	snapshot := usage.Snapshot(0, testHour, true)
	assert.True(t, snapshot.Routes)
	require.Len(t, snapshot.Entries, 2)
	assert.Equal(t, UsageEntry{
		Name: "JP 01", Type: "Vless", Addr: "JP 01:443", Kind: "relay",
		Buckets: []UsageBucket{{Hour: testHour, Up: 50}},
	}, snapshot.Entries[0])
	assert.Equal(t, UsageEntry{
		Name: "us@JpDialer", Type: "Vless", Addr: "us@JpDialer:443", Kind: "exit",
		UsageRoute: UsageRoute{Groups: []string{"MyProxy", "DnsProxy"}, Dialers: [][]string{{"JP 01", "JpDialer"}}},
		Buckets:    []UsageBucket{{Hour: testHour, Up: 40}},
	}, snapshot.Entries[1])

	merged := usage.Snapshot(0, testHour, false)
	assert.False(t, merged.Routes)
	require.Len(t, merged.Entries, 2)
	assert.Equal(t, UsageEntry{Name: "JP 01", Type: "Vless", Addr: "JP 01:443", Buckets: []UsageBucket{{Hour: testHour, Up: 50}}}, merged.Entries[0])
	assert.Empty(t, merged.Entries[1].Kind)
	assert.Empty(t, merged.Entries[1].Groups)
}

func TestUsageWithoutEntriesListsNone(t *testing.T) {
	usage, _ := useTestUsage(t)
	for _, routes := range []bool{false, true} {
		encoded, err := json.Marshal(usage.Snapshot(0, -1, routes))
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"entries":[]`)
	}
	NewTCPTracker(outboundConn("n", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)
	encoded, err := json.Marshal(usage.Snapshot(testHour+1, testHour+1, false))
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"entries":[]`, "a range without traffic")
}

func TestUsageMergesTheRoutesOfAProxy(t *testing.T) {
	usage, clock := useTestUsage(t)
	hk := &fakeAdapter{}
	direct := NewTCPTracker(connMadeBy(hk, "HK 01", "HKProxy"), &Manager{}, &C.Metadata{}, nil, 1, 0, false)
	ctx, recorder := WithDialerRecorder(context.Background())
	relay := NewTCPTracker(connMadeBy(hk, "HK 01", "HkDialer"), &Manager{}, &C.Metadata{}, nil, 2, 0, false, AsUnderlying(ctx))
	clock.setHour(testHour + 1)
	usage.tick()
	viaGroup := NewTCPTracker(connMadeBy(hk, "HK 01", "Fallback", "HKProxy"), &Manager{}, &C.Metadata{}, nil, 4, 0, false, WithDialedLayers(recorder.Layers()))
	_, _ = direct.Write(make([]byte, 8))
	defer relay.Close()
	defer viaGroup.Close()

	assert.Len(t, usage.Snapshot(0, testHour+1, true).Entries, 3)
	assert.Equal(t, []UsageBucket{{Hour: testHour, Up: 1 + 2}, {Hour: testHour + 1, Up: 8 + 4}},
		onlyEntry(t, usage.Snapshot(0, testHour+1, false)).Buckets)
}

func TestUsageDialersFollowTheDial(t *testing.T) {
	layer := func(adapter C.ProxyAdapter, chain ...string) DialedLayer {
		return DialedLayer{Chain: chain, adapter: adapter}
	}
	for name, test := range map[string]struct {
		dialer string
		dialed []DialedLayer
		want   [][]string
	}{
		"no dialer proxy": {},
		"a dialer group": {
			dialer: "G",
			dialed: []DialedLayer{layer(&fakeAdapter{}, "A", "G")},
			want:   [][]string{{"A", "G"}},
		},
		"a dialer proxy naming a node": {
			dialer: "R",
			dialed: []DialedLayer{layer(&fakeAdapter{}, "R")},
			want:   [][]string{{"R"}},
		},
		// a round-robin group first picked a node relayed through itself, then one that is not
		"the same group in two layers": {
			dialer: "G",
			dialed: []DialedLayer{layer(&fakeAdapter{}, "B", "G"), layer(&fakeAdapter{dialer: "G"}, "A", "G")},
			want:   [][]string{{"A", "G"}, {"B", "G"}},
		},
		"nested dialer groups": {
			dialer: "ToSG",
			dialed: []DialedLayer{layer(&fakeAdapter{}, "JP 01", "JpDialer", "ToSG")},
			want:   [][]string{{"JP 01", "JpDialer", "ToSG"}},
		},
		"a reused session": {
			dialer: "D",
			want:   [][]string{{"", "D"}},
		},
		"a deeper layer not dialed": {
			dialer: "G",
			dialed: []DialedLayer{layer(&fakeAdapter{dialer: "H"}, "A", "G")},
			want:   [][]string{{"A", "G"}, {"", "H"}},
		},
		"a layer naming itself": {
			dialer: "G",
			dialed: []DialedLayer{layer(&fakeAdapter{dialer: "G"}, "A", "G")},
			want:   [][]string{{"A", "G"}, {"", "G"}},
		},
		"another dial's layer after this one's": {
			dialer: "G",
			dialed: []DialedLayer{layer(&fakeAdapter{}, "A", "G"), layer(&fakeAdapter{}, "X", "Other")},
			want:   [][]string{{"A", "G"}},
		},
	} {
		assert.Equal(t, test.want, usageDialers(&fakeAdapter{dialer: test.dialer}, test.dialed), name)
	}
	assert.Nil(t, usageDialers(nil, nil))
}

func TestUnderlyingConnectionsAreRecordedWithTheirAdapter(t *testing.T) {
	ctx, recorder := WithDialerRecorder(context.Background())
	adapter := &fakeAdapter{dialer: "G"}
	NewTCPTracker(connMadeBy(adapter, "A", "G"), &Manager{}, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(ctx), WithoutUsage())

	layers := recorder.Layers()
	require.Len(t, layers, 1)
	assert.Equal(t, C.Chain{"A", "G"}, layers[0].Chain)
	assert.Same(t, adapter, layers[0].adapter)
}

func TestUsageOverflowKeepsKindsApart(t *testing.T) {
	usage, _ := useTestUsage(t)
	for i := 0; i < UsageMaxEntries; i++ {
		usage.acquire(UsageKey{Name: fmt.Sprint(i)})
	}
	exit := NewTCPTracker(outboundConn("late", "", C.Vmess, "a:1"), &Manager{}, &C.Metadata{}, nil, 3, 0, false)
	relay := NewTCPTracker(outboundConn("relay", "", C.Vmess, "b:1"), &Manager{}, &C.Metadata{}, nil, 5, 0, false, AsUnderlying(context.Background()))
	_ = exit.Close()
	_ = relay.Close()

	split := usage.Snapshot(0, testHour, true)
	assert.True(t, split.Overflow)
	require.Len(t, split.Entries, 2)
	assert.Equal(t, "exit", split.Entries[0].Kind)
	assert.Equal(t, int64(3), split.Entries[0].Buckets[0].Up)
	assert.Equal(t, "relay", split.Entries[1].Kind)
	assert.Equal(t, int64(5), split.Entries[1].Buckets[0].Up)

	entry := onlyEntry(t, usage.Snapshot(0, testHour, false))
	assert.Equal(t, UsageOverflowName, entry.Name)
	assert.Equal(t, int64(8), entry.Buckets[0].Up)
}
