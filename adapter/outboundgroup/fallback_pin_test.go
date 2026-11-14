package outboundgroup

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// flakyAdapter is a direct outbound that can be taken down, counting the dials made
// through it, tests included.
type flakyAdapter struct {
	*outbound.Direct
	down  atomic.Bool
	dials atomic.Int32
}

func (a *flakyAdapter) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	a.dials.Add(1)
	if a.down.Load() {
		return nil, errors.New("down")
	}
	return a.Direct.DialContext(ctx, metadata)
}

type pinTest struct {
	group    *Fallback
	url      string
	proxies  map[string]C.Proxy
	adapters map[string]*flakyAdapter
}

// newPinTest builds a fallback group of the named proxies, tested against a local server.
func newPinTest(t *testing.T, names ...string) *pinTest {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	url := server.URL + "/generate_204"

	pt := &pinTest{url: url, proxies: map[string]C.Proxy{}, adapters: map[string]*flakyAdapter{}}
	var members []C.Proxy
	for _, name := range names {
		a := &flakyAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: name})}
		proxy := adapter.NewProxy(a)
		pt.adapters[name] = a
		pt.proxies[name] = proxy
		members = append(members, proxy)
	}
	pd, err := provider.NewCompatibleProvider("pin-test", members, provider.NewHealthCheck(members, url, 2000, 0, true, nil))
	require.NoError(t, err)
	group, err := NewFallback(GroupCommonOption{Name: "fallback", URL: url, TestTimeout: 2000}, FallbackOption{}, nil, []P.ProxyProvider{pd})
	require.NoError(t, err)
	pt.group = group
	return pt
}

// test records whether the proxy passes a test of the group's URL.
func (pt *pinTest) test(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = pt.proxies[name].URLTest(ctx, pt.url, nil)
}

func (pt *pinTest) retestRunning() bool {
	pt.group.pinRetest.mutex.Lock()
	defer pt.group.pinRetest.mutex.Unlock()
	return pt.group.pinRetest.running
}

func TestFallbackKeepsAPinThatFailsATest(t *testing.T) {
	pt := newPinTest(t, "a", "b", "c")
	require.NoError(t, pt.group.Set("b"))
	require.Equal(t, "b", pt.group.Now())

	pt.adapters["b"].down.Store(true)
	pt.test("b")
	require.Equal(t, "a", pt.group.Now(), "the group falls back while the pinned proxy is out")
	require.Equal(t, "b", pt.group.selected, "the pin stays")

	pt.adapters["b"].down.Store(false)
	pt.test("b")
	require.Equal(t, "b", pt.group.Now(), "the group goes back to the pinned proxy")
}

func TestFallbackTestsAPinnedProxyAgainOnlyWhenInUse(t *testing.T) {
	pt := newPinTest(t, "a", "b")
	require.NoError(t, pt.group.Set("b"))
	pt.adapters["b"].down.Store(true)
	pt.test("b")
	pt.adapters["b"].down.Store(false) // recovered, but not tested yet

	dials := pt.adapters["b"].dials.Load()
	for i := 0; i < 5; i++ {
		require.Equal(t, "a", pt.group.Now())
	}
	require.False(t, pt.retestRunning())
	require.Equal(t, dials, pt.adapters["b"].dials.Load(), "reading the group tests nothing")

	require.Equal(t, "a", pt.group.findAliveProxy(true).Name())
	require.Eventually(t, func() bool { return pt.group.Now() == "b" }, 5*time.Second, 20*time.Millisecond,
		"using the group tests the pinned proxy again")
}

func TestFallbackBacksOffWhileAPinnedProxyStaysOut(t *testing.T) {
	pt := newPinTest(t, "a", "b")
	require.NoError(t, pt.group.Set("b"))
	b := pt.adapters["b"]
	b.down.Store(true)
	pt.test("b")
	dials := b.dials.Load()

	pt.group.findAliveProxy(true)
	require.Eventually(t, func() bool { return !pt.retestRunning() && b.dials.Load() == dials+1 }, 5*time.Second, 20*time.Millisecond)
	for i := 0; i < 5; i++ {
		require.Equal(t, "a", pt.group.findAliveProxy(true).Name())
	}
	require.Equal(t, dials+1, b.dials.Load(), "no other test before the backoff runs out")
	require.Equal(t, pinRetestFirst, pt.group.pinRetest.backoff)

	// the backoff runs out
	pt.group.pinRetest.mutex.Lock()
	pt.group.pinRetest.next = time.Now()
	pt.group.pinRetest.mutex.Unlock()
	pt.group.findAliveProxy(true)
	require.Eventually(t, func() bool { return !pt.retestRunning() && b.dials.Load() == dials+2 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 2*pinRetestFirst, pt.group.pinRetest.backoff, "the backoff grows while the proxy stays out")

	b.down.Store(false)
	pt.test("b")
	require.Equal(t, "b", pt.group.findAliveProxy(true).Name())
	require.Zero(t, pt.group.pinRetest.backoff, "the backoff starts over once the proxy is back")
}

func TestFallbackWithoutAPinUsesTheFirstAliveProxy(t *testing.T) {
	pt := newPinTest(t, "a", "b")
	require.NoError(t, pt.group.Set("b"))
	pt.group.ForceSet("")
	require.Equal(t, "a", pt.group.Now())

	pt.adapters["a"].down.Store(true)
	pt.test("a")
	require.Equal(t, "b", pt.group.Now())
}
