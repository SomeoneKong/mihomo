package outboundgroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// delayProxy is a proxy with a test result set by the test.
type delayProxy struct {
	C.Proxy
	name  string
	alive bool
	delay uint16 // 0 for none yet
}

func (p *delayProxy) Name() string                { return p.name }
func (p *delayProxy) AliveForTestUrl(string) bool { return p.alive }
func (p *delayProxy) LastDelayForTestUrl(string) uint16 {
	if !p.alive || p.delay == 0 {
		return 0xffff
	}
	return p.delay
}

// listProvider provides the proxies the test sets.
type listProvider struct {
	P.ProxyProvider
	proxies []C.Proxy
	version uint32
}

func (p *listProvider) Proxies() []C.Proxy { return p.proxies }
func (p *listProvider) Version() uint32    { return p.version }
func (p *listProvider) Touch()             {}

func (p *listProvider) replace(proxies ...C.Proxy) {
	p.proxies = proxies
	p.version++
}

func newDelayURLTest(t *testing.T, pd *listProvider, tolerance uint16) *URLTest {
	t.Helper()
	u, err := NewURLTest(GroupCommonOption{Name: "auto"}, URLTestOption{Tolerance: tolerance},
		adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	require.NoError(t, err)
	return u
}

func TestURLTestKeepsTheFirstProxyWithinTolerance(t *testing.T) {
	first := &delayProxy{name: "first", alive: true, delay: 100}
	second := &delayProxy{name: "second", alive: true, delay: 200}
	pd := &listProvider{proxies: []C.Proxy{first, second}, version: 1}
	u := newDelayURLTest(t, pd, 150)
	require.Equal(t, "first", u.fast(false).Name())

	first.delay = 300
	u.fastSingle.Reset()
	require.Equal(t, "first", u.fast(false).Name(), "slower by less than the tolerance")

	first.delay = 400
	u.fastSingle.Reset()
	require.Equal(t, "second", u.fast(false).Name(), "slower by more than the tolerance")
}

func TestURLTestFollowsTheSelectedProxyAcrossAProviderUpdate(t *testing.T) {
	a := &delayProxy{name: "a", alive: true, delay: 100}
	b := &delayProxy{name: "b", alive: true, delay: 200}
	pd := &listProvider{proxies: []C.Proxy{b, a}, version: 1}
	u := newDelayURLTest(t, pd, 150)
	require.Same(t, a, u.fast(false))

	// The update creates new proxies, not tested yet: the selection stays on a.
	newA := &delayProxy{name: "a", alive: true}
	newB := &delayProxy{name: "b", alive: true}
	pd.replace(newB, newA)
	u.fastSingle.Reset()
	require.Same(t, newA, u.fast(false))

	// Only some are tested so far.
	newB.delay = 200
	u.fastSingle.Reset()
	require.Same(t, newA, u.fast(false), "the selected proxy is not tested yet")

	// The new a fails its test; the old one, which nothing tests any more, still looks fine.
	newA.alive = false
	u.fastSingle.Reset()
	require.Same(t, newB, u.fast(false))
}

func TestURLTestSelectsByTheDelaysOfAGroupTest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	url := server.URL + "/generate_204"

	a := &flakyAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: "a"})}
	b := &flakyAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: "b"})}
	members := []C.Proxy{adapter.NewProxy(a), adapter.NewProxy(b)}
	pd, err := provider.NewCompatibleProvider("auto", members, provider.NewHealthCheck(members, url, 2000, 0, true, nil))
	require.NoError(t, err)
	u, err := NewURLTest(GroupCommonOption{Name: "auto", URL: url}, URLTestOption{},
		adapter.NewProxy(outbound.NewCompatible()), []P.ProxyProvider{pd})
	require.NoError(t, err)

	saved := probe.Default
	probe.Default = probe.New(time.Now, nil)
	defer func() { probe.Default = saved }()

	// a selection caches a, as a dial or a look at the group while the test runs does
	require.Equal(t, "a", u.Now())
	a.down.Store(true)
	ctx := WithManualTimeout(context.Background(), 2*time.Second)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	delays, err := GroupManualTest(ctx, u, url, nil)
	require.NoError(t, err)
	require.NotContains(t, delays, "a")
	require.Equal(t, "b", u.Now())
}
