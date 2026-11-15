package outboundgroup

import (
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
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
