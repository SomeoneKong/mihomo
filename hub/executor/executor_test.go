package executor

import (
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel"
)

type closeRecordingProvider struct {
	name   string
	closed bool
}

func (p *closeRecordingProvider) Name() string               { return p.name }
func (p *closeRecordingProvider) VehicleType() P.VehicleType { return P.Compatible }
func (p *closeRecordingProvider) Type() P.ProviderType       { return P.Proxy }
func (p *closeRecordingProvider) Initial() error             { return nil }
func (p *closeRecordingProvider) Update() error              { return nil }
func (p *closeRecordingProvider) Proxies() []C.Proxy         { return nil }
func (p *closeRecordingProvider) Count() int                 { return 0 }
func (p *closeRecordingProvider) Touch()                     {}
func (p *closeRecordingProvider) HealthCheck()               {}
func (p *closeRecordingProvider) Version() uint32            { return 0 }
func (p *closeRecordingProvider) HealthCheckURL() string     { return "" }
func (p *closeRecordingProvider) Close() error               { p.closed = true; return nil }
func (p *closeRecordingProvider) RegisterHealthCheckTask(string, utils.IntRanges[uint16], string, uint) {
}

func TestUpdateProxiesClosesReplacedProviders(t *testing.T) {
	kept := &closeRecordingProvider{name: "kept"}
	replaced := &closeRecordingProvider{name: "sub"}
	removed := &closeRecordingProvider{name: "removed"}
	tunnel.UpdateProxies(nil, map[string]P.ProxyProvider{"kept": kept, "sub": replaced, "removed": removed})
	t.Cleanup(func() { tunnel.UpdateProxies(nil, nil) })

	replacement := &closeRecordingProvider{name: "sub"}
	updateProxies(nil, map[string]P.ProxyProvider{"kept": kept, "sub": replacement})

	if !replaced.closed || !removed.closed {
		t.Fatal("replaced provider was not closed")
	}
	if kept.closed || replacement.closed {
		t.Fatal("provider of the current config was closed")
	}
}
