package provider

import (
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

func TestClosedHealthCheckDoesNotTest(t *testing.T) {
	p := adapter.NewProxy(outbound.NewDirect())
	hc := NewHealthCheck([]C.Proxy{p}, "http://127.0.0.1:1/", 1000, 0, false, nil)
	hc.close()

	// e.g. the check of a provider a new config replaced, still scheduled or triggered
	hc.check()
	if len(p.DelayHistory()) != 0 {
		t.Fatal("closed health check tested a proxy")
	}
}
