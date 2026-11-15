package provider

import (
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// An update of the provider replaces the proxies while a check may be testing them. Run with
// -race.
func TestHealthCheckProxiesReplacedDuringACheck(t *testing.T) {
	var proxies []C.Proxy
	for i := 0; i < 4; i++ {
		proxies = append(proxies, adapter.NewProxy(outbound.NewDirect()))
	}
	// a URL the test cannot fetch, so that it fails without a dial
	hc := NewHealthCheck(proxies[:1], "unsupported://127.0.0.1/", 1000, 0, false, nil)
	defer hc.close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			hc.setProxies(proxies[:i%len(proxies)+1])
		}
	}()
	for i := 0; i < 100; i++ {
		hc.check()
	}
	<-done
}
