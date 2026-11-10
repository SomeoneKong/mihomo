package outbound

import (
	C "github.com/metacubex/mihomo/constant"
)

// unwrap returns the outbound an adapter wraps for auto closing or multiplexing.
func unwrap(adapter C.ProxyAdapter) C.ProxyAdapter {
	for {
		switch a := adapter.(type) {
		case *autoCloseProxyAdapter:
			adapter = a.ProxyAdapter
		case *SingMux:
			adapter = a.ProxyAdapter
		default:
			return adapter
		}
	}
}

// IsReusable reports whether the outbound can serve another config: it is not closed
// for good, as one whose session a newer outbound took over, and it did not fail to
// start, which is not retried.
func IsReusable(adapter C.ProxyAdapter) bool {
	r, ok := unwrap(adapter).(interface{ reusable() bool })
	return !ok || r.reusable()
}

// Published is called for the proxies of a config once the config is in use.
func Published(adapter C.ProxyAdapter) {
	if p, ok := unwrap(adapter).(interface{ published() }); ok {
		p.published()
	}
}

func (w *WireGuard) reusable() bool {
	return w.runCtx.Err() == nil && !w.initFailed.Load()
}
