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

// IsClosed reports whether the outbound is closed for good, such as one whose session
// a newer outbound took over, so it can't serve another config.
func IsClosed(adapter C.ProxyAdapter) bool {
	c, ok := unwrap(adapter).(interface{ closed() bool })
	return ok && c.closed()
}

// Published is called for the proxies of a config once the config is in use.
func Published(adapter C.ProxyAdapter) {
	if p, ok := unwrap(adapter).(interface{ published() }); ok {
		p.published()
	}
}

func (w *WireGuard) closed() bool {
	return w.runCtx.Err() != nil
}
