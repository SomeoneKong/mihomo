package adapter

import "strings"

// TrafficPool returns the traffic pool, the set of nodes sharing one traffic quota, that the
// config puts the proxy in; empty when it has none.
// implements C.TrafficPoolOwner
func (p *Proxy) TrafficPool() string {
	return p.trafficPool
}

// withTrafficPool records the traffic-pool option of a proxy's config on the proxy. It is kept
// on the proxy itself rather than in a table keyed by proxies, which would keep proxies that
// are no longer used from being released.
func withTrafficPool(proxy *Proxy, mapping map[string]any) *Proxy {
	if pool, ok := mapping["traffic-pool"].(string); ok {
		proxy.trafficPool = strings.TrimSpace(pool)
	}
	return proxy
}
