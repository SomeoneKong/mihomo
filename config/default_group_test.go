package config

import (
	"testing"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/adapter/provider"

	"github.com/stretchr/testify/require"
)

func TestAGroupNamedDefaultKeepsItsProvider(t *testing.T) {
	const url = "http://www.gstatic.com/generate_204"
	raw := &RawConfig{
		Proxy: []map[string]any{
			{"name": "a", "type": "socks5", "server": "127.0.0.1", "port": 1080},
			{"name": "b", "type": "socks5", "server": "127.0.0.1", "port": 1081},
		},
		ProxyGroup: []map[string]any{
			{"name": provider.ReservedName, "type": "fallback", "url": url, "interval": 30, "proxies": []string{"a", "b"}},
		},
	}
	proxies, providers, err := parseProxies(raw)
	require.NoError(t, err)

	// the provider of the group, whose health check starts from the map
	pd := providers[provider.ReservedName]
	require.Equal(t, url, pd.HealthCheckURL())
	require.Equal(t, 2, pd.Count())

	global := proxies["GLOBAL"].Adapter().(outboundgroup.ProxyGroup)
	var names []string
	for _, p := range global.Proxies() {
		names = append(names, p.Name())
	}
	require.Contains(t, names, provider.ReservedName)
	require.Contains(t, names, "a")
}
