package outboundgroup

import (
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

func TestParseProxyGroupRejectsAnInvalidFilter(t *testing.T) {
	for _, option := range []string{"filter", "exclude-filter"} {
		for _, includeAll := range []bool{false, true} {
			proxies := map[string]C.Proxy{
				"DIRECT":     adapter.NewProxy(outbound.NewDirect()),
				"COMPATIBLE": adapter.NewProxy(outbound.NewCompatible()),
			}
			config := map[string]any{
				"name":                "g",
				"type":                "url-test",
				"proxies":             []string{"DIRECT"},
				"include-all-proxies": includeAll,
				option:                "valid`(",
			}
			_, err := ParseProxyGroup(config, proxies, map[string]P.ProxyProvider{}, []string{"DIRECT"}, nil)
			require.ErrorContains(t, err, "invalid "+option, "include-all-proxies: %v", includeAll)
		}
	}
}
