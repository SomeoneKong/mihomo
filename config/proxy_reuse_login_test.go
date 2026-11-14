//go:build with_gvisor && !no_tailscale

package config

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func testTailscaleMapping(stateDir string) map[string]any {
	return map[string]any{"name": "ts", "type": "tailscale", "state-dir": stateDir}
}

// loginRefusedAsReplaced tells whether a login of the proxy is refused because a new config
// replaced it. The context is canceled, so a login that goes on starts nothing.
func loginRefusedAsReplaced(t *testing.T, proxy C.Proxy) bool {
	t.Helper()
	l, ok := outbound.LoginableOf(proxy.Adapter())
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := l.Login(ctx, l.LoginInstance(), "")
	var loginErr *C.LoginError
	require.True(t, errors.As(err, &loginErr))
	return loginErr.Code == C.LoginReplaced
}

func TestPublishingAConfigEndsTheLoginsOfAProxyThatIsNeverReused(t *testing.T) {
	previous := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previous) })
	resetProxyReuse(t)

	// a value of a type the reuse does not copy, as a YAML date decodes to
	mapping := testTailscaleMapping("a")
	mapping["x-created-at"] = time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	first := publishTestConfig(parseTestConfig(t, mapping))["ts"]
	t.Cleanup(func() { _ = first.Close() })
	require.False(t, loginRefusedAsReplaced(t, first))

	publishTestConfig(parseTestConfig(t, testSSMapping()))
	require.True(t, loginRefusedAsReplaced(t, first), "a proxy the config removes")
}

func TestPublishingAConfigEndsTheLoginsOfAProxyWhoseNameIsSpelledOtherwise(t *testing.T) {
	previous := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previous) })
	resetProxyReuse(t)

	mappings := []map[string]any{
		{"NAME": "upper", "type": "tailscale", "state-dir": "upper"},
		{"name": 123, "type": "tailscale", "state-dir": "number"},
		{"Name": "dated", "type": "tailscale", "state-dir": "dated", "x-created-at": time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)},
	}
	published := publishTestConfig(parseTestConfig(t, mappings...))
	var proxies []C.Proxy
	for _, name := range []string{"upper", "123", "dated"} {
		proxy := published[name]
		require.NotNil(t, proxy, name)
		t.Cleanup(func() { _ = proxy.Close() })
		require.False(t, loginRefusedAsReplaced(t, proxy), name)
		proxies = append(proxies, proxy)
	}

	publishTestConfig(parseTestConfig(t, testSSMapping()))
	for _, proxy := range proxies {
		require.True(t, loginRefusedAsReplaced(t, proxy), proxy.Name())
	}
}

func TestPublishingAConfigEndsTheLoginsOfTheProxiesItReplaces(t *testing.T) {
	previous := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previous) })
	resetProxyReuse(t)

	first := publishTestConfig(parseTestConfig(t, testTailscaleMapping("a")))["ts"]
	t.Cleanup(func() { _ = first.Close() })
	require.False(t, loginRefusedAsReplaced(t, first))

	require.Equal(t, first, publishTestConfig(parseTestConfig(t, testTailscaleMapping("a")))["ts"])
	require.False(t, loginRefusedAsReplaced(t, first), "a proxy the config keeps stays in use")

	second := publishTestConfig(parseTestConfig(t, testTailscaleMapping("b")))["ts"]
	t.Cleanup(func() { _ = second.Close() })
	require.NotEqual(t, first, second)
	require.True(t, loginRefusedAsReplaced(t, first), "the replaced proxy")
	require.False(t, loginRefusedAsReplaced(t, second))

	publishTestConfig(parseTestConfig(t, testSSMapping()))
	require.True(t, loginRefusedAsReplaced(t, second), "a proxy the config removes")
}
