//go:build with_gvisor && !no_tailscale

package outbound_test

import (
	"context"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func TestLoginableOfAParsedProxy(t *testing.T) {
	previous := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(previous) })

	ts, err := adapter.ParseProxy(map[string]any{"name": "ts", "type": "tailscale", "state-dir": "state"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ts.Close() })
	l, ok := outbound.LoginableOf(ts.Adapter())
	require.True(t, ok, "found through the wrappers of the parsed proxy")
	require.Equal(t, C.LoginNotStarted, l.LoginStatus(context.Background()).State)

	direct, err := adapter.ParseProxy(map[string]any{"name": "d", "type": "direct"})
	require.NoError(t, err)
	_, ok = outbound.LoginableOf(direct.Adapter())
	require.False(t, ok)
}
