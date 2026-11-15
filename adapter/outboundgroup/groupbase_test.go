package outboundgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/provider"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"github.com/stretchr/testify/require"
)

// failingAdapter is a node whose dials fail; once the caller canceled the dial, with the
// error of a socket the cancellation closed.
type failingAdapter struct {
	*outbound.Direct
}

func (a *failingAdapter) Type() C.AdapterType { return C.Vless }

func (a *failingAdapter) DialContext(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
	if ctx.Err() != nil {
		return nil, errors.New("use of closed network connection")
	}
	return nil, errors.New("handshake failed")
}

func TestGroupsDoNotCountCanceledDialsAsFailures(t *testing.T) {
	members := []C.Proxy{adapter.NewProxy(&failingAdapter{Direct: outbound.NewDirect()})}
	pd, err := provider.NewCompatibleProvider("p", members, provider.NewHealthCheck(members, "", 1000, 0, true, nil))
	require.NoError(t, err)
	providers := []P.ProxyProvider{pd}
	fallbackProxy := adapter.NewProxy(outbound.NewCompatible())

	urlTest, err := NewURLTest(GroupCommonOption{Name: "url-test"}, URLTestOption{}, fallbackProxy, providers)
	require.NoError(t, err)
	fallback, err := NewFallback(GroupCommonOption{Name: "fallback"}, FallbackOption{}, fallbackProxy, providers)
	require.NoError(t, err)
	loadBalance, err := NewLoadBalance(GroupCommonOption{Name: "load-balance"}, LoadBalanceOption{Strategy: "round-robin"}, fallbackProxy, providers)
	require.NoError(t, err)

	for _, group := range []struct {
		C.ProxyAdapter
		base *GroupBase
	}{
		{urlTest, urlTest.GroupBase},
		{fallback, fallback.GroupBase},
		{loadBalance, loadBalance.GroupBase},
	} {
		t.Run(group.Name(), func(t *testing.T) {
			metadata := &C.Metadata{NetWork: C.TCP, Host: "example.com", DstPort: 443}
			failures := func() int {
				group.base.failedTestMux.Lock()
				defer group.base.failedTestMux.Unlock()
				return group.base.failedTimes
			}

			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			for i := 0; i < 2; i++ {
				_, err := group.DialContext(canceled, metadata)
				require.Error(t, err)
			}
			_, err := group.DialContext(context.Background(), metadata)
			require.Error(t, err)

			require.Eventually(t, func() bool { return failures() > 0 }, time.Second, time.Millisecond)
			time.Sleep(50 * time.Millisecond) // the failures are counted on their own goroutines
			require.Equal(t, 1, failures(), "only the dial its caller did not cancel counts")
		})
	}
}
