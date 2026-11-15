package adapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

func TestParseProxyKeepsTheTrafficPool(t *testing.T) {
	proxy, err := ParseProxy(map[string]any{"name": "d", "type": "direct", "traffic-pool": " jinkela "})
	require.NoError(t, err)
	owner, ok := proxy.(C.TrafficPoolOwner)
	require.True(t, ok)
	require.Equal(t, "jinkela", owner.TrafficPool())

	proxy, err = ParseProxy(map[string]any{"name": "d", "type": "direct"})
	require.NoError(t, err)
	require.Empty(t, proxy.(C.TrafficPoolOwner).TrafficPool())
}

func TestProbeURLReportsOneTest(t *testing.T) {
	status := http.StatusNoContent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	defer server.Close()
	proxy, err := ParseProxy(map[string]any{"name": "d", "type": "direct"})
	require.NoError(t, err)
	tester := proxy.(C.ProbeTester)
	expected, err := utils.NewUnsignedRanges[uint16]("204")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := tester.ProbeURL(ctx, server.URL, expected)
	require.True(t, result.Satisfied)
	require.Equal(t, http.StatusNoContent, result.Status)
	require.NotZero(t, result.Delay)
	require.True(t, proxy.AliveForTestUrl(server.URL))

	status = http.StatusInternalServerError
	result = tester.ProbeURL(ctx, server.URL, expected)
	require.False(t, result.Satisfied)
	require.Equal(t, C.ProbeErrorStatus, result.ErrorClass)
	require.False(t, proxy.AliveForTestUrl(server.URL))
	// URLTest still reports the delay of a response with an unexpected status.
	delay, err := proxy.URLTest(ctx, server.URL, expected)
	require.NoError(t, err)
	require.NotZero(t, delay)

	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	result = tester.ProbeURL(canceled, server.URL, expected)
	require.True(t, result.Canceled)
}

func TestProbeURLWithUnifiedDelayFailsWhenTheSecondRequestTimesOut(t *testing.T) {
	saved := UnifiedDelay.Load()
	UnifiedDelay.Store(true)
	defer UnifiedDelay.Store(saved)
	requests := 0
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests > 1 {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	defer close(release)
	proxy, err := ParseProxy(map[string]any{"name": "d", "type": "direct"})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	result := proxy.(C.ProbeTester).ProbeURL(ctx, server.URL, nil)
	require.False(t, result.Satisfied)
	require.Equal(t, C.ProbeErrorTimeout, result.ErrorClass)
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestIsTimeoutFindsANetworkTimeout(t *testing.T) {
	require.True(t, isTimeout(fmt.Errorf("tls: %w", timeoutError{})))
	require.False(t, isTimeout(errors.New("refused")))
}
