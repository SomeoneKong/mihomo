package route

import (
	"context"
	"encoding/json"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConn implements only what a tracker reads when it is created.
type fakeConn struct {
	C.Conn
	chain C.Chain
}

func (c *fakeConn) Chains() C.Chain { return c.chain }

func (c *fakeConn) ProviderChains() C.Chain { return nil }

func (c *fakeConn) RemoteDestination() string { return "" }

func TestConnectionsLeaveOutUnderlying(t *testing.T) {
	manager := statistic.DefaultManager
	carried := statistic.NewTCPTracker(&fakeConn{chain: C.Chain{"hk", "group"}}, manager, &C.Metadata{}, nil, 0, 0, true,
		statistic.WithDialerChains([]C.Chain{{"relay-node", "relay"}}))
	underlying := statistic.NewTCPTracker(&fakeConn{chain: C.Chain{"relay-node", "relay"}}, manager, &C.Metadata{Type: C.INNER}, nil, 0, 0, false,
		statistic.AsUnderlying(context.Background()))
	t.Cleanup(func() {
		manager.Leave(carried)
		manager.Leave(underlying)
	})

	ids := func(query string) map[string][]C.Chain {
		recorder := httptest.NewRecorder()
		getConnections(recorder, httptest.NewRequest("GET", "/connections"+query, nil))
		var snapshot struct {
			Connections []struct {
				ID           string    `json:"id"`
				DialerChains []C.Chain `json:"dialerChains"`
			} `json:"connections"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &snapshot))
		found := map[string][]C.Chain{}
		for _, connection := range snapshot.Connections {
			found[connection.ID] = connection.DialerChains
		}
		return found
	}

	listed := ids("")
	assert.Contains(t, listed, carried.ID())
	assert.NotContains(t, listed, underlying.ID())
	assert.Equal(t, []C.Chain{{"relay-node", "relay"}}, listed[carried.ID()])

	all := ids("?underlying=true")
	assert.Contains(t, all, carried.ID())
	assert.Contains(t, all, underlying.ID())
	assert.Nil(t, all[underlying.ID()])
}
