package statistic

import (
	"context"
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/assert"
)

// fakeConn implements only what a tracker reads when it is created.
type fakeConn struct {
	C.Conn
	chain, providerChain C.Chain
}

func (c *fakeConn) Chains() C.Chain { return c.chain }

func (c *fakeConn) ProviderChains() C.Chain { return c.providerChain }

func (c *fakeConn) RemoteDestination() string { return "" }

func TestUnderlyingConnectionsAreRecorded(t *testing.T) {
	manager := &Manager{}
	ctx, recorder := WithDialerRecorder(context.Background())

	deeper := NewTCPTracker(&fakeConn{chain: C.Chain{"jump"}, providerChain: C.Chain{""}}, manager, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(ctx))
	outer := NewTCPTracker(&fakeConn{chain: C.Chain{"relay-a", "relay"}, providerChain: C.Chain{"sub", ""}}, manager, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(ctx))
	unrelated := NewTCPTracker(&fakeConn{chain: C.Chain{"other"}}, manager, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(context.Background()))

	assert.True(t, deeper.Underlying())
	assert.True(t, outer.Underlying())
	assert.True(t, unrelated.Underlying())
	assert.Equal(t, []DialedLayer{
		{Chain: C.Chain{"jump"}, ProviderChain: C.Chain{""}},
		{Chain: C.Chain{"relay-a", "relay"}, ProviderChain: C.Chain{"sub", ""}},
	}, recorder.Layers())
}
