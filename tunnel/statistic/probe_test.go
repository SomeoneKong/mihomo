package statistic

import (
	"context"
	"sync/atomic"
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/assert"
)

type countingBinding struct {
	renewals atomic.Int32
}

func (b *countingBinding) Renew() { b.renewals.Add(1) }

// boundConn is a layer a group handed out, carrying the group's binding.
type boundConn struct {
	C.Conn
	binding C.ActivityBinding
}

func (c *boundConn) Upstream() any                      { return c.Conn }
func (c *boundConn) ActivityBinding() C.ActivityBinding { return c.binding }

func TestReceivedDataRenewsTheGroupsOfEveryLayer(t *testing.T) {
	useTestUsage(t)
	inner, outer := &countingBinding{}, &countingBinding{}
	conn := outboundConn("HK 01", "", C.Vmess, "hk1.example.com:443")
	layered := &boundConn{Conn: &boundConn{Conn: conn, binding: inner}, binding: outer}
	tracker := NewTCPTracker(layered, &Manager{}, &C.Metadata{}, nil, 0, 0, false)

	_, _ = tracker.Write(make([]byte, 10))
	assert.Zero(t, inner.renewals.Load(), "sending renews nothing")
	_, _ = tracker.Read(make([]byte, 10))
	_, _ = tracker.Read(make([]byte, 10))
	assert.EqualValues(t, 1, inner.renewals.Load(), "renewals are at most one a second")
	assert.EqualValues(t, 1, outer.renewals.Load())
}

func TestDataOfAProbeRenewsNothing(t *testing.T) {
	useTestUsage(t)
	binding := &countingBinding{}
	conn := &boundConn{Conn: outboundConn("HK 01", "", C.Vmess, "hk1.example.com:443"), binding: binding}
	ctx, _ := WithDialerRecorder(C.WithProbe(context.Background()))
	tracker := NewTCPTracker(conn, &Manager{}, &C.Metadata{}, nil, 0, 0, false, AsUnderlying(ctx))
	_, _ = tracker.Read(make([]byte, 10))
	assert.Zero(t, binding.renewals.Load())
}
