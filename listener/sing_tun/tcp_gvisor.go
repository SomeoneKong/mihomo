//go:build with_gvisor

package sing_tun

import (
	"context"
	"net"
	"sync"

	"github.com/metacubex/gvisor/pkg/tcpip/adapters/gonet"
	M "github.com/metacubex/sing/common/metadata"
)

// newConnection hands a TCP connection to the tunnel. The gVisor stack has
// already completed the handshake with the application, so when the tunnel
// reports that it could not connect to the destination, the connection is left
// open and the failure returned: sing-tun then resets it, and the application
// sees the failure instead of a clean end of stream.
func (h *ListenerHandler) newConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	tcpConn, ok := conn.(*gonet.TCPConn)
	if !ok {
		return h.ListenerHandler.NewConnection(ctx, conn, metadata)
	}
	c := &gvisorTCPConn{TCPConn: tcpConn}
	err := h.ListenerHandler.NewConnection(ctx, c, metadata)
	if failure := c.failure(); failure != nil {
		return failure
	}
	return err
}

type gvisorTCPConn struct {
	*gonet.TCPConn
	access sync.Mutex
	err    error
}

// HandshakeFailure implements N.HandshakeFailure.
func (c *gvisorTCPConn) HandshakeFailure(err error) error {
	c.access.Lock()
	defer c.access.Unlock()
	if c.err == nil {
		c.err = err
	}
	return nil
}

func (c *gvisorTCPConn) failure() error {
	c.access.Lock()
	defer c.access.Unlock()
	return c.err
}

// Close leaves a failed connection to the reset, which a graceful close
// would precede.
func (c *gvisorTCPConn) Close() error {
	if c.failure() != nil {
		return nil
	}
	return c.TCPConn.Close()
}

func (c *gvisorTCPConn) Upstream() any {
	return c.TCPConn
}
