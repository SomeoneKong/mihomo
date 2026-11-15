package tproxy

import (
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/common/pool"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// localConnIdleTimeout is how long a local conn may pass no packet either way
// before it is released, the same as the idle timeout of a UDP session.
var localConnIdleTimeout = 60 * time.Second

// serveLocalConn hands what the client sends to rAddr, which arrives on lc, to
// the tunnel. lc serves one destination of the client's UDP session, which
// lasts as long as any of its destinations is active, so lc is released on its
// own once it is idle or fails; a later reply from rAddr opens a new one.
func serveLocalConn(lc *net.UDPConn, rAddr, lAddr netip.AddrPort, tunnel C.Tunnel, additions ...inbound.Addition) {
	log.Debugln("TProxy listenLocalConn rAddr=%s lAddr=%s", rAddr, lAddr)
	for {
		_ = lc.SetReadDeadline(time.Now().Add(localConnIdleTimeout))
		buf := pool.Get(pool.UDPBufferSize)
		br, err := lc.Read(buf)
		if err != nil {
			pool.Put(buf)
			if !errors.Is(err, net.ErrClosed) {
				// Removed before closing, so that a write back that finds lc
				// closed gets a new conn instead of lc again.
				natTable := tunnel.NatTable()
				local, remote := lAddr.String(), rAddr.String()
				if natTable.GetForLocalConn(local, remote) == lc {
					natTable.DeleteForLocalConn(local, remote)
				}
				_ = lc.Close()
			}
			log.Debugln("TProxy local conn listener exit.. rAddr=%s lAddr=%s", rAddr, lAddr)
			return
		}
		// since following localPackets are pass through this socket which listen rAddr
		// I choose current listener as packet's packet conn
		handlePacketConn(lc, tunnel, buf[:br], lAddr, rAddr, additions...)
	}
}
