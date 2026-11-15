package tproxy

import (
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/nat"
	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

type localConnTunnel struct {
	C.Tunnel
	nat     C.NatTable
	packets chan struct{}
}

func (t *localConnTunnel) NatTable() C.NatTable { return t.nat }

func (t *localConnTunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {
	packet.Drop()
	t.packets <- struct{}{}
}

func TestLocalConnIsReleasedWhenIdle(t *testing.T) {
	previous := localConnIdleTimeout
	localConnIdleTimeout = 500 * time.Millisecond
	defer func() { localConnIdleTimeout = previous }()

	// client stands for the client's socket, lc for the local conn bound to
	// the destination and connected to the client.
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer client.Close()
	lc, err := net.DialUDP("udp", nil, client.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	defer lc.Close()
	lAddr, rAddr := client.LocalAddr().(*net.UDPAddr).AddrPort(), lc.LocalAddr().(*net.UDPAddr).AddrPort()

	table := nat.New()
	table.GetOrCreate(lAddr.String(), func() C.PacketSender { return nil })
	table.AddForLocalConn(lAddr.String(), rAddr.String(), lc)
	tunnel := &localConnTunnel{nat: table, packets: make(chan struct{}, 8)}
	done := make(chan struct{})
	go func() {
		serveLocalConn(lc, rAddr, lAddr, tunnel)
		close(done)
	}()

	reply := &packet{lAddr: lAddr, tunnel: tunnel}
	for i := 0; i < 6; i++ {
		time.Sleep(localConnIdleTimeout / 5)
		_, err := reply.WriteBack([]byte("reply"), net.UDPAddrFromAddrPort(rAddr))
		require.NoError(t, err)
	}
	select {
	case <-done:
		t.Fatal("the local conn was released while replies went through it")
	default:
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the idle local conn was not released")
	}
	require.Nil(t, table.GetForLocalConn(lAddr.String(), rAddr.String()))
	require.Empty(t, tunnel.packets, "the idle timeout was handed to the tunnel as a packet")
}
