package tunnel

import (
	"net"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/socks5"

	"github.com/stretchr/testify/require"
)

type packetTunnel struct {
	C.Tunnel
	packets []C.PacketAdapter
}

func (t *packetTunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {
	t.packets = append(t.packets, C.NewPacketAdapter(packet, metadata))
}

func TestUDPSessionsAreScopedToTheListener(t *testing.T) {
	listen := func() net.PacketConn {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { pc.Close() })
		return pc
	}
	pcA, pcB := listen(), listen()
	listenerA := &PacketConn{target: socks5.ParseAddr("198.51.100.1:51820")}
	listenerB := &PacketConn{target: socks5.ParseAddr("198.51.100.2:51820")}
	source := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 40000}
	tunnel := &packetTunnel{}

	listenerA.handleUDP(pcA, tunnel, []byte("a"), source)
	listenerB.handleUDP(pcB, tunnel, []byte("b"), source)
	listenerA.handleUDP(pcA, tunnel, []byte("c"), source)

	require.Len(t, tunnel.packets, 3)
	require.NotEqual(t, tunnel.packets[0].Key(), tunnel.packets[1].Key(), "the same client is one session per listener")
	require.Equal(t, tunnel.packets[0].Key(), tunnel.packets[2].Key())
	for _, packet := range tunnel.packets {
		require.Equal(t, source.String(), packet.Metadata().SourceAddress())
	}
}
