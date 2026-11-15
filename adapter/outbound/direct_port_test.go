package outbound

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListenPacketAvoidsTheSourcePort(t *testing.T) {
	listen := func() (net.PacketConn, error) { return net.ListenPacket("udp", "127.0.0.1:0") }
	taken, err := listen()
	require.NoError(t, err)
	port := uint16(taken.LocalAddr().(*net.UDPAddr).Port)

	// the first listen lands on the source port
	calls := 0
	pc, err := listenPacketAvoiding(func() (net.PacketConn, error) {
		calls++
		if calls == 1 {
			return taken, nil
		}
		return listen()
	}, port)
	require.NoError(t, err)
	defer pc.Close()
	require.Equal(t, 2, calls)
	require.NotEqual(t, int(port), pc.LocalAddr().(*net.UDPAddr).Port)
	_, err = taken.WriteTo([]byte("x"), pc.LocalAddr())
	require.Error(t, err, "the colliding socket is closed")

	other, err := listenPacketAvoiding(listen, port)
	require.NoError(t, err)
	defer other.Close()
	require.NotEqual(t, int(port), other.LocalAddr().(*net.UDPAddr).Port)
}
