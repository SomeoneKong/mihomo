package socks

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/component/auth"
	C "github.com/metacubex/mihomo/constant"
	authStore "github.com/metacubex/mihomo/listener/auth"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/transport/socks5"

	"github.com/stretchr/testify/require"
)

type udpTunnel struct {
	C.Tunnel
	packets chan *C.Metadata
}

func (t *udpTunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) { conn.Close() }

func (t *udpTunnel) HandleUDPPacket(packet C.UDPPacket, metadata *C.Metadata) {
	packet.Drop()
	t.packets <- metadata
}

func TestUDPRequiresAssociation(t *testing.T) {
	store := authStore.NewAuthStore(auth.NewAuthenticator([]auth.AuthUser{
		{User: "alice", Pass: "a"},
		{User: "bob", Pass: "b"},
	}))
	tunnel := &udpTunnel{packets: make(chan *C.Metadata, 8)}
	additions := []inbound.Addition{inbound.WithInName("test")}

	tl, err := NewWithConfig(LC.AuthServer{Enable: true, Listen: "127.0.0.1:0", AuthStore: store}, inbound.NewListenConfig(), tunnel, additions...)
	require.NoError(t, err)
	defer tl.Close()
	_, port, err := net.SplitHostPort(tl.Address())
	require.NoError(t, err)
	ul, err := NewUDPWithConfig(LC.AuthServer{Enable: true, Listen: tl.Address(), AuthStore: store}, inbound.NewListenConfig(), tunnel, additions...)
	require.NoError(t, err)
	defer ul.Close()

	client, err := net.Dial("udp", tl.Address())
	require.NoError(t, err)
	defer client.Close()
	send := func(dstPort int) {
		packet, err := socks5.EncodeUDPPacket(socks5.ParseAddr("127.0.0.1:"+strconv.Itoa(dstPort)), []byte("ping"))
		require.NoError(t, err)
		_, err = client.Write(packet)
		require.NoError(t, err)
	}
	receive := func() *C.Metadata {
		select {
		case metadata := <-tunnel.packets:
			return metadata
		case <-time.After(5 * time.Second):
			t.Fatal("no datagram reached the tunnel")
			return nil
		}
	}
	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)
	key := associationKey{port: uint16(portNum), ip: addrPortOf(client.LocalAddr()).Addr()}
	waitAssociation := func(want bool) {
		require.Eventually(t, func() bool {
			_, ok := lookupAssociation(key)
			return ok == want
		}, 5*time.Second, 10*time.Millisecond)
	}
	associate := func(user, pass string) net.Conn {
		conn, err := net.Dial("tcp", tl.Address())
		require.NoError(t, err)
		_, err = socks5.ClientHandshake(conn, socks5.ParseAddr("0.0.0.0:0"), socks5.CmdUDPAssociate, &socks5.User{Username: user, Password: pass})
		require.NoError(t, err)
		waitAssociation(true)
		return conn
	}

	// Datagrams are handled in order, so a later admitted one proves that the
	// earlier one was dropped.
	send(1)
	alice := associate("alice", "a")
	send(2)
	metadata := receive()
	require.Equal(t, uint16(2), metadata.DstPort)
	require.Equal(t, "alice", metadata.InUser)

	alice.Close()
	waitAssociation(false)
	send(3)
	bob := associate("bob", "b")
	defer bob.Close()
	send(4)
	metadata = receive()
	require.Equal(t, uint16(4), metadata.DstPort)
	require.Equal(t, "bob", metadata.InUser)
}
