//go:build with_gvisor

package sing_tun

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/sing"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/gvisor/pkg/tcpip"
	"github.com/metacubex/gvisor/pkg/tcpip/adapters/gonet"
	"github.com/metacubex/gvisor/pkg/tcpip/link/channel"
	"github.com/metacubex/gvisor/pkg/tcpip/network/ipv4"
	"github.com/metacubex/gvisor/pkg/tcpip/stack"
	"github.com/metacubex/gvisor/pkg/tcpip/transport/tcp"
	tun "github.com/metacubex/sing-tun"
	"github.com/stretchr/testify/require"
)

type failingDirect struct{ *outbound.Base }

func (failingDirect) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, resolver.ErrIPNotFound // not retried
}

func TestGVisorResetsWhenDialFails(t *testing.T) {
	oldMode, oldStatus, oldProxies, oldProviders := tunnel.Mode(), tunnel.Status(), tunnel.Proxies(), tunnel.Providers()
	tunnel.UpdateProxies(map[string]C.Proxy{
		"DIRECT": adapter.NewProxy(failingDirect{outbound.NewBase(outbound.BaseOption{Name: "DIRECT", Type: C.Direct})}),
	}, nil)
	tunnel.SetMode(tunnel.Direct)
	tunnel.OnRunning()
	defer func() {
		tunnel.UpdateProxies(oldProxies, oldProviders)
		tunnel.SetMode(oldMode)
		if oldStatus != tunnel.Running {
			tunnel.OnSuspend()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The application's stack and the TUN stack, linked back to back.
	appLink, tunLink := channel.New(64, 1500, ""), channel.New(64, 1500, "")
	appStack, err := tun.NewGVisorStack(appLink)
	require.NoError(t, err)
	defer appStack.Close()
	tunStack, err := tun.NewGVisorStack(tunLink)
	require.NoError(t, err)
	defer tunStack.Close()
	base, err := sing.NewListenerHandler(sing.ListenerConfig{Tunnel: tunnel.Tunnel, Type: C.TUN})
	require.NoError(t, err)
	tunStack.SetTransportProtocolHandler(tcp.ProtocolNumber, tun.NewTCPForwarder(ctx, tunStack, &ListenerHandler{ListenerHandler: base}).HandlePacket)
	require.Nil(t, appStack.AddProtocolAddress(tun.DefaultNIC, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: tun.AddressFromAddr(netip.MustParseAddr("192.0.2.1")), PrefixLen: 32},
	}, stack.AddressProperties{}))
	forward := func(from, to *channel.Endpoint) {
		for {
			packet := from.ReadContext(ctx)
			if packet == nil {
				return
			}
			incoming := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: packet.ToBuffer()})
			to.InjectInbound(packet.NetworkProtocolNumber, incoming)
			incoming.DecRef()
			packet.DecRef()
		}
	}
	go forward(appLink, tunLink)
	go forward(tunLink, appLink)

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()
	conn, err := gonet.DialContextTCP(dialCtx, appStack, tcpip.FullAddress{
		NIC: tun.DefaultNIC, Addr: tun.AddressFromAddr(netip.MustParseAddr("198.51.100.1")), Port: 443,
	}, ipv4.ProtocolNumber)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	require.False(t, errors.Is(err, io.EOF), "the failed dial reached the application as a clean end of stream")
	require.ErrorContains(t, err, "reset")
}
