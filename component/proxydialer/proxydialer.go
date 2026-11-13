package proxydialer

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

type proxyDialer struct {
	proxy        C.ProxyAdapter
	statistic    bool
	withoutUsage bool
}

func New(proxy C.ProxyAdapter, statistic bool) C.Dialer {
	return proxyDialer{proxy: proxy, statistic: statistic}
}

// NewWithoutUsage is New for the transports a proxy multiplexes its own connections over:
// their trackers still show in the connection list, but are left out of the per-proxy
// usage, which already counts the connections they carry.
func NewWithoutUsage(proxy C.ProxyAdapter, statistic bool) C.Dialer {
	return proxyDialer{proxy: proxy, statistic: statistic, withoutUsage: true}
}

func (p proxyDialer) trackerOptions(ctx context.Context) []statistic.TrackerOption {
	options := []statistic.TrackerOption{statistic.AsUnderlying(ctx)}
	if p.withoutUsage {
		options = append(options, statistic.WithoutUsage())
	}
	return options
}

func (p proxyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	currentMeta := &C.Metadata{Type: C.INNER}
	if err := currentMeta.SetRemoteAddress(address); err != nil {
		return nil, err
	}
	if strings.Contains(network, "udp") { // using in wireguard outbound
		pc, err := p.listenPacket(ctx, currentMeta)
		if err != nil {
			return nil, err
		}
		if !currentMeta.Resolved() { // should not happen, maybe by a wrongly implemented proxy, but we can handle this (:
			err = pc.ResolveUDP(ctx, currentMeta)
			if err != nil {
				_ = pc.Close()
				return nil, err
			}
		}
		return N.NewBindPacketConn(pc, currentMeta.UDPAddr()), nil
	}
	conn, err := p.proxy.DialContext(ctx, currentMeta)
	if err != nil {
		return nil, err
	}
	if p.statistic {
		conn = statistic.NewTCPTracker(conn, statistic.DefaultManager, currentMeta, nil, 0, 0, false, p.trackerOptions(ctx)...)
	}
	return conn, err
}

func (p proxyDialer) ListenPacket(ctx context.Context, network, address string, rAddrPort netip.AddrPort) (net.PacketConn, error) {
	if !strings.HasPrefix(network, "udp") {
		return nil, fmt.Errorf("proxyDialer only support udp network, but got: %s", network)
	}
	currentMeta := &C.Metadata{Type: C.INNER, DstIP: rAddrPort.Addr(), DstPort: rAddrPort.Port()}
	return p.listenPacket(ctx, currentMeta)
}

func (p proxyDialer) listenPacket(ctx context.Context, currentMeta *C.Metadata) (C.PacketConn, error) {
	currentMeta.NetWork = C.UDP
	pc, err := p.proxy.ListenPacketContext(ctx, currentMeta)
	if err != nil {
		return nil, err
	}
	if p.statistic {
		pc = statistic.NewUDPTracker(pc, statistic.DefaultManager, currentMeta, nil, 0, 0, false, p.trackerOptions(ctx)...)
	}
	return pc, nil
}
