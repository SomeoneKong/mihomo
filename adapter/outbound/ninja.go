package outbound

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/ninja"
	"github.com/metacubex/mihomo/transport/vmess"
	M "github.com/metacubex/sing/common/metadata"
)

type Ninja struct {
	*Base
	option NinjaOption
	client *ninja.Client
}

type NinjaOption struct {
	BasicOption
	Name              string            `proxy:"name"`
	Server            string            `proxy:"server"`
	Port              int               `proxy:"port"`
	Method            string            `proxy:"method"`
	Password          string            `proxy:"password"`
	NodePassword      string            `proxy:"node_password"`
	Network           string            `proxy:"network,omitempty"`
	TLS               bool              `proxy:"tls,omitempty"`
	ServerName        string            `proxy:"servername,omitempty"`
	SkipCertVerify    bool              `proxy:"skip-cert-verify,omitempty"`
	ClientFingerprint string            `proxy:"client-fingerprint,omitempty"`
	UDP               bool              `proxy:"udp,omitempty"`
	Pass              bool              `proxy:"pass,omitempty"`
	PassVersion       int               `proxy:"passversion,omitempty"`
	PassOpts          ninja.PassOptions `proxy:"pass-opts,omitempty"`
	RealityOpts       map[string]any    `proxy:"reality-opts,omitempty"`
}

func ninjaDestination(metadata *C.Metadata) ([]byte, error) {
	var buffer bytes.Buffer
	err := M.SocksaddrSerializer.WriteAddrPort(&buffer, M.ParseSocksaddr(metadata.RemoteAddress()))
	return buffer.Bytes(), err
}

func (n *Ninja) wrap(ctx context.Context, conn net.Conn) (net.Conn, error) {
	if !n.option.TLS {
		return conn, nil
	}
	return vmess.StreamTLSConn(ctx, conn, &vmess.TLSConfig{
		Host: n.option.ServerName, SkipCertVerify: n.option.SkipCertVerify,
		ClientFingerprint: n.option.ClientFingerprint,
	})
}

func (n *Ninja) StreamConnContext(ctx context.Context, conn net.Conn, metadata *C.Metadata) (_ net.Conn, err error) {
	destination, err := ninjaDestination(metadata)
	if err != nil {
		return nil, err
	}
	// The first frame is written here, so the dial context has to be able to interrupt it.
	done := N.SetupContextForConn(ctx, conn)
	defer done(&err)
	conn, err = n.wrap(ctx, conn)
	if err != nil {
		return nil, err
	}
	return n.client.StreamConn(conn, destination)
}

func (n *Ninja) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	conn, err := n.dialer.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return nil, err
	}
	defer func() { safeConnClose(conn, err) }()
	wrapped, err := n.StreamConnContext(ctx, conn, metadata)
	if err != nil {
		return nil, fmt.Errorf("Ninja connect: %w", err)
	}
	return NewConn(wrapped, n), nil
}

func (n *Ninja) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	if !n.option.UDP {
		return nil, C.ErrNotSupport
	}
	if err = n.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}
	destination, err := ninjaDestination(metadata)
	if err != nil {
		return nil, err
	}
	conn, err := n.dialer.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return nil, err
	}
	defer func() { safeConnClose(conn, err) }()
	wrapped, err := n.wrap(ctx, conn)
	if err != nil {
		return nil, err
	}
	return NewPacketConn(n.client.PacketConn(wrapped, destination), n), nil
}

// ProxyInfo implements C.ProxyAdapter
func (n *Ninja) ProxyInfo() C.ProxyInfo {
	info := n.Base.ProxyInfo()
	info.DialerProxy = n.option.DialerProxy
	return info
}

// SupportUOT implements C.ProxyAdapter
func (n *Ninja) SupportUOT() bool {
	return true
}

func NewNinja(option NinjaOption) (*Ninja, error) {
	if option.Network != "" && option.Network != "tcp" {
		return nil, fmt.Errorf("unsupported Ninja network %q", option.Network)
	}
	if option.PassVersion != 0 && option.PassVersion != 1 {
		return nil, fmt.Errorf("unsupported Ninja passversion %d", option.PassVersion)
	}
	if len(option.RealityOpts) != 0 {
		return nil, fmt.Errorf("Ninja REALITY is not supported")
	}
	client, err := ninja.NewClient(ninja.Options{Method: option.Method, Password: option.Password, NodePassword: option.NodePassword, Pass: option.Pass, PassOpts: option.PassOpts})
	if err != nil {
		return nil, err
	}
	if option.ServerName == "" {
		option.ServerName = option.Server
	}
	if option.ClientFingerprint == "" {
		option.ClientFingerprint = "chrome"
	}
	n := &Ninja{Base: NewBase(BaseOption{Name: option.Name, Addr: net.JoinHostPort(option.Server, strconv.Itoa(option.Port)), Type: C.Ninja, UDP: option.UDP, ProviderName: option.ProviderName, TFO: option.TFO, MPTCP: option.MPTCP, Interface: option.Interface, RoutingMark: option.RoutingMark, Prefer: option.IPVersion}), option: option, client: client}
	n.dialer = option.NewDialer(n.DialOptions())
	return n, nil
}
