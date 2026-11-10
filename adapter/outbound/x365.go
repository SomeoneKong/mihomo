package outbound

import (
	"context"
	"fmt"
	"net"
	"strings"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/x365"
)

// X365 carries the X365 request header over the same REALITY + xhttp
// stack as VLESS, so it reuses the VLESS transport and only replaces the
// protocol layer. UDP is plain VLESS UDP, a stream per destination: X365
// servers accept neither XUDP nor packetaddr.
type X365 struct {
	*Vless
	client *x365.Client
}

type X365Option struct {
	BasicOption
	Name              string         `proxy:"name"`
	Server            string         `proxy:"server"`
	Port              int            `proxy:"port"`
	UUID              string         `proxy:"uuid"`
	UDP               bool           `proxy:"udp,omitempty"`
	ALPN              []string       `proxy:"alpn,omitempty"`
	ServerName        string         `proxy:"servername,omitempty"`
	ClientFingerprint string         `proxy:"client-fingerprint,omitempty"`
	RealityOpts       RealityOptions `proxy:"reality-opts,omitempty"`
	XHTTPOpts         XHTTPOptions   `proxy:"xhttp-opts,omitempty"`
}

// StreamConnContext implements C.ProxyAdapter
func (x *X365) StreamConnContext(ctx context.Context, c net.Conn, metadata *C.Metadata) (net.Conn, error) {
	return x.client.StreamConn(c, parseX365Addr(metadata)), nil
}

// DialContext implements C.ProxyAdapter
func (x *X365) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	c, err := x.dialContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %s", x.addr, err.Error())
	}
	defer func(c net.Conn) {
		safeConnClose(c, err)
	}(c)

	c, err = x.StreamConnContext(ctx, c, metadata)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %s", x.addr, err.Error())
	}
	return NewConn(c, x), err
}

// ListenPacketContext implements C.ProxyAdapter
func (x *X365) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	if err = x.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}

	c, err := x.dialContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s connect error: %s", x.addr, err.Error())
	}
	defer func(c net.Conn) {
		safeConnClose(c, err)
	}(c)

	return NewPacketConn(N.NewThreadSafePacketConn(x.packetConn(c, metadata)), x), nil
}

// packetConn carries the packets of metadata over c, framed as in VLESS,
// each with a 2-byte length.
func (x *X365) packetConn(c net.Conn, metadata *C.Metadata) net.PacketConn {
	dst := parseX365Addr(metadata)
	dst.UDP = true
	return x.Vless.client.PacketConn(x.client.StreamConn(c, dst), metadata.UDPAddr())
}

// SupportUOT implements C.ProxyAdapter
func (x *X365) SupportUOT() bool {
	return false
}

func parseX365Addr(metadata *C.Metadata) *x365.DstAddr {
	var addrType byte
	var addr []byte
	switch metadata.AddrType() {
	case C.AtypIPv4:
		addrType = x365.AtypIPv4
		addr = metadata.DstIP.AsSlice()
	case C.AtypIPv6:
		addrType = x365.AtypIPv6
		addr = metadata.DstIP.AsSlice()
	case C.AtypDomainName:
		addrType = x365.AtypDomainName
		addr = make([]byte, len(metadata.Host)+1)
		addr[0] = byte(len(metadata.Host))
		copy(addr[1:], metadata.Host)
	}

	return &x365.DstAddr{
		AddrType: addrType,
		Addr:     addr,
		Port:     metadata.DstPort,
	}
}

func NewX365(option X365Option) (*X365, error) {
	switch option.XHTTPOpts.Mode {
	case "", "auto":
		option.XHTTPOpts.Mode = "stream-one"
	case "stream-one":
	default:
		return nil, fmt.Errorf("x365 only supports xhttp mode stream-one, not %s", option.XHTTPOpts.Mode)
	}
	headers := make(map[string]string, len(option.XHTTPOpts.Headers)+1)
	hasUserAgent := false
	for key, value := range option.XHTTPOpts.Headers {
		headers[key] = value
		if strings.EqualFold(key, "User-Agent") {
			hasUserAgent = true
		}
	}
	if !hasUserAgent {
		headers["User-Agent"] = x365.UserAgent
	}
	option.XHTTPOpts.Headers = headers

	v, err := NewVless(VlessOption{
		BasicOption:       option.BasicOption,
		Name:              option.Name,
		Server:            option.Server,
		Port:              option.Port,
		UUID:              option.UUID,
		UDP:               option.UDP,
		TLS:               true,
		ALPN:              option.ALPN,
		Network:           "xhttp",
		RealityOpts:       option.RealityOpts,
		XHTTPOpts:         option.XHTTPOpts,
		ServerName:        option.ServerName,
		ClientFingerprint: option.ClientFingerprint,
	})
	if err != nil {
		return nil, err
	}
	v.tp = C.X365
	v.xudp = false

	return &X365{
		Vless:  v,
		client: x365.NewClient(option.UUID),
	}, nil
}
