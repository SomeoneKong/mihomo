package outbound

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"

	D "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

// goneResolver knows no names: only IP literals resolve.
type goneResolver struct{}

func (goneResolver) lookup(host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	return nil, resolver.ErrIPNotFound
}

func (r goneResolver) LookupIP(_ context.Context, host string) ([]netip.Addr, error) {
	return r.lookup(host)
}

func (r goneResolver) LookupIPv4(_ context.Context, host string) ([]netip.Addr, error) {
	return r.lookup(host)
}

func (r goneResolver) LookupIPv6(_ context.Context, host string) ([]netip.Addr, error) {
	return r.lookup(host)
}

func (goneResolver) ResolveECH(context.Context, string) ([]byte, error) { return nil, nil }

func (goneResolver) ExchangeContext(context.Context, *D.Msg) (*D.Msg, error) {
	return nil, errors.New("not implemented")
}

func (goneResolver) Invalid() bool { return true }

func (goneResolver) ClearCache() {}

func (goneResolver) ResetConnection() {}

func TestDirectDialsMappedIPWhenHostDoesNotResolve(t *testing.T) {
	previous := resolver.DirectHostResolver
	resolver.DirectHostResolver = goneResolver{}
	defer func() { resolver.DirectHostResolver = previous }()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	target := netip.MustParseAddrPort(listener.Addr().String())

	dial := func(dnsMode C.DNSMode) error {
		conn, err := NewDirect().DialContext(context.Background(), &C.Metadata{
			NetWork: C.TCP,
			Host:    "gone.example.com",
			DstIP:   target.Addr(),
			DstPort: target.Port(),
			DNSMode: dnsMode,
		})
		if err == nil {
			conn.Close()
		}
		return err
	}
	require.NoError(t, dial(C.DNSMapping), "the client dialed the IP, whose host is only known from the mapping")
	require.ErrorIs(t, dial(C.DNSNormal), resolver.ErrIPNotFound, "a host the client asked for by name is not replaced")
}
