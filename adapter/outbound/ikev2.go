package outbound

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/contextutils"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/dns"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/ikev2"

	"golang.org/x/sync/semaphore"
)

const ikev2DefaultMTU = 1400

type IKEv2 struct {
	*Base
	option *IKEv2Option
	config *ikev2.Config

	tunDevice ipStack
	client    *ikev2.Client
	resolver  resolver.Resolver
	dns       []dns.NameServer

	runCtx    context.Context
	runCancel context.CancelFunc
	runLock   *semaphore.Weighted
	running   bool
}

type IKEv2Option struct {
	BasicOption
	Name             string   `proxy:"name"`
	Server           string   `proxy:"server"`
	Port             int      `proxy:"port,omitempty"`
	PSK              string   `proxy:"psk"`
	Username         string   `proxy:"username,omitempty"`
	Password         string   `proxy:"password,omitempty"`
	LocalID          string   `proxy:"local-id,omitempty"`
	IKEProposals     []string `proxy:"ike-proposals,omitempty"`
	ESPProposals     []string `proxy:"esp-proposals,omitempty"`
	IKELifetime      int      `proxy:"ike-lifetime,omitempty"`
	ESPLifetime      int      `proxy:"esp-lifetime,omitempty"`
	DPDInterval      int      `proxy:"dpd-interval,omitempty"`
	HandshakeTimeout int      `proxy:"handshake-timeout,omitempty"`
	MTU              int      `proxy:"mtu,omitempty"`
	UDP              bool     `proxy:"udp,omitempty"`

	IPStack IPStackOption `proxy:"ip-stack,omitempty"`

	RemoteDnsResolve bool     `proxy:"remote-dns-resolve,omitempty"`
	Dns              []string `proxy:"dns,omitempty"`
}

func NewIKEv2(option IKEv2Option) (*IKEv2, error) {
	if option.HandshakeTimeout < 0 {
		return nil, errors.New("ikev2 handshake timeout must be non-negative")
	}
	if option.Port == 0 {
		option.Port = ikev2.DefaultPort
	}
	if option.Port < 0 || option.Port > 65535 {
		return nil, errors.New("ikev2 port is out of range")
	}
	option.IPStack.normalize()
	if err := option.IPStack.validate(); err != nil {
		return nil, err
	}
	cfg := &ikev2.Config{
		PSK:          option.PSK,
		Username:     option.Username,
		Password:     option.Password,
		LocalID:      option.LocalID,
		Port:         uint16(option.Port),
		IKEProposals: option.IKEProposals,
		ESPProposals: option.ESPProposals,
		IKELifetime:  time.Duration(option.IKELifetime) * time.Second,
		ESPLifetime:  time.Duration(option.ESPLifetime) * time.Second,
		DPDInterval:  time.Duration(option.DPDInterval) * time.Second,
	}
	if err := cfg.Prepare(); err != nil {
		return nil, err
	}

	outbound := &IKEv2{
		Base: NewBase(BaseOption{
			Name:         option.Name,
			Addr:         net.JoinHostPort(option.Server, strconv.Itoa(option.Port)),
			Type:         C.IKEv2,
			ProviderName: option.ProviderName,
			UDP:          true,
			TFO:          option.TFO,
			MPTCP:        option.MPTCP,
			Interface:    option.Interface,
			RoutingMark:  option.RoutingMark,
			Prefer:       option.IPVersion,
		}),
		option:  &option,
		config:  cfg,
		runLock: semaphore.NewWeighted(1),
	}
	if option.RemoteDnsResolve && len(option.Dns) > 0 {
		nss, err := dns.ParseNameServer(option.Dns)
		if err != nil {
			return nil, err
		}
		outbound.dns = nss
	}
	outbound.dialer = option.NewDialer(outbound.DialOptions())
	outbound.runCtx, outbound.runCancel = context.WithCancel(context.Background())
	return outbound, nil
}

func (o *IKEv2) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	tunDevice, r, err := o.run(ctx)
	if err != nil {
		return nil, err
	}
	var conn net.Conn
	if !metadata.Resolved() || r != nil {
		if r == nil {
			r = resolver.DefaultResolver
		}
		options := o.DialOptions()
		options = append(options, dialer.WithResolver(r))
		options = append(options, dialer.WithNetDialer(ipStackNetDialer{stack: tunDevice}))
		conn, err = dialer.NewDialer(options...).DialContext(ctx, "tcp", metadata.RemoteAddress())
	} else {
		conn, err = tunDevice.DialTCP(ctx, "tcp", netip.AddrPort{}, metadata.AddrPort())
	}
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("conn is nil")
	}
	return NewConn(conn, o), nil
}

func (o *IKEv2) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	tunDevice, r, err := o.run(ctx)
	if err != nil {
		return nil, err
	}
	if err = o.resolveUDP(ctx, metadata, r); err != nil {
		return nil, err
	}
	pc, err := tunDevice.ListenUDP(ctx, "udp", netip.AddrPort{})
	if err != nil {
		return nil, err
	}
	if pc == nil {
		return nil, errors.New("packetConn is nil")
	}
	return NewPacketConn(pc, o), nil
}

func (o *IKEv2) ResolveUDP(ctx context.Context, metadata *C.Metadata) error {
	_, r, err := o.run(ctx)
	if err != nil {
		return err
	}
	return o.resolveUDP(ctx, metadata, r)
}

func (o *IKEv2) resolveUDP(ctx context.Context, metadata *C.Metadata, r resolver.Resolver) error {
	if (!metadata.Resolved() || r != nil) && metadata.Host != "" {
		if r == nil {
			r = resolver.DefaultResolver
		}
		ip, err := resolveIPWithResolver(ctx, metadata.Host, o.prefer, r)
		if err != nil {
			return fmt.Errorf("can't resolve ip: %w", err)
		}
		metadata.DstIP = ip
	}
	return nil
}

func (o *IKEv2) ProxyInfo() C.ProxyInfo {
	info := o.Base.ProxyInfo()
	info.DialerProxy = o.option.DialerProxy
	return info
}

func (o *IKEv2) IsL3Protocol(metadata *C.Metadata) bool {
	return true
}

func (o *IKEv2) Close() error {
	if o.runCancel != nil {
		o.runCancel()
	}
	_ = o.runLock.Acquire(context.Background(), 1)
	client := o.client
	tunDevice := o.tunDevice
	o.client = nil
	o.tunDevice = nil
	o.running = false
	o.runLock.Release(1)

	if client != nil {
		_ = client.Close()
	}
	if tunDevice != nil {
		return tunDevice.Close()
	}
	return nil
}

// run starts the tunnel on first use and again after it went down.
func (o *IKEv2) run(ctx context.Context) (ipStack, resolver.Resolver, error) {
	runCtx, cancel := context.WithCancel(ctx)
	stop := contextutils.AfterFunc(o.runCtx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	if err := o.runLock.Acquire(runCtx, 1); err != nil {
		return nil, nil, err
	}
	releaseRunLock := true
	defer func() {
		if releaseRunLock {
			o.runLock.Release(1)
		}
	}()

	if o.running {
		if o.tunDevice == nil {
			return nil, nil, net.ErrClosed
		}
		return o.tunDevice, o.resolver, nil
	}
	if o.runCtx.Err() != nil {
		return nil, nil, o.runCtx.Err()
	}

	if o.option.HandshakeTimeout > 0 {
		type runResult struct {
			tunDevice ipStack
			resolver  resolver.Resolver
			err       error
		}

		releaseRunLock = false
		resultCh := make(chan runResult, 1)
		go func() {
			defer o.runLock.Release(1)

			handshakeTimeout := time.Duration(o.option.HandshakeTimeout) * time.Second
			handshakeCtx, handshakeCancel := context.WithTimeout(o.runCtx, handshakeTimeout)
			defer handshakeCancel()

			tunDevice, r, err := o.startLocked(handshakeCtx)
			resultCh <- runResult{tunDevice: tunDevice, resolver: r, err: err}
		}()

		select {
		case result := <-resultCh:
			return result.tunDevice, result.resolver, result.err
		case <-runCtx.Done():
			return nil, nil, runCtx.Err()
		}
	}

	return o.startLocked(runCtx)
}

func (o *IKEv2) startLocked(handshakeCtx context.Context) (ipStack, resolver.Resolver, error) {
	// Resolve the gateway once, so that IKE on port 500 and NAT-T on port 4500 reach the same address.
	serverAddr, err := resolveUDPAddr(handshakeCtx, "udp", o.addr, o.prefer)
	if err != nil {
		return nil, nil, err
	}
	dial := func(ctx context.Context, port uint16) (net.Conn, error) {
		return o.dialer.DialContext(ctx, "udp", net.JoinHostPort(serverAddr.IP.String(), strconv.Itoa(int(port))))
	}
	client, err := ikev2.NewClient(o.config, dial)
	if err != nil {
		return nil, nil, err
	}
	lease, err := client.Handshake(handshakeCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("make IKEv2 handshake: %w", err)
	}
	log.Infoln("[IKEv2](%s) connected: addresses=%v dns=%v routes=%v", o.name, lease.Addresses, lease.DNS, lease.Routes)

	prefixes := make([]netip.Prefix, 0, len(lease.Addresses))
	has6 := false
	for _, addr := range lease.Addresses {
		prefixes = append(prefixes, netip.PrefixFrom(addr, addr.BitLen()))
		has6 = has6 || addr.Is6()
	}
	mtu := o.option.MTU
	if mtu == 0 {
		mtu = ikev2DefaultMTU
	}
	tunDevice, err := newIPStack(o.option.IPStack, prefixes, uint32(mtu))
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("create IKEv2 stack device: %w", err)
	}
	if err := tunDevice.Start(); err != nil {
		_ = client.Close()
		_ = tunDevice.Close()
		return nil, nil, err
	}
	o.client = client
	o.tunDevice = tunDevice
	o.running = true
	o.resolver = nil
	if o.option.RemoteDnsResolve {
		nss := append([]dns.NameServer(nil), o.dns...)
		if len(nss) == 0 && len(lease.DNS) > 0 {
			servers := make([]string, len(lease.DNS))
			for i, addr := range lease.DNS {
				servers[i] = addr.String()
			}
			nss, err = dns.ParseNameServer(servers)
			if err != nil {
				log.Warnln("[IKEv2](%s) ignore DNS servers from gateway: %v", o.name, err)
			}
		}
		if len(nss) > 0 {
			for i := range nss {
				nss[i].ProxyAdapter = o
			}
			o.resolver = dns.NewResolver(dns.Config{Main: nss, IPv6: has6})
		}
	}
	o.startPacketLoops()
	return o.tunDevice, o.resolver, nil
}

func (o *IKEv2) startPacketLoops() {
	runCtx, runCancel := context.WithCancel(o.runCtx)
	client := o.client
	tunDevice := o.tunDevice
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			runCancel()
			_ = client.Close()
			_ = tunDevice.Close()
			_ = o.runLock.Acquire(context.Background(), 1)
			if o.client == client {
				o.client = nil
				o.tunDevice = nil
				o.running = false
			}
			o.runLock.Release(1)
		})
	}
	go func() {
		defer stop()
		buf := make([]byte, 64*1024)
		bufs := [][]byte{buf}
		sizes := []int{0}
		for runCtx.Err() == nil {
			_, err := tunDevice.Read(bufs, sizes, 0)
			if err != nil {
				if runCtx.Err() == nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, os.ErrClosed) {
					log.Errorln("[IKEv2](%s) error reading from stack device: %v", o.name, err)
				}
				return
			}
			if err := client.WriteIPPacket(runCtx, buf[:sizes[0]]); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
					log.Warnln("[IKEv2](%s) error writing packet to tunnel: %v", o.name, err)
				}
				return
			}
		}
	}()

	go func() {
		defer stop()
		for runCtx.Err() == nil {
			packet, err := client.ReadIPPacket(runCtx)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, os.ErrClosed) {
					log.Warnln("[IKEv2](%s) tunnel closed: %v", o.name, err)
				}
				return
			}
			if _, err := tunDevice.Write([][]byte{packet}, 0); err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Errorln("[IKEv2](%s) error writing to stack device: %v", o.name, err)
				}
				return
			}
		}
	}()
}
