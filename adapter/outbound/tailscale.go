//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/contextutils"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/iface/anet"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/dns"
	"github.com/metacubex/mihomo/log"

	ts "github.com/metacubex/tailscale"
	"github.com/metacubex/tailscale/envknob"
	"github.com/metacubex/tailscale/hostinfo"
	"github.com/metacubex/tailscale/ipn"
	"github.com/metacubex/tailscale/net/netmon"
	"github.com/metacubex/tailscale/tailcfg"
	"github.com/metacubex/tailscale/tsnet"
	D "github.com/miekg/dns"
	"github.com/samber/lo"
)

var (
	tailscaleSerial       atomic.Uint64
	tailscaleServersMutex sync.Mutex
	tailscaleServers      = make(map[string]*Tailscale) // state dir -> outbound running a server on it

	errTailscaleOutdated = errors.New("tailscale state-dir is in use by a newer outbound")
	errTailscaleClosed   = errors.New("tailscale outbound closed")
)

type Tailscale struct {
	*Base
	serial      uint64 // creation order, a newer outbound takes over the state dir of an older one
	server      *tsnet.Server
	dnsResolver *dns.Resolver
	option      TailscaleOption
	ctx         context.Context
	cancel      context.CancelFunc
	startOnce   sync.Once
	startErr    error
	// startClaimed is set once the start runs, and startDone is closed once it is over,
	// so callers can stop waiting for it.
	startClaimed atomic.Bool
	startDone    chan struct{}
	stateDirKey  string      // the key the state dir is held under, set by takeOverState
	stateDirInfo os.FileInfo // what tells the state dir apart, set with stateDirKey

	backendInitOnce sync.Once
	backendInitCh   chan struct{}
	backendInitErr  error
	startFailed     atomic.Bool // backendInitErr is set

	serverStarted bool

	dnsRegistrationMutex  sync.Mutex
	unregisterDNSResolver func()
}

type TailscaleOption struct {
	BasicOption
	Name       string `proxy:"name"`
	Hostname   string `proxy:"hostname,omitempty"`
	AuthKey    string `proxy:"auth-key,omitempty"`
	ControlURL string `proxy:"control-url,omitempty"`
	StateDir   string `proxy:"state-dir,omitempty"`
	Ephemeral  bool   `proxy:"ephemeral,omitempty"`
	UDP        bool   `proxy:"udp,omitempty"`

	AcceptRoutes           *bool  `proxy:"accept-routes,omitempty"`
	ExitNode               string `proxy:"exit-node,omitempty"`
	ExitNodeAllowLANAccess *bool  `proxy:"exit-node-allow-lan-access,omitempty"`
}

func init() {
	hostinfo.RegisterHostinfoNewHook(func(hi *tailcfg.Hostinfo) {
		versionDotTxt := strings.TrimSpace(ts.VersionDotTxt)
		hi.IPNVersion = fmt.Sprintf("%s-%s-%s", versionDotTxt, C.MihomoName, C.Version)
	})
	envknob.SetNoLogsNoSupport()
	if runtime.GOOS == "android" { // Android SDK 30 no longer permits Go's net.Interfaces to work (Issue 2293)
		netmon.RegisterInterfaceGetter(func() (nif []netmon.Interface, err error) {
			log.Debugln("[Tailscale] InterfaceGetter: start, IsForceAnet: %v", anet.IsForceAnet())
			ifaces, err := anet.Interfaces()
			if err != nil {
				log.Warnln("[Tailscale] anet.Interfaces failed: %v", err)
				return nil, err
			}
			for _, iff := range ifaces {
				addrs, err := anet.InterfaceAddrsByInterface(&iff)
				if err != nil {
					log.Warnln("[Tailscale] anet.InterfaceAddrsByInterface(%v) failed: %v", iff.Name, err)
					continue
				}
				nif = append(nif, netmon.Interface{
					Interface: &net.Interface{
						Index:        iff.Index,
						MTU:          iff.MTU,
						Name:         iff.Name,
						HardwareAddr: iff.HardwareAddr,
						Flags:        iff.Flags,
					},
					AltAddrs: addrs,
				})
			}

			log.Debugln("[Tailscale] InterfaceGetter: %v", lo.Map(nif, func(item netmon.Interface, index int) string {
				var addrs any
				addrs, err := item.Addrs()
				if err != nil {
					addrs = err
				}
				return fmt.Sprintf("{Name: %s, Addrs: %v, IsUp: %v, IsLoopback: %v}", item.Name, addrs, item.IsUp(), item.IsLoopback())
			}))
			return
		})
	}
}

func NewTailscale(option TailscaleOption) (*Tailscale, error) {
	if _, err := buildTailscaleMaskedPrefs(option); err != nil {
		return nil, err
	}
	if option.StateDir == "" {
		option.StateDir = "tailscale"
	}
	option.StateDir = C.Path.Resolve(option.StateDir)
	if !C.Path.IsSafePath(option.StateDir) {
		return nil, C.Path.ErrNotSafePath(option.StateDir)
	}

	addr := option.ControlURL
	if addr == "" {
		addr = "tailscale"
	}
	ctx, cancel := context.WithCancel(context.Background())
	outbound := &Tailscale{
		Base: NewBase(BaseOption{
			Name:         option.Name,
			Addr:         addr,
			Type:         C.Tailscale,
			ProviderName: option.ProviderName,
			UDP:          option.UDP,
			Interface:    option.Interface,
			RoutingMark:  option.RoutingMark,
			Prefer:       option.IPVersion,
		}),
		serial:        tailscaleSerial.Add(1),
		option:        option,
		ctx:           ctx,
		cancel:        cancel,
		backendInitCh: make(chan struct{}),
		startDone:     make(chan struct{}),
	}
	outbound.dialer = option.NewDialer(outbound.DialOptions())
	outbound.server = &tsnet.Server{
		Dir:        option.StateDir,
		Hostname:   option.Hostname,
		AuthKey:    option.AuthKey,
		ControlURL: option.ControlURL,
		Ephemeral:  option.Ephemeral,
		SystemDialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			ctx, cancel := outbound.withLifetime(ctx)
			defer cancel()
			log.Debugln("[Tailscale](%s) SystemDialer: start dial %s %s", option.Name, network, address)
			conn, err := outbound.dialer.DialContext(ctx, network, address)
			log.Debugln("[Tailscale](%s) SystemDialer: finish dial %s %s, err: %v", option.Name, network, address, err)
			return conn, err
		},
		SystemPacketListener: func(ctx context.Context, network, address string) (net.PacketConn, error) {
			ctx, cancel := outbound.withLifetime(ctx)
			defer cancel()
			log.Debugln("[Tailscale](%s) SystemPacketListener: start listen %s %s", option.Name, network, address)
			pc, err := outbound.dialer.ListenPacket(ctx, network, address, netip.AddrPort{})
			log.Debugln("[Tailscale](%s) SystemPacketListener: finish listen %s %s, err: %v", option.Name, network, address, err)
			return pc, err
		},
		ExtraRootCAs: ca.GetCertPool(),
		LookupHook: func(ctx context.Context, host string) ([]netip.Addr, error) {
			log.Debugln("[Tailscale](%s) LookupHook: start lookup %s", option.Name, host)
			ips, err := resolver.LookupIPWithResolver(ctx, host, resolver.ProxyServerHostResolver)
			log.Debugln("[Tailscale](%s) LookupHook: finish lookup %s, ips: %v, err: %v", option.Name, host, ips, err)
			return ips, err
		},
		UserLogf: func(format string, args ...any) {
			log.Infoln("[Tailscale](%s) %s", option.Name, fmt.Sprintf(format, args...))
		},
		Logf: func(format string, args ...any) {
			log.Debugln("[Tailscale](%s) %s", option.Name, fmt.Sprintf(format, args...))
		},
	}
	outbound.dnsResolver = dns.NewResolverFromClient(tailscaleDNSTransport{tailscale: outbound})
	return outbound, nil
}

func (t *Tailscale) start() error {
	t.startOnce.Do(func() {
		defer close(t.startDone)
		if err := t.takeOverState(); err != nil {
			t.startErr = err
			t.setBackendInitialized(err)
			return
		}
		// closed while the older outbound was shutting down
		if t.ctx.Err() != nil {
			t.startErr = errTailscaleClosed
			t.setBackendInitialized(t.startErr)
			return
		}
		if err := t.server.Start(); err != nil {
			t.startErr = err
			t.setBackendInitialized(err)
			return
		}
		t.serverStarted = true
		ctx, cancel := context.WithTimeout(t.ctx, 30*time.Second)
		defer cancel()
		if err := t.applyPrefs(ctx); err != nil {
			t.startErr = err
			t.setBackendInitialized(err)
			return
		}
		go t.watchBackendState()
	})
	return t.startErr
}

// withLifetime returns ctx canceled as well once the outbound is closed. The server
// dials through it, and a start stuck on such a dial would otherwise keep a newer
// outbound from taking the state dir over. The dial context is canceled once the
// dial returns, as the tunnel does for any outbound, so connections outlive it.
func (t *Tailscale) withLifetime(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := contextutils.AfterFunc(t.ctx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (t *Tailscale) ensureStarted(ctx context.Context) error {
	// a canceled caller, such as the health check of a provider a new config replaced,
	// must not start the server and take the state dir over
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.startWithin(ctx); err != nil {
		return err
	}
	if err := t.waitBackendInitialized(ctx); err != nil {
		return err
	}
	// a server closed after it started is not restarted
	if t.ctx.Err() != nil {
		return errTailscaleClosed
	}
	return nil
}

// startWithin starts the outbound on its own and waits for the start for as long as
// ctx allows. The start may wait for this very caller: tsnet dials and listens through
// the dialer proxy as it starts, which can lead to the start of another outbound, and
// the older outbound that one closes can be stuck on such a dial of its own. That dial
// gives way once its outbound is closed, which only helps if no start runs on the
// stack of a caller.
func (t *Tailscale) startWithin(ctx context.Context) error {
	if t.startClaimed.CompareAndSwap(false, true) {
		go t.start()
	}
	select {
	case <-t.startDone:
		return t.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stateDirIdentity reads what tells the dir apart from others. It is read from an open
// handle, so that comparing it needs no more calls to the file system, which can be slow
// to answer, as a network share is.
func stateDirIdentity(dir string) (os.FileInfo, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

// stateOwner returns the outbound that holds the state dir and the key it holds it
// under. Another spelling of the dir, as in other case on a case-insensitive file
// system or through a symbolic link, finds it too. tailscaleServersMutex must be held.
func stateOwner(key string, info os.FileInfo) (string, *Tailscale) {
	if owner, ok := tailscaleServers[key]; ok {
		return key, owner
	}
	for other, owner := range tailscaleServers {
		if owner.stateDirInfo != nil && os.SameFile(info, owner.stateDirInfo) {
			return other, owner
		}
	}
	return "", nil
}

// takeOverState must be called before the server is started.
// A config reload or provider update replaces the outbound, but the old one is only
// closed by the GC, which can take minutes and doesn't happen at all while a connection
// still uses it. Two servers on the same state dir are the same node: the control server
// keeps dropping one of them, and both write the same state file. So the newer outbound
// closes the server of the older one first, whatever proxy the older one was created for.
func (t *Tailscale) takeOverState() error {
	key := filepath.Clean(t.option.StateDir)
	// as tsnet does on start; another spelling of the dir is only found once it exists
	if err := os.MkdirAll(key, 0o700); err != nil {
		return err
	}
	info, err := stateDirIdentity(key)
	if err != nil {
		return err
	}
	tailscaleServersMutex.Lock()
	oldKey, old := stateOwner(key, info)
	if old != nil && old.serial > t.serial {
		tailscaleServersMutex.Unlock()
		return errTailscaleOutdated
	}
	// held under the spelling of this outbound, which outlives the one the older used
	delete(tailscaleServers, oldKey)
	tailscaleServers[key] = t
	t.stateDirKey, t.stateDirInfo = key, info
	tailscaleServersMutex.Unlock()

	if old != nil && old != t {
		log.Infoln("[Tailscale](%s) closing the server of %s, which runs on the same state-dir %s", t.option.Name, old.option.Name, key)
		// waits for a start of the older outbound in progress
		_ = old.Close()
	}
	return nil
}

func (t *Tailscale) watchBackendState() {
	lc, err := t.server.LocalClient()
	if err != nil {
		t.setBackendInitialized(err)
		return
	}
	watcher, err := lc.WatchIPNBus(t.ctx, ipn.NotifyInitialState)
	if err != nil {
		t.setBackendInitialized(err)
		return
	}
	defer watcher.Close()

	backendInitialized := false
	exitNodeNeedsStatus := tailscaleExitNodeNeedsStatus(t.option)
	for {
		n, err := watcher.Next()
		if err != nil {
			t.setBackendInitialized(err)
			return
		}
		if n.State == nil {
			continue
		}

		if *n.State != ipn.NoState && !backendInitialized {
			t.setBackendInitialized(nil)
			backendInitialized = true
			if !exitNodeNeedsStatus {
				return
			}
		}
		if exitNodeNeedsStatus && *n.State == ipn.Running {
			if err := t.applyExitNodePrefs(t.ctx); err != nil {
				log.Warnln("[Tailscale](%s) set exit node failed: %v", t.Name(), err)
			}
			return
		}
	}
}

func (t *Tailscale) setBackendInitialized(err error) {
	t.backendInitOnce.Do(func() {
		t.backendInitErr = err
		t.startFailed.Store(err != nil)
		close(t.backendInitCh)
	})
}

func (t *Tailscale) waitBackendInitialized(ctx context.Context) error {
	select {
	case <-t.backendInitCh:
		return t.backendInitErr
	case <-ctx.Done():
		return ctx.Err()
	case <-t.ctx.Done():
		return t.ctx.Err()
	}
}

func (t *Tailscale) applyPrefs(ctx context.Context) error {
	mp, err := buildTailscaleMaskedPrefs(t.option)
	if err != nil {
		return err
	}
	if mp == nil {
		return nil
	}
	lc, err := t.server.LocalClient()
	if err != nil {
		return err
	}
	_, err = lc.EditPrefs(ctx, mp)
	return err
}

func (t *Tailscale) applyExitNodePrefs(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	lc, err := t.server.LocalClient()
	if err != nil {
		return err
	}
	status, err := lc.Status(ctx)
	if err != nil {
		return err
	}
	mp := &ipn.MaskedPrefs{
		ExitNodeIPSet: true,
	}
	if t.option.ExitNodeAllowLANAccess != nil {
		mp.ExitNodeAllowLANAccess = *t.option.ExitNodeAllowLANAccess
		mp.ExitNodeAllowLANAccessSet = true
	}
	if err = mp.SetExitNodeIP(t.option.ExitNode, status); err != nil {
		return err
	}
	_, err = lc.EditPrefs(ctx, mp)
	return err
}

func buildTailscaleMaskedPrefs(option TailscaleOption) (*ipn.MaskedPrefs, error) {
	var mp ipn.MaskedPrefs
	changed := false

	if option.AcceptRoutes != nil {
		mp.RouteAll = *option.AcceptRoutes
		mp.RouteAllSet = true
		changed = true
	}
	if option.ExitNode != "" {
		if autoExitNode, ok := ipn.ParseAutoExitNodeString(option.ExitNode); ok {
			mp.AutoExitNode = autoExitNode
			mp.AutoExitNodeSet = true
			changed = true
		}
	}
	if option.ExitNodeAllowLANAccess != nil && !tailscaleExitNodeNeedsStatus(option) {
		mp.ExitNodeAllowLANAccess = *option.ExitNodeAllowLANAccess
		mp.ExitNodeAllowLANAccessSet = true
		changed = true
	}
	if !changed {
		return nil, nil
	}
	return &mp, nil
}

func tailscaleExitNodeNeedsStatus(option TailscaleOption) bool {
	if option.ExitNode == "" {
		return false
	}
	_, ok := ipn.ParseAutoExitNodeString(option.ExitNode)
	return !ok
}

func (t *Tailscale) DialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, err error) {
	if err = t.ensureStarted(ctx); err != nil {
		return nil, err
	}
	netStack, err := t.server.Netstack(ctx)
	if err != nil {
		return nil, err
	}
	v4, v6 := t.server.TailscaleIPs()
	options := t.DialOptions()
	options = append(options, dialer.WithResolver(t.dnsResolver))
	options = append(options, dialer.WithNetDialer(dialer.NetDialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		dst, err := netip.ParseAddrPort(address) // the dialer will resolve the domain to ip
		if err != nil {
			return nil, err
		}
		src := v4
		if dst.Addr().Is6() {
			src = v6
		}
		tcpConn, err := netStack.DialContextTCPWithBind(ctx, src, dst)
		if err != nil {
			return nil, err
		}
		return tcpConn, nil
	})))
	var conn net.Conn
	conn, err = dialer.NewDialer(options...).DialContext(ctx, "tcp", metadata.RemoteAddress())
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("conn is nil")
	}
	return NewConn(conn, t), nil
}

func (t *Tailscale) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (_ C.PacketConn, err error) {
	if err = t.ensureStarted(ctx); err != nil {
		return nil, err
	}
	if err = t.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}
	v4, v6 := t.server.TailscaleIPs()
	src := v4
	if metadata.DstIP.Is6() {
		src = v6
	}
	pc, err := t.server.ListenPacket("udp", net.JoinHostPort(src.String(), "0"))
	if err != nil {
		return nil, err
	}
	if pc == nil {
		return nil, errors.New("packetConn is nil")
	}
	return NewPacketConn(pc, t), nil
}

func (t *Tailscale) ResolveUDP(ctx context.Context, metadata *C.Metadata) error {
	if metadata.Host != "" {
		ip, err := resolveIPWithResolver(ctx, metadata.Host, t.prefer, t.dnsResolver)
		if err != nil {
			return fmt.Errorf("can't resolve ip: %w", err)
		}
		metadata.DstIP = ip
	}
	return nil
}

type tailscaleDNSTransport struct {
	tailscale *Tailscale
}

func (t tailscaleDNSTransport) Address() string {
	return "tailscale://" + t.tailscale.Name()
}

func (t tailscaleDNSTransport) ResetConnection() {}

func (t tailscaleDNSTransport) ExchangeContext(ctx context.Context, msg *D.Msg) (*D.Msg, error) {
	if len(msg.Question) == 0 {
		return nil, errors.New("should have one question at least")
	}
	if err := t.tailscale.ensureStarted(ctx); err != nil {
		return nil, err
	}
	q := msg.Question[0]
	qtypeName, ok := D.TypeToString[q.Qtype]
	if !ok {
		return nil, fmt.Errorf("unsupported query type: %d", q.Qtype)
	}
	lc, err := t.tailscale.server.LocalClient()
	if err != nil {
		return nil, err
	}
	response, _, err := lc.QueryDNS(ctx, q.Name, qtypeName)
	if err != nil {
		return nil, err
	}
	var responseMsg D.Msg
	if err = responseMsg.Unpack(response); err != nil {
		return nil, err
	}
	responseMsg.Id = msg.Id
	return &responseMsg, nil
}

func (t *Tailscale) ProxyInfo() C.ProxyInfo {
	info := t.Base.ProxyInfo()
	info.DialerProxy = t.option.DialerProxy
	return info
}

func (t *Tailscale) IsL3Protocol(metadata *C.Metadata) bool {
	return true
}

func (t *Tailscale) closed() bool {
	return t.ctx.Err() != nil
}

func (t *Tailscale) reusable() bool {
	return !t.closed() && !t.startFailed.Load()
}

// published registers the tailscale:// DNS of the proxy name once the outbound is in
// use. An outbound of a config that is never applied, or of a provider that was
// replaced, registers nothing, so it never takes the name from the one in use.
func (t *Tailscale) published() {
	t.dnsRegistrationMutex.Lock()
	defer t.dnsRegistrationMutex.Unlock()
	if t.closed() {
		return
	}
	// registering replaces the entry in place, so a lookup never finds it missing, and
	// the unregister function of the earlier registration no longer removes it
	t.unregisterDNSResolver = dns.RegisterTailscaleDnsClient(t.option.Name, tailscaleDNSTransport{tailscale: t})
}

func (t *Tailscale) Close() error {
	t.cancel()
	t.dnsRegistrationMutex.Lock()
	if t.unregisterDNSResolver != nil {
		t.unregisterDNSResolver()
	}
	t.dnsRegistrationMutex.Unlock()
	t.startOnce.Do(func() {
		t.startErr = errTailscaleClosed
		close(t.startDone)
	})
	var err error
	if t.server != nil && t.serverStarted { // tsnet.Server.Close() must not be called before or concurrently with Start.
		err = t.server.Close()
	}
	// release the state dir only after the server is down
	tailscaleServersMutex.Lock()
	if key := t.stateDirKey; tailscaleServers[key] == t {
		delete(tailscaleServers, key)
	}
	tailscaleServersMutex.Unlock()
	return err
}
