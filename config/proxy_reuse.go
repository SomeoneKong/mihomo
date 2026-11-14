package config

import (
	"crypto/x509"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	T "github.com/metacubex/mihomo/tunnel"
)

// A config reload parses every proxy again, and a fresh outbound replaces the running
// one even when nothing about it changed. Outbounds with a session, such as QUIC based
// or multiplexed ones, WireGuard and Tailscale, drop the connections over it then, and
// a Tailscale node has to log in again. Clients such as Clash Verge reload the whole
// config on every subscription update, so an inline proxy that is configured exactly
// as in the running config keeps the running instance.
//
// Exactly also covers what an outbound is built from besides its mapping: the files
// the mapping names, which some outbounds read only when they are built, and the
// certificate pool, which outbounds that verify TLS take when they are built. It also
// covers what the sessions of an outbound are made with, as a session keeps carrying
// new connections: a dialer proxy that is a proxy, and the settings of the config that
// decide on the outbound interface and routing mark, including the tun and iptables
// ones, and its experimental settings. A dialer proxy that is a group only has to remain
// a group: its members change with every update of the subscriptions they come from,
// and the outbound dials through the group as it is when it dials. Whatever else
// outbounds take from the running config, such as the DNS resolvers, they look up when
// they are used.

type reusableProxy struct {
	mapping any            // deep copy of the mapping the proxy was parsed from
	files   string         // the files the mapping names, with their size and modification time
	certs   *x509.CertPool // certificate pool when the proxy was built
	proxy   C.Proxy
}

// proxyEnvironment holds the settings of a config that sessions are made with. The
// tun and iptables settings change the outbound interface and routing mark too.
type proxyEnvironment struct {
	interfaceName string
	routingMark   int
	tun           RawTun
	iptables      RawIPTables
	experimental  RawExperimental
}

var (
	proxyReuseMutex sync.Mutex
	runningProxies  map[string]*reusableProxy // name -> inline proxy of the running config
	runningEnv      proxyEnvironment
)

// inlineProxyParser parses the inline proxies of one config and keeps what the config
// needs to hand them on once it is published, so a config that is never published
// leaves nothing behind.
type inlineProxyParser struct {
	running    map[string]*reusableProxy
	env        proxyEnvironment
	certs      *x509.CertPool
	certsEqual map[*x509.CertPool]bool
	parsed     map[string]*reusableProxy // name -> inline proxy of this config
	reused     map[string]map[string]any // name -> mapping of a proxy that is reused
	groups     map[string]bool           // the names of the proxy groups of this config
}

func newInlineProxyParser(cfg *RawConfig) *inlineProxyParser {
	proxyReuseMutex.Lock()
	running, env := runningProxies, runningEnv
	proxyReuseMutex.Unlock()

	p := &inlineProxyParser{
		env: proxyEnvironment{
			interfaceName: cfg.Interface,
			routingMark:   cfg.RoutingMark,
			tun:           cfg.Tun,
			iptables:      cfg.IPTables,
			experimental:  cfg.Experimental,
		},
		certs:      ca.GetCertPool(),
		certsEqual: make(map[*x509.CertPool]bool),
		parsed:     make(map[string]*reusableProxy),
		reused:     make(map[string]map[string]any),
		groups:     make(map[string]bool),
	}
	for _, mapping := range cfg.ProxyGroup {
		if name, ok := mapping["name"].(string); ok {
			p.groups[name] = true
		}
	}
	if reflect.DeepEqual(p.env, env) {
		p.running = running
	}
	return p
}

func (p *inlineProxyParser) parse(mapping map[string]any) (C.Proxy, error) {
	snapshot, ok := copyProxyValue(mapping)
	if !ok {
		proxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))
		if err != nil {
			return nil, err
		}
		// never reused, but kept to be told once a config no longer uses it
		p.parsed[proxy.Name()] = &reusableProxy{proxy: proxy}
		return proxy, nil
	}
	files := proxyFiles(mapping)
	name, _ := mapping["name"].(string)

	record := p.running[name]
	if record != nil && p.reusable(record, name, snapshot, files) {
		p.reused[name] = mapping
	} else {
		proxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))
		if err != nil {
			return nil, err
		}
		record = &reusableProxy{mapping: snapshot, files: files, certs: p.certs, proxy: proxy}
	}
	// under the name the proxy decoder read, which may be spelled otherwise in the mapping
	p.parsed[record.proxy.Name()] = record
	return record.proxy, nil
}

func (p *inlineProxyParser) reusable(record *reusableProxy, name string, mapping any, files string) bool {
	return reflect.DeepEqual(record.mapping, mapping) &&
		record.files == files &&
		p.sameCerts(record.certs) &&
		// the instance the running config actually uses, which can still serve
		T.Proxies()[name] == record.proxy &&
		outbound.IsReusable(record.proxy.Adapter())
}

// sameCerts compares by content: each config applied gets a new pool.
func (p *inlineProxyParser) sameCerts(certs *x509.CertPool) bool {
	if certs == p.certs {
		return true
	}
	equal, ok := p.certsEqual[certs]
	if !ok {
		equal = certs != nil && p.certs != nil && certs.Equal(p.certs)
		p.certsEqual[certs] = equal
	}
	return equal
}

// finish is called once all inline proxies are parsed. It rebuilds the reused proxies
// whose dialer proxy is a proxy that is not reused as well, or is neither a proxy nor a
// group of this config. What only parsing needs is dropped, so the config doesn't keep
// the proxies of the running one alive.
func (p *inlineProxyParser) finish(proxies map[string]C.Proxy) error {
	for changed := true; changed; {
		changed = false
		for name, mapping := range p.reused {
			// as decoded, the mapping may spell the key or value differently
			dialer := p.parsed[name].proxy.ProxyInfo().DialerProxy
			if _, ok := p.reused[dialer]; dialer == "" || ok || p.groups[dialer] {
				continue
			}
			proxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))
			if err != nil {
				return fmt.Errorf("proxy %s: %w", name, err)
			}
			record := p.parsed[name]
			p.parsed[name] = &reusableProxy{mapping: record.mapping, files: record.files, certs: p.certs, proxy: proxy}
			proxies[name] = proxy
			delete(p.reused, name)
			changed = true
		}
	}
	p.running, p.reused, p.certsEqual, p.groups = nil, nil, nil, nil
	return nil
}

// CommitProxyReuse is called once the proxies of a config are published. It keeps the
// inline proxies of that config for the next parse.
func CommitProxyReuse(cfg *Config) {
	running := make(map[string]*reusableProxy)
	var env proxyEnvironment
	if p := cfg.inlineProxies; p != nil {
		env = p.env
		for name, record := range p.parsed {
			if cfg.Proxies[name] == record.proxy {
				running[name] = record
			}
		}
	}
	proxyReuseMutex.Lock()
	previous := runningProxies
	runningProxies, runningEnv = running, env
	proxyReuseMutex.Unlock()

	// the inline proxies the config no longer uses, removed or replaced by new ones
	for name, record := range previous {
		if cfg.Proxies[name] != record.proxy {
			outbound.Unpublished(record.proxy.Adapter())
		}
	}

	// The proxies providers hold already too, such as those of inline providers; the ones
	// a provider fetches are published as it updates. The inline proxies come last and
	// keep a name they share.
	for _, pd := range cfg.Providers {
		for _, proxy := range pd.Proxies() {
			outbound.Published(proxy.Adapter())
		}
	}
	for _, proxy := range cfg.Proxies {
		outbound.Published(proxy.Adapter())
	}
}

// copyProxyValue deep copies a mapping as the config decoder produces it. Values keep
// their types, so values the proxy decoder reads differently, such as 1 and 1.0, don't
// compare equal. Other types are not reused, as they might not compare like that.
func copyProxyValue(value any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		m := make(map[string]any, len(v))
		for key, item := range v {
			c, ok := copyProxyValue(item)
			if !ok {
				return nil, false
			}
			m[key] = c
		}
		return m, true
	case []any:
		s := make([]any, len(v))
		for i, item := range v {
			c, ok := copyProxyValue(item)
			if !ok {
				return nil, false
			}
			s[i] = c
		}
		return s, true
	case nil, string, bool, int, int64, uint64, float64:
		return v, true
	}
	return nil, false
}

// proxyFiles describes the files a mapping names: every string that is the path of a
// regular file, relative to the home dir or absolute.
func proxyFiles(mapping map[string]any) string {
	var files []string
	var walk func(value any)
	walk = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			for _, item := range v {
				walk(item)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		case string:
			// inline certificates and keys are not paths
			if v == "" || strings.ContainsAny(v, "\r\n") {
				return
			}
			path := C.Path.Resolve(v)
			if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
				files = append(files, fmt.Sprintf("%s|%d|%d", path, info.Size(), info.ModTime().UnixNano()))
			}
		}
	}
	walk(mapping)
	sort.Strings(files)
	return strings.Join(files, "\n")
}
