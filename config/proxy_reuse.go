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
// certificate pool, which outbounds that verify TLS take when they are built. Whatever
// else they take from the running config, such as the DNS resolvers, the default
// interface or a dialer proxy, they look up when they are used.

type reusableProxy struct {
	mapping any            // deep copy of the mapping the proxy was parsed from
	files   string         // the files the mapping names, with their size and modification time
	certs   *x509.CertPool // certificate pool when the proxy was built
	proxy   C.Proxy
}

var (
	proxyReuseMutex sync.Mutex
	parsedProxies   = make(map[C.Proxy]*reusableProxy) // inline proxies parsed since the last publish
	runningProxies  map[string]*reusableProxy          // name -> inline proxy of the running config
)

type inlineProxyParser struct {
	running    map[string]*reusableProxy
	certs      *x509.CertPool
	certsEqual map[*x509.CertPool]bool
}

func newInlineProxyParser() *inlineProxyParser {
	proxyReuseMutex.Lock()
	defer proxyReuseMutex.Unlock()
	return &inlineProxyParser{
		running:    runningProxies,
		certs:      ca.GetCertPool(),
		certsEqual: make(map[*x509.CertPool]bool),
	}
}

func (p *inlineProxyParser) parse(mapping map[string]any) (C.Proxy, error) {
	snapshot, ok := copyProxyValue(mapping)
	if !ok {
		return adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))
	}
	files := proxyFiles(mapping)
	name, _ := mapping["name"].(string)

	record := p.running[name]
	if record == nil || !p.reusable(record, name, snapshot, files) {
		proxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))
		if err != nil {
			return nil, err
		}
		record = &reusableProxy{mapping: snapshot, files: files, certs: p.certs, proxy: proxy}
	}
	proxyReuseMutex.Lock()
	parsedProxies[record.proxy] = record
	proxyReuseMutex.Unlock()
	return record.proxy, nil
}

func (p *inlineProxyParser) reusable(record *reusableProxy, name string, mapping any, files string) bool {
	return reflect.DeepEqual(record.mapping, mapping) &&
		record.files == files &&
		p.sameCerts(record.certs) &&
		// the instance the running config actually uses, which is not closed for good,
		// as after another outbound took its session over
		T.Proxies()[name] == record.proxy &&
		!outbound.IsClosed(record.proxy.Adapter())
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

// CommitProxyReuse is called once the proxies of a config are published. It keeps the
// inline proxies of that config for the next parse, and drops whatever other parses
// left, such as a config that failed to apply.
func CommitProxyReuse(proxies map[string]C.Proxy) {
	proxyReuseMutex.Lock()
	running := make(map[string]*reusableProxy)
	for name, proxy := range proxies {
		if record := parsedProxies[proxy]; record != nil {
			running[name] = record
		}
	}
	runningProxies = running
	parsedProxies = make(map[C.Proxy]*reusableProxy)
	proxyReuseMutex.Unlock()

	for _, proxy := range proxies {
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
