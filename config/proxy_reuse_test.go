package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	T "github.com/metacubex/mihomo/tunnel"
)

func testWireGuardMapping(port int) map[string]any {
	return map[string]any{
		"name":        "wg",
		"type":        "wireguard",
		"server":      "127.0.0.1",
		"port":        port,
		"ip":          "10.9.0.2",
		"private-key": "4F7zVRZpPEAHIEFBcZj4NITwyiXwzHHbRMpRlZr9bnk=",
		"public-key":  "rsPXORPczwaVYfkc29uC6p2Vcjh2POFmL+quiQbBezk=",
		"udp":         true,
	}
}

func testSSMapping() map[string]any {
	return map[string]any{
		"name":     "ss",
		"type":     "ss",
		"server":   "127.0.0.1",
		"port":     8388,
		"cipher":   "aes-128-gcm",
		"password": "password",
	}
}

func parseTestRawConfig(t *testing.T, raw *RawConfig) *Config {
	t.Helper()
	proxies, _, err := parseProxies(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &Config{Proxies: proxies, inlineProxies: raw.inlineProxies}
}

func parseTestConfig(t *testing.T, mappings ...map[string]any) *Config {
	t.Helper()
	return parseTestRawConfig(t, &RawConfig{Proxy: mappings})
}

func parseTestProxies(t *testing.T, mappings ...map[string]any) map[string]C.Proxy {
	t.Helper()
	return parseTestConfig(t, mappings...).Proxies
}

// publishTestConfig applies the proxies of a config like the executor does.
func publishTestConfig(cfg *Config) map[string]C.Proxy {
	T.UpdateProxies(cfg.Proxies, nil)
	CommitProxyReuse(cfg)
	return cfg.Proxies
}

func resetProxyReuse(t *testing.T) {
	t.Cleanup(func() {
		T.UpdateProxies(nil, nil)
		proxyReuseMutex.Lock()
		runningProxies, runningEnv = nil, proxyEnvironment{}
		proxyReuseMutex.Unlock()
	})
}

func TestReuseUnchangedInlineProxy(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestConfig(parseTestConfig(t, testWireGuardMapping(51820)))
	reloaded := publishTestConfig(parseTestConfig(t, testWireGuardMapping(51820)))
	if reloaded["wg"] != running["wg"] {
		t.Fatal("unchanged proxy was rebuilt")
	}
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] != running["wg"] {
		t.Fatal("unchanged proxy was rebuilt on the second reload")
	}
}

func TestReuseUnchangedInlineProxyOfAnyType(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestConfig(parseTestConfig(t, testSSMapping()))
	if parseTestProxies(t, testSSMapping())["ss"] != running["ss"] {
		t.Fatal("unchanged proxy was rebuilt")
	}
}

func TestReuseUnchangedInlineProxyFromYAML(t *testing.T) {
	resetProxyReuse(t)
	const yaml = `
proxies:
  - name: wg
    type: wireguard
    ip: 10.9.0.2
    private-key: 4F7zVRZpPEAHIEFBcZj4NITwyiXwzHHbRMpRlZr9bnk=
    udp: true
    peers:
      - server: 127.0.0.1
        port: 51820
        public-key: rsPXORPczwaVYfkc29uC6p2Vcjh2POFmL+quiQbBezk=
        allowed-ips: ['0.0.0.0/0']
        reserved: [1, 2, 3]
`
	parse := func() *Config {
		raw, err := UnmarshalRawConfig([]byte(yaml))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := copyProxyValue(raw.Proxy[0]); !ok {
			t.Fatal("proxy read from yaml can't be compared")
		}
		return parseTestRawConfig(t, raw)
	}
	running := publishTestConfig(parse())
	if parse().Proxies["wg"] != running["wg"] {
		t.Fatal("unchanged proxy read from yaml was rebuilt")
	}
}

func TestRebuildChangedInlineProxy(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestConfig(parseTestConfig(t, testWireGuardMapping(51820)))
	if parseTestProxies(t, testWireGuardMapping(51821))["wg"] == running["wg"] {
		t.Fatal("changed proxy was reused")
	}
}

func TestKeepRunningProxyAfterUnappliedConfig(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestConfig(parseTestConfig(t, testWireGuardMapping(51820)))
	// a changed config that is parsed but never applied, e.g. its groups fail to parse
	parseTestProxies(t, testWireGuardMapping(51821))
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] != running["wg"] {
		t.Fatal("running proxy was rebuilt after a config that was never applied")
	}
}

func TestRebuildInlineProxyNotInRunningConfig(t *testing.T) {
	resetProxyReuse(t)
	unapplied := parseTestProxies(t, testWireGuardMapping(51820))
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] == unapplied["wg"] {
		t.Fatal("proxy of a config that was never applied was reused")
	}
}

func TestRebuildClosedInlineProxy(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestConfig(parseTestConfig(t, testWireGuardMapping(51820)))
	// e.g. after a newer outbound took its session over
	_ = running["wg"].Adapter().Close()
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] == running["wg"] {
		t.Fatal("closed proxy was reused")
	}
}

func TestProxyMappingKeepsValueTypes(t *testing.T) {
	asInt, _ := copyProxyValue(map[string]any{"udp": 1, "peers": []any{map[string]any{"port": 1}}})
	asFloat, _ := copyProxyValue(map[string]any{"udp": 1.0, "peers": []any{map[string]any{"port": 1}}})
	again, _ := copyProxyValue(map[string]any{"peers": []any{map[string]any{"port": 1}}, "udp": 1})
	if reflect.DeepEqual(asInt, asFloat) {
		t.Fatal("values the decoder reads differently compare equal")
	}
	if !reflect.DeepEqual(asInt, again) {
		t.Fatal("equal mappings compare different")
	}
	if _, ok := copyProxyValue(map[string]any{"at": time.Now()}); ok {
		t.Fatal("value of an unknown type was accepted")
	}
}

func TestRebuildInlineProxyAfterFileChange(t *testing.T) {
	resetProxyReuse(t)
	home := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() { C.SetHomeDir(home) })
	file := filepath.Join(C.Path.HomeDir(), "key.txt")
	if err := os.WriteFile(file, []byte("key"), 0o600); err != nil {
		t.Fatal(err)
	}
	// any string that names a file counts, as for the key file of an ssh proxy
	mapping := func() map[string]any {
		m := testSSMapping()
		m["password"] = "key.txt"
		return m
	}

	running := publishTestConfig(parseTestConfig(t, mapping()))
	if parseTestProxies(t, mapping())["ss"] != running["ss"] {
		t.Fatal("proxy was rebuilt although its file is unchanged")
	}
	if err := os.WriteFile(file, []byte("another key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if parseTestProxies(t, mapping())["ss"] == running["ss"] {
		t.Fatal("proxy was reused although its file changed")
	}
}

func testCertificatePEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "proxy reuse test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestRebuildInlineProxiesAfterCertificateChange(t *testing.T) {
	resetProxyReuse(t)
	t.Cleanup(ca.ResetCertificate)
	running := publishTestConfig(parseTestConfig(t, testSSMapping()))

	// applying a config builds a new pool, with the same certificates here
	ca.ResetCertificate()
	if parseTestProxies(t, testSSMapping())["ss"] != running["ss"] {
		t.Fatal("proxy was rebuilt although the certificates are unchanged")
	}

	// and with a certificate added here
	ca.ResetCertificate()
	if err := ca.AddCertificate(testCertificatePEM(t)); err != nil {
		t.Fatal(err)
	}
	if parseTestProxies(t, testSSMapping())["ss"] == running["ss"] {
		t.Fatal("proxy was reused although the certificates changed")
	}
}

func TestReuseAfterConfigsParsedBeforeEitherIsApplied(t *testing.T) {
	resetProxyReuse(t)
	first := parseTestConfig(t, testSSMapping())
	second := parseTestConfig(t, testWireGuardMapping(51820))
	publishTestConfig(first)
	running := publishTestConfig(second)
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] != running["wg"] {
		t.Fatal("proxy of the config applied last was rebuilt")
	}
}

func TestRebuildInlineProxyAfterDialerProxyChange(t *testing.T) {
	resetProxyReuse(t)
	chained := func() map[string]any {
		m := testWireGuardMapping(51820)
		m["dialer-proxy"] = "ss"
		return m
	}
	dialer := func(port int) map[string]any {
		m := testSSMapping()
		m["port"] = port
		return m
	}

	// the dialer proxy comes later in the config than the proxy that dials through it
	running := publishTestConfig(parseTestConfig(t, chained(), dialer(8388)))
	reloaded := publishTestConfig(parseTestConfig(t, chained(), dialer(8388)))
	if reloaded["wg"] != running["wg"] || reloaded["ss"] != running["ss"] {
		t.Fatal("unchanged proxies were rebuilt")
	}
	// its sessions would keep carrying new connections over the old dialer proxy
	if parseTestProxies(t, chained(), dialer(8389))["wg"] == running["wg"] {
		t.Fatal("proxy was reused although its dialer proxy changed")
	}
}

func TestRebuildInlineProxiesAfterInterfaceChange(t *testing.T) {
	resetProxyReuse(t)
	parse := func(iface string) *Config {
		return parseTestRawConfig(t, &RawConfig{Proxy: []map[string]any{testSSMapping()}, Interface: iface})
	}
	running := publishTestConfig(parse("eth0"))
	if parse("eth0").Proxies["ss"] != running["ss"] {
		t.Fatal("proxy was rebuilt although the interface is unchanged")
	}
	if parse("eth1").Proxies["ss"] == running["ss"] {
		t.Fatal("proxy was reused although the interface changed")
	}
}

func TestRebuildInlineProxyAfterChangeOfDialerProxySpelledDifferently(t *testing.T) {
	resetProxyReuse(t)
	chained := func() map[string]any {
		m := testWireGuardMapping(51820)
		m["dialer_proxy"] = "ss" // the decoder takes this spelling as well
		return m
	}
	dialer := func(port int) map[string]any {
		m := testSSMapping()
		m["port"] = port
		return m
	}
	running := publishTestConfig(parseTestConfig(t, chained(), dialer(8388)))
	if parseTestProxies(t, chained(), dialer(8389))["wg"] == running["wg"] {
		t.Fatal("proxy was reused although its dialer proxy changed")
	}
}

func TestRebuildInlineProxiesAfterIPTablesChange(t *testing.T) {
	resetProxyReuse(t)
	// enabling iptables changes the default routing mark
	parse := func(enable bool) *Config {
		return parseTestRawConfig(t, &RawConfig{Proxy: []map[string]any{testSSMapping()}, IPTables: RawIPTables{Enable: enable}})
	}
	running := publishTestConfig(parse(false))
	if parse(false).Proxies["ss"] != running["ss"] {
		t.Fatal("proxy was rebuilt although iptables are unchanged")
	}
	if parse(true).Proxies["ss"] == running["ss"] {
		t.Fatal("proxy was reused although iptables changed")
	}
}
