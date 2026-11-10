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

func parseTestProxies(t *testing.T, mappings ...map[string]any) map[string]C.Proxy {
	t.Helper()
	proxies, _, err := parseProxies(&RawConfig{Proxy: mappings})
	if err != nil {
		t.Fatal(err)
	}
	return proxies
}

// publishTestProxies applies the proxies like the executor does for a new config.
func publishTestProxies(proxies map[string]C.Proxy) map[string]C.Proxy {
	T.UpdateProxies(proxies, nil)
	CommitProxyReuse(proxies)
	return proxies
}

func resetProxyReuse(t *testing.T) {
	t.Cleanup(func() {
		T.UpdateProxies(nil, nil)
		proxyReuseMutex.Lock()
		runningProxies = nil
		parsedProxies = make(map[C.Proxy]*reusableProxy)
		proxyReuseMutex.Unlock()
	})
}

func TestReuseUnchangedInlineProxy(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestProxies(parseTestProxies(t, testWireGuardMapping(51820)))
	reloaded := publishTestProxies(parseTestProxies(t, testWireGuardMapping(51820)))
	if reloaded["wg"] != running["wg"] {
		t.Fatal("unchanged proxy was rebuilt")
	}
	if parseTestProxies(t, testWireGuardMapping(51820))["wg"] != running["wg"] {
		t.Fatal("unchanged proxy was rebuilt on the second reload")
	}
}

func TestReuseUnchangedInlineProxyOfAnyType(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestProxies(parseTestProxies(t, testSSMapping()))
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
	parse := func() map[string]C.Proxy {
		raw, err := UnmarshalRawConfig([]byte(yaml))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := copyProxyValue(raw.Proxy[0]); !ok {
			t.Fatal("proxy read from yaml can't be compared")
		}
		return parseTestProxies(t, raw.Proxy...)
	}
	running := publishTestProxies(parse())
	if parse()["wg"] != running["wg"] {
		t.Fatal("unchanged proxy read from yaml was rebuilt")
	}
}

func TestRebuildChangedInlineProxy(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestProxies(parseTestProxies(t, testWireGuardMapping(51820)))
	if parseTestProxies(t, testWireGuardMapping(51821))["wg"] == running["wg"] {
		t.Fatal("changed proxy was reused")
	}
}

func TestKeepRunningProxyAfterUnappliedConfig(t *testing.T) {
	resetProxyReuse(t)
	running := publishTestProxies(parseTestProxies(t, testWireGuardMapping(51820)))
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
	running := publishTestProxies(parseTestProxies(t, testWireGuardMapping(51820)))
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

	running := publishTestProxies(parseTestProxies(t, mapping()))
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
	running := publishTestProxies(parseTestProxies(t, testSSMapping()))

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
