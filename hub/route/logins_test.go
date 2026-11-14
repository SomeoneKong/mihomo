package route

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

// fakeLoginAdapter is a direct outbound that a user logs in, recording what it is asked.
type fakeLoginAdapter struct {
	*outbound.Direct
	instance string
	stateDir string
	err      error
	calls    []string
	keys     []string
}

func (f *fakeLoginAdapter) LoginInstance() string {
	if f.instance != "" {
		return f.instance
	}
	return "instance-1"
}

func (f *fakeLoginAdapter) LoginConfig() C.LoginConfig {
	return C.LoginConfig{ControlURL: "http://hs.example", StateDir: f.stateDir, ConfigAuthKey: true}
}

func (f *fakeLoginAdapter) LoginStatus(context.Context) C.LoginStatus {
	f.calls = append(f.calls, "status")
	return C.LoginStatus{State: C.LoginNotStarted}
}

func (f *fakeLoginAdapter) Login(_ context.Context, instance, authKey string) (C.LoginStatus, error) {
	f.calls = append(f.calls, "login:"+instance)
	f.keys = append(f.keys, authKey)
	if f.err != nil {
		return C.LoginStatus{}, f.err
	}
	return C.LoginStatus{State: C.LoginRunning, Identity: true, Device: "laptop"}, nil
}

func (f *fakeLoginAdapter) Logout(_ context.Context, instance string) (C.LoginStatus, error) {
	f.calls = append(f.calls, "logout:"+instance)
	if f.err != nil {
		return C.LoginStatus{}, f.err
	}
	return C.LoginStatus{State: C.LoginNeedsLogin}, nil
}

func useLoginProxies(t *testing.T, fakes map[string]*fakeLoginAdapter) {
	t.Helper()
	proxies := map[string]C.Proxy{
		"direct": adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "direct"})),
	}
	for name, fake := range fakes {
		fake.Direct = outbound.NewDirectWithOption(outbound.DirectOption{Name: name})
		proxies[name] = adapter.NewProxy(fake)
	}
	tunnel.UpdateProxies(proxies, nil)
	t.Cleanup(func() { tunnel.UpdateProxies(nil, nil) })
}

func serveLogins(method, target, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	loginRouter().ServeHTTP(recorder, httptest.NewRequest(method, target, strings.NewReader(body)))
	return recorder
}

func TestGetLoginsListsLoginableNodesWithoutStartingThem(t *testing.T) {
	b, a := &fakeLoginAdapter{stateDir: "/state/b"}, &fakeLoginAdapter{stateDir: "/state/a"}
	useLoginProxies(t, map[string]*fakeLoginAdapter{"b": b, "a": a})

	res := serveLogins("GET", "/", "")
	require.Equal(t, 200, res.Code)
	var got struct {
		Logins []map[string]any `json:"logins"`
	}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &got))
	require.Len(t, got.Logins, 2)
	require.Equal(t, "a", got.Logins[0]["name"])
	require.Equal(t, "b", got.Logins[1]["name"])
	require.Equal(t, "instance-1", got.Logins[0]["instance"])
	require.Equal(t, "http://hs.example", got.Logins[0]["controlUrl"])
	require.Equal(t, true, got.Logins[0]["configAuthKey"])
	require.Equal(t, "not_started", got.Logins[0]["state"])
	require.Equal(t, []string{"status"}, a.calls, "listing only reads the status")
	require.NotContains(t, res.Body.String(), "matches")
}

func TestGetLoginsMatchesStateDirs(t *testing.T) {
	previous := C.Path.HomeDir()
	home := t.TempDir()
	C.SetHomeDir(home)
	t.Cleanup(func() { C.SetHomeDir(previous) })
	useLoginProxies(t, map[string]*fakeLoginAdapter{"a": {stateDir: home + "/a"}})

	res := serveLogins("GET", "/?stateDir=a&stateDir=", "")
	require.Equal(t, 200, res.Code)
	var got struct {
		Matches []outbound.StateDirMatch `json:"matches"`
	}
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &got))
	require.Len(t, got.Matches, 2)
	require.Equal(t, "a", got.Matches[0].StateDir)
	require.Equal(t, []outbound.LoginNodeRef{{Name: "a", Instance: "instance-1"}}, got.Matches[0].Nodes)
	require.Empty(t, got.Matches[1].Nodes, "no state-dir is the default dir, which no node uses")
}

func TestLoginPassesTheInstanceAndKeepsTheKeyOutOfTheResponse(t *testing.T) {
	fake := &fakeLoginAdapter{}
	useLoginProxies(t, map[string]*fakeLoginAdapter{"a/b": fake})

	res := serveLogins("POST", "/a%2Fb", `{"instance":"instance-1","authKey":"tskey-auth-secret"}`)
	require.Equal(t, 200, res.Code, res.Body.String())
	require.Equal(t, []string{"login:instance-1"}, fake.calls)
	require.Equal(t, []string{"tskey-auth-secret"}, fake.keys)
	require.Contains(t, res.Body.String(), `"state":"running"`)
	require.NotContains(t, res.Body.String(), "tskey-auth-secret")
}

func TestLoginErrorsCarryTheirCodes(t *testing.T) {
	cases := []struct {
		code   C.LoginErrorCode
		status int
	}{
		{C.LoginLoggedIn, 409},
		{C.LoginStartFailed, 409},
		{C.LoginRejected, 400},
		{C.LoginSuperseded, 409},
		{C.LoginReplaced, 409},
		{C.LoginTimeout, 504},
		{C.LoginOtherError, 500},
	}
	for _, c := range cases {
		fake := &fakeLoginAdapter{err: &C.LoginError{Code: c.code, Message: "why"}}
		useLoginProxies(t, map[string]*fakeLoginAdapter{"a": fake})
		for _, res := range []*httptest.ResponseRecorder{
			serveLogins("POST", "/a", `{"instance":"instance-1","authKey":"tskey-auth-secret"}`),
			serveLogins("DELETE", "/a?instance=instance-1", ""),
		} {
			require.Equal(t, c.status, res.Code, c.code)
			var got C.LoginError
			require.NoError(t, json.Unmarshal(res.Body.Bytes(), &got))
			require.Equal(t, c.code, got.Code)
			require.NotContains(t, res.Body.String(), "tskey-auth-secret")
		}
		require.Equal(t, []string{"login:instance-1", "logout:instance-1"}, fake.calls)
	}
}

// replacingBody publishes new proxies once the request body is read.
type replacingBody struct {
	io.Reader
	replace func()
	once    sync.Once
}

func (b *replacingBody) Read(p []byte) (int, error) {
	b.once.Do(b.replace)
	return b.Reader.Read(p)
}

func TestLoginLooksTheNodeUpOnceTheBodyIsRead(t *testing.T) {
	old, replacement := &fakeLoginAdapter{}, &fakeLoginAdapter{instance: "instance-2"}
	useLoginProxies(t, map[string]*fakeLoginAdapter{"a": old})
	body := &replacingBody{
		Reader: strings.NewReader(`{"instance":"instance-1","authKey":"tskey-auth-secret"}`),
		replace: func() {
			replacement.Direct = outbound.NewDirectWithOption(outbound.DirectOption{Name: "a"})
			tunnel.UpdateProxies(map[string]C.Proxy{"a": adapter.NewProxy(replacement)}, nil)
		},
	}
	recorder := httptest.NewRecorder()
	loginRouter().ServeHTTP(recorder, httptest.NewRequest("POST", "/a", body))
	require.Empty(t, old.calls, "the node a new config replaced is not logged in")
	require.Equal(t, []string{"login:instance-1"}, replacement.calls, "the node in use gets the instance the caller listed")
}

func TestLoginOfAnUnknownNodeOrWithABadBody(t *testing.T) {
	useLoginProxies(t, map[string]*fakeLoginAdapter{"a": {}})

	res := serveLogins("POST", "/direct", `{}`)
	require.Equal(t, 404, res.Code, "a node that cannot log in")
	require.Contains(t, res.Body.String(), `"code":"not_found"`)
	require.Equal(t, 404, serveLogins("DELETE", "/missing", "").Code)

	res = serveLogins("POST", "/a", `{"instance":`)
	require.Equal(t, 400, res.Code)
	require.Contains(t, res.Body.String(), `"code":"invalid_request"`)
}

func TestLoginTakesOneJSONObjectWithinTheLimit(t *testing.T) {
	fake := &fakeLoginAdapter{}
	useLoginProxies(t, map[string]*fakeLoginAdapter{"a": fake})
	request := `{"instance":"instance-1","authKey":"tskey-auth-secret"}`
	for _, body := range []string{
		request + ` trailing`,
		request + request,
		request + strings.Repeat(" ", 16<<10),
	} {
		res := serveLogins("POST", "/a", body)
		require.Equal(t, 400, res.Code)
		require.Contains(t, res.Body.String(), `"code":"invalid_request"`)
	}
	require.Empty(t, fake.calls)

	require.Equal(t, 200, serveLogins("POST", "/a", request+" \n").Code)
	require.Equal(t, []string{"login:instance-1"}, fake.calls)
}
