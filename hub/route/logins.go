package route

import (
	"encoding/json"
	"errors"
	"io"
	"sort"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// loginRouter serves the outbounds of the running config that a user logs in at run time.
func loginRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getLogins)
	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName)
		r.Post("/", login)
		r.Delete("/", logout)
	})
	return r
}

type loginEntry struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Instance string `json:"instance"`
	C.LoginConfig
	C.LoginStatus
}

// loginProxy is a node that logs in, as one snapshot of the proxies has it.
type loginProxy struct {
	outbound.LoginNode
	proxy C.Proxy
}

func loginProxies() []loginProxy {
	var nodes []loginProxy
	for name, proxy := range tunnel.Proxies() {
		if l, ok := outbound.LoginableOf(proxy.Adapter()); ok {
			nodes = append(nodes, loginProxy{LoginNode: outbound.LoginNode{Name: name, Loginable: l}, proxy: proxy})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes
}

func loginEntryOf(node loginProxy, status C.LoginStatus) loginEntry {
	return loginEntry{
		Name:        node.Name,
		Type:        node.proxy.Type().String(),
		Instance:    node.Loginable.LoginInstance(),
		LoginConfig: node.Loginable.LoginConfig(),
		LoginStatus: status,
	}
}

// getLogins lists the nodes without starting any. Each stateDir parameter, as a node of
// another config writes it, is matched against them.
func getLogins(w http.ResponseWriter, r *http.Request) {
	nodes := loginProxies()
	entries := make([]loginEntry, 0, len(nodes))
	loginNodes := make([]outbound.LoginNode, 0, len(nodes))
	for _, node := range nodes {
		entries = append(entries, loginEntryOf(node, node.Loginable.LoginStatus(r.Context())))
		loginNodes = append(loginNodes, node.LoginNode)
	}
	response := render.M{"logins": entries}
	if dirs, ok := r.URL.Query()["stateDir"]; ok {
		matches := make([]outbound.StateDirMatch, 0, len(dirs))
		for _, dir := range dirs {
			matches = append(matches, outbound.MatchStateDir(dir, loginNodes))
		}
		response["matches"] = matches
	}
	render.JSON(w, r, response)
}

// findLoginNode looks the node up in the proxies in use when it is called, which a request
// does once it has read its body: a node a new config replaced in the meantime is not found
// under the instance the caller listed.
func findLoginNode(r *http.Request) (loginProxy, bool) {
	name := r.Context().Value(CtxKeyProxyName).(string)
	for _, node := range loginProxies() {
		if node.Name == name {
			return node, true
		}
	}
	return loginProxy{}, false
}

func login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Instance string `json:"instance"`
		AuthKey  string `json:"authKey"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil {
		renderLoginError(w, r, &C.LoginError{Code: C.LoginInvalidRequest, Message: "invalid request body"})
		return
	}
	node, ok := findLoginNode(r)
	if !ok {
		renderLoginError(w, r, &C.LoginError{Code: C.LoginNotFound, Message: "no such node to log in"})
		return
	}
	status, err := node.Loginable.Login(r.Context(), req.Instance, req.AuthKey)
	if err != nil {
		renderLoginError(w, r, err)
		return
	}
	render.JSON(w, r, loginEntryOf(node, status))
}

func logout(w http.ResponseWriter, r *http.Request) {
	node, ok := findLoginNode(r)
	if !ok {
		renderLoginError(w, r, &C.LoginError{Code: C.LoginNotFound, Message: "no such node to log in"})
		return
	}
	status, err := node.Loginable.Logout(r.Context(), r.URL.Query().Get("instance"))
	if err != nil {
		renderLoginError(w, r, err)
		return
	}
	render.JSON(w, r, loginEntryOf(node, status))
}

func renderLoginError(w http.ResponseWriter, r *http.Request, err error) {
	var loginErr *C.LoginError
	if !errors.As(err, &loginErr) {
		loginErr = &C.LoginError{Code: C.LoginOtherError, Message: "login failed"}
	}
	code := http.StatusInternalServerError
	switch loginErr.Code {
	case C.LoginInvalidRequest, C.LoginRejected:
		code = http.StatusBadRequest
	case C.LoginNotFound:
		code = http.StatusNotFound
	case C.LoginLoggedIn, C.LoginStartFailed, C.LoginSuperseded, C.LoginReplaced:
		code = http.StatusConflict
	case C.LoginTimeout:
		code = http.StatusGatewayTimeout
	}
	render.Status(r, code)
	render.JSON(w, r, loginErr)
}
