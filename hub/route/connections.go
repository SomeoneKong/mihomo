package route

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func connectionRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getConnections)
	r.Delete("/", closeAllConnections)
	r.Delete("/{id}", closeConnection)
	return r
}

func getConnections(w http.ResponseWriter, r *http.Request) {
	if !(r.Header.Get("Upgrade") == "websocket") {
		snapshot := connectionSnapshot(r)
		render.JSON(w, r, snapshot)
		return
	}

	intervalStr := r.URL.Query().Get("interval")
	interval := 1000
	if intervalStr != "" {
		t, err := strconv.Atoi(intervalStr)
		if err != nil || t <= 0 || int64(t) > math.MaxInt64/int64(time.Millisecond) {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, ErrBadRequest)
			return
		}

		interval = t
	}

	conn, rw, err := wsUpgrade(r, w)
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return
	}
	// Nothing the client sends is used; reading only notices when it goes away.
	clientGone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, rw.Reader)
		close(clientGone)
	}()

	buf := &bytes.Buffer{}
	sendSnapshot := func() error {
		buf.Reset()
		snapshot := connectionSnapshot(r)
		if err := json.NewEncoder(buf).Encode(snapshot); err != nil {
			return err
		}

		return wsWriteServerText(conn, buf.Bytes())
	}

	if err := sendSnapshot(); err != nil {
		return
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer tick.Stop()
	for {
		select {
		case <-clientGone:
			return
		case <-tick.C:
			if err := sendSnapshot(); err != nil {
				return
			}
		}
	}
}

// connectionSnapshot leaves out the connections that carry other proxies' traffic, which the
// connections they carry describe with their dialer chains, unless asked with underlying=true.
func connectionSnapshot(r *http.Request) *statistic.Snapshot {
	snapshot := statistic.DefaultManager.Snapshot()
	if r.URL.Query().Get("underlying") != "true" {
		snapshot.Connections = slices.DeleteFunc(snapshot.Connections, (*statistic.TrackerInfo).Underlying)
	}
	return snapshot
}

func closeConnection(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if c := statistic.DefaultManager.Get(id); c != nil {
		_ = c.Close()
	}
	render.NoContent(w, r)
}

func closeAllConnections(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("outdated") == "true" {
		closeOutdatedConnections(w, r)
		return
	}
	statistic.DefaultManager.Range(func(c statistic.Tracker) bool {
		_ = c.Close()
		return true
	})
	render.NoContent(w, r)
}
