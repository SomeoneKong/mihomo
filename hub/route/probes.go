package route

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/component/probe"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func probeRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/events", getProbeEvents)
	r.Get("/contexts", getProbeContexts)
	r.Get("/groups", getProbeGroups)
	r.Get("/view", getProbeView)
	return r
}

func poolRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getPools)
	r.Put("/exclusions", putPoolExclusions)
	return r
}

// getProbeEvents returns the decision records after the sequence number after, at most limit
// of them.
func getProbeEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	after, err := parseUint(query.Get("after"), 0)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("after must be a sequence number"))
		return
	}
	limit, err := parseUint(query.Get("limit"), 1000)
	if err != nil || limit == 0 {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("limit must be a positive number"))
		return
	}
	render.JSON(w, r, probe.Default.Records().Events(after, int(minUint(limit, 100000))))
}

// getProbeContexts returns the params context and the asked config and exclusions versions,
// or every context still kept when no version is asked.
func getProbeContexts(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	parse := func(values []string) ([]uint64, bool) {
		var versions []uint64
		for _, value := range values {
			version, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, false
			}
			versions = append(versions, version)
		}
		return versions, true
	}
	configs, ok1 := parse(query["config"])
	exclusions, ok2 := parse(query["exclusions"])
	if !ok1 || !ok2 {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("versions must be numbers"))
		return
	}
	render.JSON(w, r, probe.Default.Records().Contexts(configs, exclusions))
}

func getProbeGroups(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, render.M{"groups": outboundgroup.State()})
}

// getProbeView returns the view of the strategy for a GUI, made of generic elements.
func getProbeView(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, outboundgroup.View())
}

type poolState struct {
	Pool     string     `json:"pool"`
	Nodes    int        `json:"nodes"`
	Excluded bool       `json:"excluded"`
	Until    *time.Time `json:"until"`
	Known    bool       `json:"known"`
}

// getPools returns the traffic pools of the config in use, and the excluded pools it does not
// know.
func getPools(w http.ResponseWriter, r *http.Request) {
	counts := outboundgroup.TrafficPools(tunnel.Proxies(), tunnel.Providers())
	excluded := map[string]probe.ExclusionEntry{}
	for _, entry := range probe.Default.Exclusions().List() {
		excluded[entry.Pool] = entry
	}
	var pools []poolState
	for pool, nodes := range counts {
		state := poolState{Pool: pool, Nodes: nodes, Known: true}
		if entry, ok := excluded[pool]; ok {
			state.Excluded = true
			if !entry.Until.IsZero() {
				until := entry.Until
				state.Until = &until
			}
		}
		pools = append(pools, state)
	}
	for pool, entry := range excluded {
		if _, ok := counts[pool]; ok {
			continue
		}
		state := poolState{Pool: pool, Excluded: true}
		if !entry.Until.IsZero() {
			until := entry.Until
			state.Until = &until
		}
		pools = append(pools, state)
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].Pool < pools[j].Pool })
	if pools == nil {
		pools = []poolState{}
	}
	render.JSON(w, r, render.M{"pools": pools})
}

// putPoolExclusions replaces the table of excluded traffic pools.
func putPoolExclusions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Pools []probe.ExclusionEntry `json:"pools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}
	for _, entry := range req.Pools {
		if entry.Pool == "" {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("pool must not be empty"))
			return
		}
	}
	probe.Default.Exclusions().Replace(req.Pools)
	render.NoContent(w, r)
}

func parseUint(value string, fallback uint64) (uint64, error) {
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func minUint(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
