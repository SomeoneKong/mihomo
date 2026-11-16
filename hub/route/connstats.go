package route

import (
	"strconv"

	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// getConnectionUsage returns the connection stats of the hours from since to until (UTC
// hours since the Unix epoch, both inclusive), which default to the earliest hour kept and
// the current hour. With touchedSince, a tick of an earlier response, only the entries
// counted at that tick or later are returned.
func getConnectionUsage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	since, sinceErr := parseHour(query.Get("since"), 0)
	until, untilErr := parseHour(query.Get("until"), 1<<62)
	if sinceErr != nil || untilErr != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("since and until must be hours since the Unix epoch"))
		return
	}
	var touchedSince int64
	incremental := query.Get("touchedSince") != ""
	if incremental {
		var err error
		if touchedSince, err = strconv.ParseInt(query.Get("touchedSince"), 10, 64); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, newError("touchedSince must be a tick of an earlier response"))
			return
		}
	}
	render.JSON(w, r, statistic.DefaultConnStats.Snapshot(since, until, incremental, touchedSince))
}

// getClosedConnections returns up to limit of the connections closed recently, those with
// sequence numbers after after, ascending.
func getClosedConnections(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	after, afterErr := parseHour(query.Get("after"), 0) // a sequence number, parsed alike
	limit := statistic.ClosedDefaultLimit
	var limitErr error
	if value := query.Get("limit"); value != "" {
		limit, limitErr = strconv.Atoi(value)
	}
	if afterErr != nil || limitErr != nil || limit <= 0 {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("after must be a sequence number and limit a positive count"))
		return
	}
	render.JSON(w, r, statistic.DefaultClosed.Snapshot(after, limit))
}
