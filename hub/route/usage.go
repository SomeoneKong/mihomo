package route

import (
	"strconv"

	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func usageRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/proxies", getProxyUsage)
	return r
}

// getProxyUsage returns the per-proxy usage of the hours from since to until (UTC hours
// since the Unix epoch, both inclusive). They default to the earliest hour kept and the
// current hour. With routes=true, the usage of each proxy is split by kind and route.
func getProxyUsage(w http.ResponseWriter, r *http.Request) {
	since, sinceErr := parseHour(r.URL.Query().Get("since"), 0)
	until, untilErr := parseHour(r.URL.Query().Get("until"), 1<<62)
	if sinceErr != nil || untilErr != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("since and until must be hours since the Unix epoch"))
		return
	}
	render.JSON(w, r, statistic.DefaultUsage.Snapshot(since, until, r.URL.Query().Get("routes") == "true"))
}

func parseHour(value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseInt(value, 10, 64)
}
