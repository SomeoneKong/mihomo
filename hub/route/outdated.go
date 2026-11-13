package route

import (
	"context"
	"time"

	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// outdatedJudgeTimeout bounds waiting for the config updates queued before and judging
// the connections, as matching them again may wait on DNS.
var outdatedJudgeTimeout = 10 * time.Second

// closeOutdatedConnections closes the connections the config in use would not carry the
// way they are carried, as after switching to another config. Only the connections there
// are when asked are judged. Config updates are held off while judging, so all of them
// are judged against the same config. Closing doesn't need that, and is started once
// judging is over: closing some connections sends a last frame that can take a while.
func closeOutdatedConnections(w http.ResponseWriter, r *http.Request) {
	var trackers []statistic.Tracker
	statistic.DefaultManager.Range(func(t statistic.Tracker) bool {
		trackers = append(trackers, t)
		return true
	})
	ctx, cancel := context.WithTimeout(r.Context(), outdatedJudgeTimeout)
	defer cancel()
	unlock, err := executor.LockUpdateContext(ctx)
	if err != nil {
		render.JSON(w, r, tunnel.OutdatedResult{Undetermined: len(trackers)})
		return
	}
	outdated, result := tunnel.FindOutdatedConnections(ctx, trackers)
	unlock()
	for _, tracker := range outdated {
		go func(tracker statistic.Tracker) { _ = tracker.Close() }(tracker)
	}
	render.JSON(w, r, result)
}
