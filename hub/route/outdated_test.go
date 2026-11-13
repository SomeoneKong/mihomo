package route

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

// idleTracker is a connection with no chain, which judging can't tell about.
type idleTracker struct {
	statistic.Tracker
	info *statistic.TrackerInfo
}

func (t *idleTracker) ID() string                   { return t.info.UUID.String() }
func (t *idleTracker) Info() *statistic.TrackerInfo { return t.info }

func joinIdleTracker(t *testing.T) {
	tracker := &idleTracker{info: &statistic.TrackerInfo{UUID: utils.NewUUIDV4(), Metadata: &C.Metadata{}}}
	statistic.DefaultManager.Join(tracker)
	t.Cleanup(func() { statistic.DefaultManager.Leave(tracker) })
}

func TestOutdatedConnectionsAreTheOnesThereWhenAsked(t *testing.T) {
	joinIdleTracker(t)
	unlock := executor.LockUpdate()
	answer := make(chan tunnel.OutdatedResult, 1)
	go func() {
		w := httptest.NewRecorder()
		closeOutdatedConnections(w, httptest.NewRequest(http.MethodDelete, "/connections?outdated=true", nil))
		var result tunnel.OutdatedResult
		_ = json.Unmarshal(w.Body.Bytes(), &result)
		answer <- result
	}()
	time.Sleep(100 * time.Millisecond) // queued behind the update
	joinIdleTracker(t)
	unlock()
	select {
	case result := <-answer:
		if result.Closed+result.Kept+result.Undetermined != 1 {
			t.Fatalf("judged a connection made after asking: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never answered")
	}
}
