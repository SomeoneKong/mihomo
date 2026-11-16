package route

import (
	"encoding/json"
	"strings"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPatchConfigsSetsConnectionStats(t *testing.T) {
	saved := statistic.DefaultConnStats.Enabled()
	t.Cleanup(func() { statistic.DefaultConnStats.SetEnabled(saved) })

	for _, enabled := range []bool{true, false} {
		body := `{"connection-stats":` + map[bool]string{true: "true", false: "false"}[enabled] + `}`
		recorder := httptest.NewRecorder()
		patchConfigs(recorder, httptest.NewRequest("PATCH", "/configs", strings.NewReader(body)))
		require.Equal(t, http.StatusNoContent, recorder.Code)
		assert.Equal(t, enabled, statistic.DefaultConnStats.Enabled())
		assert.Equal(t, enabled, executor.GetGeneral().ConnectionStats)
	}

	recorder := httptest.NewRecorder()
	patchConfigs(recorder, httptest.NewRequest("PATCH", "/configs", strings.NewReader(`{"log-level":"info"}`)))
	require.Equal(t, http.StatusNoContent, recorder.Code)
	assert.False(t, statistic.DefaultConnStats.Enabled(), "a patch without the key leaves it")
}

func TestConnectionUsageQuery(t *testing.T) {
	get := func(query string) (int, map[string]any) {
		recorder := httptest.NewRecorder()
		getConnectionUsage(recorder, httptest.NewRequest("GET", "/usage/connections"+query, nil))
		var body map[string]any
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		return recorder.Code, body
	}
	code, body := get("")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, statistic.Instance(), body["instance"])
	assert.Contains(t, body, "tick")
	assert.Contains(t, body, "counting")

	code, _ = get("?touchedSince=1e3")
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = get("?touchedSince=5")
	assert.Equal(t, http.StatusOK, code)
}

func TestClosedConnectionsAndInstance(t *testing.T) {
	tracker := statistic.NewTCPTracker(&closingConn{fakeConn{chain: C.Chain{"hk", "group"}}}, statistic.DefaultManager, &C.Metadata{}, nil, 0, 0, false)
	_ = tracker.Close()

	recorder := httptest.NewRecorder()
	getClosedConnections(recorder, httptest.NewRequest("GET", "/connections/closed?limit=100000", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	var closed struct {
		Instance string `json:"instance"`
		Latest   int64  `json:"latest"`
		Records  []struct {
			ID  string `json:"id"`
			Seq int64  `json:"seq"`
		} `json:"records"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &closed))
	assert.Equal(t, statistic.Instance(), closed.Instance)
	require.NotEmpty(t, closed.Records)
	assert.Equal(t, tracker.ID(), closed.Records[len(closed.Records)-1].ID)

	for _, query := range []string{"?limit=0", "?after=x"} {
		recorder = httptest.NewRecorder()
		getClosedConnections(recorder, httptest.NewRequest("GET", "/connections/closed"+query, nil))
		assert.Equal(t, http.StatusBadRequest, recorder.Code, query)
	}

	recorder = httptest.NewRecorder()
	getConnections(recorder, httptest.NewRequest("GET", "/connections", nil))
	var snapshot struct {
		Instance string `json:"instance"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &snapshot))
	assert.Equal(t, statistic.Instance(), snapshot.Instance)
}

// closingConn is a fakeConn a tracker can close.
type closingConn struct {
	fakeConn
}

func (c *closingConn) Close() error { return nil }
