package route

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/require"
)

type memoryGroup struct {
	name  string
	inbox chan probe.MemoryCleared
}

func (g memoryGroup) Name() string                             { return g.name }
func (g memoryGroup) References(C.Proxy, probe.Condition) bool { return false }
func (g memoryGroup) Deliver(n probe.Notice) {
	if m, ok := n.(probe.MemoryCleared); ok {
		g.inbox <- m
	}
}

func TestClearMemoryRouteWaitsForEveryInstanceOfAName(t *testing.T) {
	original := probe.Default
	t.Cleanup(func() { probe.Default = original })
	c := probe.New(time.Now, nil)
	probe.Default = c
	old := memoryGroup{"g", make(chan probe.MemoryCleared, 1)}
	c.Register(old)
	c.Activate(nil, nil)
	c.BeginPublish()
	fresh := memoryGroup{"g", make(chan probe.MemoryCleared, 1)}
	other := memoryGroup{"other", make(chan probe.MemoryCleared, 1)}
	c.Register(fresh)
	c.Register(other)
	r := probeRouter()
	next := func(group memoryGroup) probe.MemoryCleared {
		t.Helper()
		select {
		case m := <-group.inbox:
			return m
		case <-time.After(3 * time.Second):
			t.Fatal("no clear notice for " + group.name)
			return probe.MemoryCleared{}
		}
	}
	request := func() <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/memory", nil))
			done <- w
		}()
		return done
	}
	first := request()
	late := next(old)
	next(fresh).Processed()
	next(other).Processed()
	second := request()
	response := <-first
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"clear":1,"groups":1,"pending":1}`, response.Body.String())
	late.Processed()
	for _, group := range []memoryGroup{old, fresh, other} {
		m := next(group)
		require.Equal(t, uint64(2), m.Clear)
		m.Processed()
	}
	require.JSONEq(t, `{"clear":2,"groups":2,"pending":0}`, (<-second).Body.String())

	var events []map[string]any
	for _, raw := range c.Records().Events(0, 100).Events {
		var e map[string]any
		require.NoError(t, json.Unmarshal(raw, &e))
		if e["kind"] == "performance" {
			events = append(events, e)
		}
	}
	require.Len(t, events, 4)
	for i, e := range events {
		require.EqualValues(t, 1+i/2, e["clear"])
		display := e["display"].(map[string]any)
		require.Equal(t, "global", display["scope"].(map[string]any)["kind"])
		require.Equal(t, map[string]any{"en": "quality", "zh": "质量"}, display["categoryText"])
		message := display["text"].(map[string]any)
		require.NotEmpty(t, message["en"])
		if i%2 == 0 {
			require.Equal(t, "memoryCleared", e["event"])
			require.Equal(t, "已受理清除策略记忆，共享计次已清除", message["zh"])
		} else {
			require.Equal(t, "memoryClearDone", e["event"])
			if i == 1 {
				require.Equal(t, "已清除 1 个组的策略记忆，1 个组稍后完成", message["zh"])
			} else {
				require.Equal(t, "已清除 2 个组的策略记忆", message["zh"])
			}
		}
	}
}
