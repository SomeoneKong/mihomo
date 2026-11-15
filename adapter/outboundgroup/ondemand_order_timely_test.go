package outboundgroup

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/yaml"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"

	"github.com/stretchr/testify/require"
)

func TestOnDemandOrderTimelyOption(t *testing.T) {
	for _, tt := range []struct {
		name        string
		options     string
		wantOrder   string
		wantTimely  int
		wantWarning string
	}{
		{"Omitted", "order: reliable", OrderReliable, 1000, ""},
		{"DefaultTimeout", "order: reliable\norder-timely: 2000", OrderReliable, 2000, ""},
		{"Minimum", "order: reliable\ntimeout: 800\norder-timely: 100", OrderReliable, 100, ""},
		{"BelowShortTimeout", "order: reliable\ntimeout: 800\norder-timely: 799", OrderReliable, 799, ""},
		{"EqualToShortTimeout", "order: reliable\ntimeout: 800\norder-timely: 800", OrderReliable, 1000, "uses the default"},
		{"ExplicitDefaultAboveShortTimeout", "order: reliable\ntimeout: 800\norder-timely: 1000", OrderReliable, 1000, "uses the default"},
		{"AboveShortTimeout", "order: reliable\ntimeout: 800\norder-timely: 2000", OrderReliable, 1000, "uses the default"},
		{"BelowLongTimeout", "order: reliable-cost\ntimeout: 5000\norder-timely: 4999", OrderReliableCost, 4999, ""},
		{"EqualToLongTimeout", "order: reliable-cost\ntimeout: 5000\norder-timely: 5000", OrderReliableCost, 1000, "uses the default"},
		{"BelowMinimum", "order: reliable\norder-timely: 99", OrderReliable, 1000, "uses the default"},
		{"ZeroIsOmitted", "order: reliable\norder-timely: 0", OrderReliable, 1000, ""},
		{"QuotedNumber", "order: reliable\norder-timely: '2000'", OrderReliable, 2000, ""},
		{"NoOrder", "order-timely: 2000", OrderConfig, 1000, "ignores order-timely"},
		{"StrictOrder", "strict-order: true\norder: reliable\norder-timely: 2000", OrderConfig, 1000, "ignores order-timely"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const groupName = "timely-option-test"
			proxies := map[string]C.Proxy{
				"A":          adapter.NewProxy(&routeAdapter{Direct: outbound.NewDirectWithOption(outbound.DirectOption{Name: "A"})}),
				"COMPATIBLE": adapter.NewProxy(outbound.NewCompatible()),
			}
			config := map[string]any{}
			input := fmt.Sprintf("name: %s\ntype: fallback\nprobe: on-demand\nproxies: [A]\n%s", groupName, tt.options)
			require.NoError(t, yaml.Unmarshal([]byte(input), &config))
			logs := log.Subscribe()
			defer log.UnSubscribe(logs)
			group, err := ParseProxyGroup(config, proxies, map[string]P.ProxyProvider{}, nil, nil)
			require.NoError(t, err)
			f := group.(*Fallback)
			t.Cleanup(f.od.close)
			require.Equal(t, tt.wantOrder, f.od.ordering)
			require.Equal(t, tt.wantOrder == OrderConfig, f.od.prefer == nil)

			proxies[groupName] = adapter.NewProxy(f)
			context := configContext(proxies, map[string]*onDemand{groupName: f.od}, "load", 0)
			groups := context["groups"].([]any)
			require.Len(t, groups, 1)
			fields := groups[0].(map[string]any)
			require.EqualValues(t, tt.wantTimely, fields["orderTimely"])
			require.Equal(t, tt.wantOrder, fields["order"])

			const marker = "Finished parsing the order-timely test group"
			log.Debugln(marker)
			var warnings []string
			for event := range logs {
				if event.Payload == marker {
					break
				}
				if event.LogLevel == log.WARNING && strings.Contains(event.Payload, "["+groupName+"]") && strings.Contains(event.Payload, "order-timely") {
					warnings = append(warnings, event.Payload)
				}
			}
			if tt.wantWarning == "" {
				require.Empty(t, warnings)
			} else {
				require.Len(t, warnings, 1)
				require.Contains(t, warnings[0], tt.wantWarning)
			}
		})
	}
}

func TestOnDemandOrderTimelyCostPreference(t *testing.T) {
	for _, tt := range []struct {
		name      string
		timely    int
		cheap     []time.Duration
		wantTier  string
		preferred string
	}{
		{"DefaultPromotes", 1000, []time.Duration{1100 * time.Millisecond, 1300 * time.Millisecond, 1500 * time.Millisecond}, "slow", "B"},
		{"RaisedThresholdTies", 2000, []time.Duration{1100 * time.Millisecond, 1300 * time.Millisecond, 1500 * time.Millisecond}, "timely", ""},
		{"RaisedThresholdStillPromotes", 2000, []time.Duration{3 * time.Second, 3 * time.Second, 3 * time.Second}, "slow", "B"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ot, clock := newClockedODTest(t)
			ot.runner.set(true, "A", "B")
			ot.runner.setFirst(650*time.Millisecond, "B")
			f := ot.buildOption(GroupCommonOption{Name: "g", Order: OrderReliableCost, OrderTimely: tt.timely, TestTimeout: 5000}, "A", "B")
			ot.activate(f)
			ot.settle(f)
			require.Equal(t, "A", ot.current(f))
			for i, elapsed := range tt.cheap {
				ot.runner.setFirst(elapsed, "A")
				event := ot.compareRound(f, clock)
				require.Equal(t, "done", event["end"])
				require.EqualValues(t, tt.timely, event["timely"])
				members := event["members"].([]any)
				require.Len(t, members, 2)
				require.Equal(t, tt.wantTier, members[0].(map[string]any)["first"])
				require.EqualValues(t, elapsed.Milliseconds(), members[0].(map[string]any)["firstElapsed"])
				require.Equal(t, "timely", members[1].(map[string]any)["first"])
				require.EqualValues(t, 650, members[1].(map[string]any)["firstElapsed"])
				if i < len(tt.cheap)-1 {
					require.Empty(t, ot.preferred(f))
				}
			}
			require.Equal(t, tt.preferred, ot.preferred(f))

			var params struct {
				Kind    string
				Compare struct{ Timely int }
			}
			contexts := ot.coord.Records().Contexts(nil, nil).Contexts
			require.NotEmpty(t, contexts)
			require.NoError(t, json.Unmarshal(contexts[0], &params))
			require.Equal(t, probe.ContextParams, params.Kind)
			require.Equal(t, 1000, params.Compare.Timely)
		})
	}
}

func TestOnDemandOrderTimelyReload(t *testing.T) {
	for _, tt := range []struct {
		name  string
		old   int
		fresh int
		keep  bool
	}{
		{"Changed", 1000, 2000, false},
		{"OmittedToExplicitDefault", 0, 1000, true},
		{"ExplicitDefaultToOmitted", 1000, 0, true},
		{"InvalidToDefault", 99, 1000, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ot, clock := newClockedODTest(t)
			release := ot.runner.holdOn("C")
			t.Cleanup(func() { close(release) })
			option := GroupCommonOption{Name: "g", Order: OrderReliable, OrderTimely: tt.old, TestTimeout: 5000}
			old := ot.buildOption(option, "A", "B", "C")
			ot.activate(old)
			ot.settle(old)
			a, b, c := ot.proxies["A"], ot.proxies["B"], ot.proxies["C"]
			now := clock.Now()
			lastDone, nextAt, retryAt := now.Add(-10*time.Minute), now.Add(20*time.Minute), now.Add(time.Minute)
			confirm := confirmation{exec: 42, after: now}
			old.od.mutex.Lock()
			noteRound(old.od, lastDone, tiered{a, tierSlow}, tiered{b, tierTimely})
			ps := old.od.prefer
			ps.preferred, ps.since, ps.seen, ps.lastChange = b, lastDone, lastDone, lastDone
			ps.basis = ps.records[[2]C.Proxy{a, b}].results
			ps.lastDone, ps.nextAt, ps.retryAt, ps.cursor = lastDone, nextAt, retryAt, b
			ps.owed[b], ps.confirms[c] = true, confirm
			q := old.od.qualityOf(c)
			q.standing, q.readmitAt = standingYield, now.Add(time.Hour)
			old.od.computeOrder()
			old.od.mutex.Unlock()

			option.OrderTimely = tt.fresh
			fresh := ot.buildOption(option, "A", "B", "C")
			ot.replace(old, fresh)
			ot.settle(fresh)
			fresh.od.mutex.Lock()
			defer fresh.od.mutex.Unlock()
			ps = fresh.od.prefer
			require.Equal(t, lastDone, ps.lastDone)
			require.Equal(t, nextAt, ps.nextAt)
			require.Equal(t, retryAt, ps.retryAt)
			require.Equal(t, b, ps.cursor)
			require.True(t, ps.owed[b])
			require.Equal(t, confirm, ps.confirms[c])
			require.Equal(t, standingYield, fresh.od.quality[c].standing)
			events := ot.groupEvents("preference")
			if tt.keep {
				require.Len(t, ps.records[[2]C.Proxy{a, b}].results, 1)
				require.Equal(t, b, ps.preferred)
				require.Equal(t, lastDone, ps.since)
				require.Equal(t, lastDone, ps.seen)
				require.Equal(t, lastDone, ps.lastChange)
				require.Len(t, ps.basis, 1)
				require.Equal(t, b, fresh.od.current)
				require.Empty(t, events)
			} else {
				require.Empty(t, ps.records)
				require.Nil(t, ps.preferred)
				require.Zero(t, ps.since)
				require.Zero(t, ps.seen)
				require.Zero(t, ps.lastChange)
				require.Empty(t, ps.basis)
				require.Equal(t, a, fresh.od.current)
				require.Len(t, events, 1)
				event := events[0]
				require.Equal(t, causePreferenceEnd, event["reason"])
				require.Equal(t, "timelyChanged", event["why"])
				require.EqualValues(t, 1000, event["oldTimely"])
				require.EqualValues(t, 2000, event["timely"])
				display := event["display"].(map[string]any)["text"].(map[string]any)
				require.Contains(t, display["zh"], "及时界限已改变")
				require.Contains(t, display["en"], "the timely threshold changed")
			}
		})
	}
}
