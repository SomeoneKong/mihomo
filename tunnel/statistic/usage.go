package statistic

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

const (
	// UsageRetainHours is how many hours before the current one the per-proxy usage keeps.
	UsageRetainHours = 31 * 24
	// UsageOverflowName names the entry that takes the traffic of proxies beyond UsageMaxEntries.
	UsageOverflowName = "*overflow*"
	// UsageMaxEntries bounds the number of proxies tracked individually.
	UsageMaxEntries = 10000
)

// DefaultUsage records the traffic of every tracked connection per outbound proxy and hour.
var DefaultUsage *Usage

func init() {
	DefaultUsage = newUsage(time.Now)
	go DefaultUsage.run()
}

// OutboundIdentity is implemented by the connections outbound adapters build.
type OutboundIdentity interface {
	OutboundIdentity() (adapterType C.AdapterType, addr string)
}

// UsageKey identifies what a connection's traffic is credited to: the proxy that made it,
// and whether the connection relays another proxy's traffic or carries traffic of its own
// along a route.
type UsageKey struct {
	Provider string
	Name     string
	Type     string
	Addr     string
	// Incomplete is set when the connection did not reveal the adapter that made it.
	Incomplete bool
	// Relay is set for the connections a dialer proxy made to carry another proxy's.
	Relay bool
	// Route is the UsageRoute of the other connections, encoded.
	Route string
}

// UsageRoute is the route a connection took: the groups from the proxy that made it out to
// the one a rule chose, and, layer by layer, the dialer proxies it reached its server
// through, each as the node and then its groups.
type UsageRoute struct {
	Groups  []string   `json:"groups,omitempty"`
	Dialers [][]string `json:"dialers,omitempty"`
}

func usageKeyOf(conn any, info *TrackerInfo, adapter C.ProxyAdapter) UsageKey {
	var key UsageKey
	if len(info.Chain) > 0 {
		key.Name = info.Chain[0]
	}
	if len(info.ProviderChain) > 0 {
		key.Provider = info.ProviderChain[0]
	}
	// The first identity met is the proxy this connection was made by; going further down
	// would reach the connection of a dialer proxy it was relayed through.
	if identity, ok := N.FindUpstream[OutboundIdentity](conn, nil); ok {
		adapterType, addr := identity.OutboundIdentity()
		key.Type = adapterType.String()
		key.Addr = addr
	} else {
		key.Incomplete = true
	}
	if info.underlying {
		key.Relay = true
		return key
	}
	route := UsageRoute{Dialers: usageDialers(adapter, info.dialedLayers)}
	if len(info.Chain) > 1 {
		route.Groups = slices.Clone(info.Chain[1:])
	}
	encoded, _ := json.Marshal(route)
	key.Route = string(encoded)
	return key
}

type usageEntry = hourEntry[UsageKey]

// Usage counts traffic per outbound proxy in UTC hours.
//
// Hours before the current one are sealed once the clock passes them, and are final from
// then on; settled is the latest such hour. Hours before retainedSince are dropped.
type Usage struct {
	*hourTable[UsageKey]
	instance  string
	startedAt time.Time
}

func newUsage(now func() time.Time) *Usage {
	startedAt := now()
	// The overflow takes the traffic of keys beyond UsageMaxEntries: that of connections of
	// their own first, that of relays second.
	overflow := []UsageKey{{Name: UsageOverflowName}, {Name: UsageOverflowName, Relay: true}}
	table := newHourTable(now, UsageRetainHours, UsageMaxEntries, overflow, func(key UsageKey) int {
		if key.Relay {
			return 1
		}
		return 0
	})
	return &Usage{
		hourTable: table,
		instance:  utils.NewUUIDV4().String(),
		startedAt: startedAt,
	}
}

func (u *Usage) run() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		u.tick()
	}
}

// UsageBucket is the traffic of one proxy in one hour.
type UsageBucket struct {
	Hour int64 `json:"hour"`
	Up   int64 `json:"up"`
	Down int64 `json:"down"`
}

// UsageEntry is the traffic of one proxy, or with routes, of one of its kinds and routes.
type UsageEntry struct {
	Provider   string `json:"provider"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Addr       string `json:"addr"`
	Incomplete bool   `json:"incomplete"`
	// Kind is "relay" for the traffic the proxy relayed for others, "exit" for the rest.
	Kind string `json:"kind,omitempty"`
	UsageRoute
	Buckets []UsageBucket `json:"buckets"`
}

// UsageSnapshot is the per-proxy usage of a range of hours.
//
// Hours from RetainedSince to Settled are final. Later hours may still grow.
// Hours before RetainedSince are no longer kept: their absence says nothing about usage.
type UsageSnapshot struct {
	Instance      string       `json:"instance"`
	StartedAt     time.Time    `json:"startedAt"`
	Hour          int64        `json:"hour"`
	Settled       int64        `json:"settled"`
	RetainedSince int64        `json:"retainedSince"`
	Overflow      bool         `json:"overflow"`
	Routes        bool         `json:"routes,omitempty"` // the entries are split by kind and route
	Entries       []UsageEntry `json:"entries"`
}

// Snapshot returns the non-zero buckets of hours from since to until, both inclusive. With
// routes, there is an entry per kind and route of each proxy; without, one per proxy, as
// clients that key what they keep by proxy expect.
func (u *Usage) Snapshot(since, until int64, routes bool) *UsageSnapshot {
	u.snapshotMu.RLock()
	snapshot := &UsageSnapshot{
		Instance:      u.instance,
		StartedAt:     u.startedAt,
		RetainedSince: u.retainedSince.Load(),
		Settled:       u.settled.Load(),
		Hour:          u.hour.Load(),
		Routes:        routes,
		Entries:       []UsageEntry{},
	}
	var keys []UsageKey
	for _, e := range u.allEntries() {
		buckets := usageBuckets(e.collect(since, until))
		if len(buckets) == 0 {
			continue
		}
		if u.isOverflow(e) {
			snapshot.Overflow = true
		}
		keys = append(keys, e.key)
		snapshot.Entries = append(snapshot.Entries, UsageEntry{
			Provider:   e.key.Provider,
			Name:       e.key.Name,
			Type:       e.key.Type,
			Addr:       e.key.Addr,
			Incomplete: e.key.Incomplete,
			Buckets:    buckets,
		})
	}
	u.snapshotMu.RUnlock()

	if routes {
		for i, key := range keys {
			entry := &snapshot.Entries[i]
			entry.Kind = "exit"
			if key.Relay {
				entry.Kind = "relay"
			}
			_ = json.Unmarshal([]byte(key.Route), &entry.UsageRoute)
		}
	} else {
		snapshot.Entries = mergeByProxy(snapshot.Entries)
	}
	slices.SortStableFunc(snapshot.Entries, func(a, b UsageEntry) int {
		for _, order := range [...]int{
			strings.Compare(a.Provider, b.Provider),
			strings.Compare(a.Name, b.Name),
			strings.Compare(a.Type, b.Type),
			strings.Compare(a.Addr, b.Addr),
			strings.Compare(a.Kind, b.Kind),
		} {
			if order != 0 {
				return order
			}
		}
		return 0
	})
	return snapshot
}

// mergeByProxy adds up the entries of the same proxy, hour by hour.
func mergeByProxy(entries []UsageEntry) []UsageEntry {
	type proxy struct {
		provider, name, adapterType, addr string
		incomplete                        bool
	}
	index := map[proxy]int{}
	merged := make([]UsageEntry, 0, len(entries)) // an empty list, not null
	for _, entry := range entries {
		id := proxy{entry.Provider, entry.Name, entry.Type, entry.Addr, entry.Incomplete}
		i, ok := index[id]
		if !ok {
			index[id] = len(merged)
			merged = append(merged, entry)
			continue
		}
		merged[i].Buckets = addBuckets(merged[i].Buckets, entry.Buckets)
	}
	return merged
}

func usageBuckets(counts []hourCount) []UsageBucket {
	var buckets []UsageBucket
	for _, c := range counts {
		if c.up != 0 || c.down != 0 {
			buckets = append(buckets, UsageBucket{Hour: c.hour, Up: c.up, Down: c.down})
		}
	}
	return buckets
}

func addBuckets(a, b []UsageBucket) []UsageBucket {
	sum := make([]UsageBucket, 0, len(a)+len(b))
	for len(a) > 0 || len(b) > 0 {
		switch {
		case len(b) == 0 || len(a) > 0 && a[0].Hour < b[0].Hour:
			sum, a = append(sum, a[0]), a[1:]
		case len(a) == 0 || b[0].Hour < a[0].Hour:
			sum, b = append(sum, b[0]), b[1:]
		default:
			sum = append(sum, UsageBucket{Hour: a[0].Hour, Up: a[0].Up + b[0].Up, Down: a[0].Down + b[0].Down})
			a, b = a[1:], b[1:]
		}
	}
	return sum
}

// usageCounter credits a tracker's traffic to the proxy that made its connection.
type usageCounter struct {
	entry    *usageEntry
	release  sync.Once
	activity *activity // renews the groups the connection was handed out by
}

func (c *usageCounter) add(up, down int64) {
	if c != nil {
		c.entry.add(up, down, 0)
		if down > 0 && c.activity != nil {
			c.activity.renew()
		}
	}
}

func (c *usageCounter) leave() {
	if c != nil {
		c.release.Do(func() {
			c.entry.table.release(c.entry)
		})
	}
}

// WithoutUsage keeps the tracker out of the per-proxy usage, for connections whose traffic
// is already counted on the connections they carry.
func WithoutUsage() TrackerOption {
	return func(info *TrackerInfo) {
		info.withoutUsage = true
	}
}

// joinUsage, once the options are applied, reports an underlying connection to the dial it
// was made for, along with the adapter that made it, and has the tracker's traffic counted.
func (info *TrackerInfo) joinUsage(conn any, upload, download int64) {
	adapter := adapterOfConn(conn)
	if info.recorder != nil {
		info.recorder.record(info, adapter)
	}
	if info.withoutUsage {
		return
	}
	info.usage = &usageCounter{entry: DefaultUsage.acquire(usageKeyOf(conn, info, adapter))}
	if !info.probe {
		info.usage.activity = activityOf(conn)
	}
	info.usage.add(upload, download)
}
