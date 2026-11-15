package statistic

import (
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// Instance returns the identity of this process's usage, which other per-process records
// share so that readers can tell which process they came from.
func (u *Usage) Instance() (string, time.Time) {
	return u.instance, u.startedAt
}

// OutboundOf returns the outbound that made a connection, nil when unknown.
func OutboundOf(conn any) C.ProxyAdapter {
	return adapterOfConn(conn)
}

// DialerRoute rebuilds, layer by layer, the dialer proxies a dial made by adapter under the
// recorder reached its server through, as the usage of a connection records them.
func DialerRoute(adapter C.ProxyAdapter, recorder *DialerRecorder) [][]string {
	if recorder == nil {
		return nil
	}
	return usageDialers(adapter, recorder.Layers())
}

// activity renews, while a connection receives data, the groups whose layers it was handed
// out by. Renewals are coarse: at most one a second.
type activity struct {
	bindings  []C.ActivityBinding
	renewedAt atomic.Int64
}

// activityOf collects the bindings of every layer of conn, outermost first.
func activityOf(conn any) *activity {
	var bindings []C.ActivityBinding
	N.FindUpstream(conn, func(binder C.ActivityBinder) bool {
		if binding := binder.ActivityBinding(); binding != nil {
			bindings = append(bindings, binding)
		}
		return false // keep walking to the innermost layer
	})
	if len(bindings) == 0 {
		return nil
	}
	return &activity{bindings: bindings}
}

func (a *activity) renew() {
	now := time.Now().Unix()
	last := a.renewedAt.Load()
	if now == last || !a.renewedAt.CompareAndSwap(last, now) {
		return
	}
	for _, binding := range a.bindings {
		binding.Renew()
	}
}
