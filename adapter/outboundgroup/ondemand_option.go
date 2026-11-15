package outboundgroup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// checkProbeOption validates the probe option of a group and reports whether the group tests
// its members on demand; such a group has its compatible provider skip periodic health
// checks and registers no test with the providers it uses.
func checkProbeOption(option *GroupCommonOption) bool {
	onDemand := checkProbeMode(option)
	if option.StrictOrder && !onDemand {
		log.Warnln("The group [%s] ignores strict-order, which only groups with probe: %s support", option.Name, ProbeOnDemand)
		option.StrictOrder = false
	}
	switch option.Order {
	case "", OrderConfig:
		option.Order = OrderConfig
	case OrderReliable, OrderReliableCost:
		switch {
		case !onDemand:
			log.Warnln("The group [%s] ignores order: %s, which only groups with probe: %s support", option.Name, option.Order, ProbeOnDemand)
			option.Order = OrderConfig
		case option.StrictOrder:
			log.Warnln("The group [%s] keeps its members in config order: strict-order excludes order: %s", option.Name, option.Order)
			option.Order = OrderConfig
		}
	default:
		log.Warnln("The group [%s] has an unknown order %q, kept in config order", option.Name, option.Order)
		option.Order = OrderConfig
	}
	if option.Order == OrderConfig && option.OrderTimely != 0 {
		log.Warnln("The group [%s] ignores order-timely because reliability ordering is disabled", option.Name)
		option.OrderTimely = 0
	}
	return onDemand
}

func effectiveOrderTimely(name string, timely int, timeout time.Duration) time.Duration {
	if timely == 0 {
		return probe.CompareTimely
	}
	if timely < 100 || int64(timely) >= timeout.Milliseconds() {
		log.Warnln("The group [%s] uses the default order-timely of %d ms because the configured value must be an integer of at least 100 ms and less than the test timeout of %d ms", name, probe.CompareTimely.Milliseconds(), timeout.Milliseconds())
		return probe.CompareTimely
	}
	return time.Duration(timely) * time.Millisecond
}

func checkProbeMode(option *GroupCommonOption) bool {
	switch option.Probe {
	case "":
		return false
	case ProbeOnDemand:
		if option.Type == "fallback" {
			option.Interval = 0
			return true
		}
		log.Warnln("The group [%s] of type %s ignores probe: %s, which only fallback groups support", option.Name, option.Type, option.Probe)
	default:
		log.Warnln("The group [%s] has an unknown probe option %q, ignored", option.Name, option.Probe)
	}
	option.Probe = ""
	return false
}

// OnDemand reports whether the group tests its members on demand and avoids excluded ones.
func (f *Fallback) OnDemand() bool {
	return f.od != nil
}

// GroupManualTest tests every member of a group as `/group/{name}/delay` asks, through the
// coordinator, so that the groups testing on demand under the same condition use the results.
// An url-test group tests with its own URL, as it always has.
func GroupManualTest(ctx context.Context, group ProxyGroup, url string, expectedStatus utils.IntRanges[uint16]) (map[string]uint16, error) {
	if u, ok := group.(*URLTest); ok {
		url = u.testUrl
	}
	cond := ManualCondition(ctx, url, expectedStatus)
	var (
		wg    sync.WaitGroup
		mutex sync.Mutex
	)
	delays := map[string]uint16{}
	for _, proxy := range group.Proxies() {
		proxy := proxy
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := probe.Default.Manual(ctx, proxy, cond, "group")
			if result.Satisfied {
				mutex.Lock()
				delays[proxy.Name()] = result.Delay
				mutex.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(delays) == 0 {
		return delays, fmt.Errorf("get delay: all proxies timeout")
	}
	return delays, nil
}

type manualTimeoutKey struct{}

// WithManualTimeout records on ctx the time a manual test asked for, which is part of its
// test condition; the deadline of ctx is not, as it shrinks while the test waits.
func WithManualTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, manualTimeoutKey{}, timeout)
}

// ManualCondition is the test condition of a manual test under ctx.
func ManualCondition(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) probe.Condition {
	timeout, _ := ctx.Value(manualTimeoutKey{}).(time.Duration)
	if timeout == 0 {
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline).Round(time.Millisecond)
		}
	}
	return probe.Condition{URL: url, Expected: expectedStatus.String(), Timeout: timeout}
}

// normalizeExpected writes a status requirement as a parsed one prints, so that the same
// requirement written in two ways is one test condition.
func normalizeExpected(expected string) string {
	ranges, err := utils.NewUnsignedRanges[uint16](expected)
	if err != nil {
		return expected
	}
	return ranges.String()
}

// ManualTest runs a manual test of proxy through the coordinator, so that groups testing on
// demand under the same condition use its result; api names the API that asked.
func ManualTest(ctx context.Context, proxy C.Proxy, cond probe.Condition, api string) (uint16, error) {
	result := probe.Default.Manual(ctx, proxy, cond, api)
	switch {
	case result.Satisfied:
		return result.Delay, nil
	case result.Error != "":
		return 0, fmt.Errorf("%s", result.Error)
	default:
		return 0, fmt.Errorf("unexpected status %d", result.Status)
	}
}
