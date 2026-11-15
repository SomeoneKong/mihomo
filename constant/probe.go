package constant

import (
	"context"
	"time"

	"github.com/metacubex/mihomo/common/utils"
)

type probeContextKey struct{}

// WithProbe marks ctx as carrying a probe rather than real traffic. Groups do not count what a
// probe dials or receives as use, and the mark follows the context into dialer proxies.
func WithProbe(ctx context.Context) context.Context {
	if IsProbe(ctx) {
		return ctx
	}
	return context.WithValue(ctx, probeContextKey{}, true)
}

// IsProbe reports whether ctx carries a probe.
func IsProbe(ctx context.Context) bool {
	probe, _ := ctx.Value(probeContextKey{}).(bool)
	return probe
}

// Classes of failed probes.
const (
	ProbeErrorDial    = "dial"
	ProbeErrorTimeout = "timeout"
	ProbeErrorStatus  = "status"
	ProbeErrorOther   = "other"
)

// ProbeResult is what one URL test found, apart from the state it shares with other tests.
type ProbeResult struct {
	Started time.Time
	Elapsed time.Duration
	// Delay is the measured delay in milliseconds of a satisfied test.
	Delay uint16
	// Status is the HTTP status the test got, 0 without a response.
	Status int
	// Satisfied is set when the test got a response with an expected status.
	Satisfied bool
	// Canceled is set when the caller canceled the test; such a test says nothing about the proxy.
	Canceled bool
	// ErrorClass is one of the ProbeError classes for an unsatisfied test.
	ErrorClass string
	Error      string
	// Outbound is the outbound the test's connection was made by, as a group passes the dial
	// on to one of its members; nil when the dial failed.
	Outbound ProxyAdapter
}

// ProbeTester is implemented by proxies that report the result of one URL test.
type ProbeTester interface {
	ProbeURL(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) ProbeResult
}

// TrafficPoolOwner is implemented by proxies that tell which traffic pool, a set of nodes
// sharing one traffic quota, they belong to.
type TrafficPoolOwner interface {
	TrafficPool() string
}

// ActivityBinding is held by a connection a group handed out; renewing it tells the group that
// the connection still carries data.
type ActivityBinding interface {
	Renew()
}

// ActivityBinder is implemented by connections that carry activity bindings of their own layer.
type ActivityBinder interface {
	ActivityBinding() ActivityBinding
}
