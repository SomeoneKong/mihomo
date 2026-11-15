package adapter

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/queue"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/metacubex/http"
)

// ProbeURL tests the proxy against url once and reports what this test found. The delay
// histories are updated as URLTest updates them.
// implements C.ProbeTester
func (p *Proxy) ProbeURL(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) C.ProbeResult {
	_, result, _ := p.test(ctx, url, expectedStatus)
	return result
}

// test runs one URL test. It returns the measured delay and error as URLTest always has (a
// response with an unexpected status still has a delay and no error), and the result of the
// test on its own.
func (p *Proxy) test(ctx context.Context, url string, expectedStatus utils.IntRanges[uint16]) (t uint16, result C.ProbeResult, err error) {
	// Every URL test is a probe: groups do not count it as use.
	ctx = C.WithProbe(ctx)
	result.Started = time.Now()
	var (
		satisfied bool
		dialed    bool
	)

	defer func() {
		result.Elapsed = time.Since(result.Started)
		// A test canceled by its caller, such as the health check of a provider a new
		// config replaced, says nothing about the proxy, which the new config may still
		// use. A test that timed out is a failure as before.
		if err != nil && errors.Is(ctx.Err(), context.Canceled) {
			result.Canceled = true
			result.ErrorClass = C.ProbeErrorOther
			result.Error = err.Error()
			return
		}
		alive := err == nil
		record := C.DelayHistory{Time: time.Now()}
		if alive {
			record.Delay = t
		}

		p.alive.Store(alive)
		p.history.Put(record)
		if p.history.Len() > defaultHistoriesNum {
			p.history.Pop()
		}

		state, _ := p.extra.LoadOrStoreFn(url, func() *internalProxyState {
			return &internalProxyState{
				history: queue.New[C.DelayHistory](defaultHistoriesNum),
				alive:   atomic.NewBool(true),
			}
		})

		if !satisfied {
			record.Delay = 0
			alive = false
		}

		state.alive.Store(alive)
		state.history.Put(record)
		if state.history.Len() > defaultHistoriesNum {
			state.history.Pop()
		}

		switch {
		case err == nil && satisfied:
			result.Satisfied = true
			result.Delay = t
		case err == nil:
			result.ErrorClass = C.ProbeErrorStatus
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || isTimeout(err):
			result.ErrorClass = C.ProbeErrorTimeout
			result.Error = err.Error()
		case !dialed:
			result.ErrorClass = C.ProbeErrorDial
			result.Error = err.Error()
		default:
			result.ErrorClass = C.ProbeErrorOther
			result.Error = err.Error()
		}
	}()

	unifiedDelay := UnifiedDelay.Load()

	addr, err := urlToMetadata(url)
	if err != nil {
		return
	}

	start := time.Now()
	instance, err := p.DialContext(ctx, &addr)
	if err != nil {
		return
	}
	dialed = true
	result.Outbound = statistic.OutboundOf(instance)
	defer func() {
		_ = instance.Close()
	}()

	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return
	}
	req = req.WithContext(ctx)

	tlsConfig, err := ca.GetTLSConfig(ca.Option{})
	if err != nil {
		return
	}

	transport := &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return instance, nil
		},
		// from http.DefaultTransport
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
	}

	client := http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	defer client.CloseIdleConnections()

	resp, err := client.Do(req)

	if err != nil {
		return
	}

	_ = resp.Body.Close()

	if unifiedDelay {
		second := time.Now()
		var ignoredErr error
		var secondResp *http.Response
		secondResp, ignoredErr = client.Do(req)
		if ignoredErr == nil {
			resp = secondResp
			_ = resp.Body.Close()
			start = second
		} else if ctx.Err() != nil {
			// canceled or timed out: the test did not complete
			err = ignoredErr
			return
		} else {
			if strings.HasPrefix(url, "http://") {
				log.Errorln("%s failed to get the second response from %s: %v", p.Name(), url, ignoredErr)
				log.Warnln("It is recommended to use HTTPS for provider.health-check.url and group.url to ensure better reliability. Due to some proxy providers hijacking test addresses and not being compatible with repeated HEAD requests, using HTTP may result in failed tests.")
			}
		}
	}

	result.Status = resp.StatusCode
	satisfied = resp != nil && (expectedStatus == nil || expectedStatus.Check(uint16(resp.StatusCode)))
	t = uint16(time.Since(start) / time.Millisecond)
	if t == 0 {
		// a zero delay means a failed test to callers, so report a test
		// that completed in under a millisecond as 1 ms
		t = 1
	}
	return
}

// isTimeout reports a timeout of the network or of a step of the test with a limit of its
// own, such as the TLS handshake.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
