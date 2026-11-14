package outboundgroup

import (
	"context"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

const (
	pinRetestFirst = 15 * time.Second
	pinRetestMax   = 5 * time.Minute
)

// pinRetest tests a pinned proxy again once the group is used while the proxy tests dead,
// so that the group goes back to it soon after a test it failed by chance. While the proxy
// stays dead, the tests back off.
type pinRetest struct {
	mutex   sync.Mutex
	name    string // the proxy the backoff is for
	next    time.Time
	backoff time.Duration
	running bool
}

// pinnedAlive returns the pinned proxy if it tests alive. A pinned proxy that tests dead is
// kept pinned and left out until it tests alive again; when the group is in use, it is
// tested again in the background.
func (f *Fallback) pinnedAlive(proxies []C.Proxy, touch bool) C.Proxy {
	selected := f.selected
	if selected == "" {
		return nil
	}
	for _, proxy := range proxies {
		if proxy.Name() != selected {
			continue
		}
		if proxy.AliveForTestUrl(f.testUrl) {
			f.pinRetest.reset(selected)
			return proxy
		}
		if touch {
			f.pinRetest.start(proxy, f.retestPinned)
		}
		return nil
	}
	return nil
}

func (f *Fallback) retestPinned(proxy C.Proxy) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(f.testTimeout)*time.Millisecond)
	defer cancel()
	expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
	_, _ = proxy.URLTest(ctx, f.testUrl, expectedStatus)
	return proxy.AliveForTestUrl(f.testUrl)
}

func (r *pinRetest) reset(name string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.name == name {
		r.next = time.Time{}
		r.backoff = 0
	}
}

func (r *pinRetest) start(proxy C.Proxy, test func(C.Proxy) bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.name != proxy.Name() {
		r.name = proxy.Name()
		r.next = time.Time{}
		r.backoff = 0
	}
	if r.running || time.Now().Before(r.next) {
		return
	}
	r.running = true
	go func() {
		alive := test(proxy)
		r.mutex.Lock()
		defer r.mutex.Unlock()
		r.running = false
		if r.name != proxy.Name() {
			return
		}
		if alive {
			r.next = time.Time{}
			r.backoff = 0
			return
		}
		switch {
		case r.backoff < pinRetestFirst:
			r.backoff = pinRetestFirst
		case r.backoff*2 > pinRetestMax:
			r.backoff = pinRetestMax
		default:
			r.backoff *= 2
		}
		r.next = time.Now().Add(r.backoff)
	}()
}
