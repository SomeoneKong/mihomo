package provider

import "sync/atomic"

var proxiesChanged atomic.Pointer[func()]

// SetProxiesChanged sets what is called, on its own goroutine, after a provider replaced its
// proxies.
func SetProxiesChanged(fn func()) {
	proxiesChanged.Store(&fn)
}

func notifyProxiesChanged() {
	if fn := proxiesChanged.Load(); fn != nil {
		go (*fn)()
	}
}
