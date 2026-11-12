package statistic

import (
	"context"
	"slices"
	"sync"

	C "github.com/metacubex/mihomo/constant"
)

// DialedLayer is the route an underlying connection took, as its chains name it.
type DialedLayer struct {
	Chain         C.Chain
	ProviderChain C.Chain
}

// DialerRecorder collects the underlying connections proxy dialers make under a context.
type DialerRecorder struct {
	mutex  sync.Mutex
	layers []DialedLayer
}

type dialerRecorderKey struct{}

// WithDialerRecorder returns a context under which proxy dialers report the underlying
// connections they make to the returned recorder.
func WithDialerRecorder(ctx context.Context) (context.Context, *DialerRecorder) {
	recorder := &DialerRecorder{}
	return context.WithValue(ctx, dialerRecorderKey{}, recorder), recorder
}

// Layers returns the underlying connections recorded so far, the deepest first.
func (r *DialerRecorder) Layers() []DialedLayer {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return slices.Clone(r.layers)
}

func (r *DialerRecorder) record(info *TrackerInfo) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.layers = append(r.layers, DialedLayer{Chain: info.Chain, ProviderChain: info.ProviderChain})
}
