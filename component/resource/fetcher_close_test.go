package resource

import (
	"context"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	P "github.com/metacubex/mihomo/constant/provider"
)

type memoryVehicle struct{ buf []byte }

func (v *memoryVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	return v.buf, utils.MakeHash(v.buf), nil
}
func (v *memoryVehicle) Write([]byte) error { return nil }
func (v *memoryVehicle) Path() string       { return "" }
func (v *memoryVehicle) Url() string        { return "" }
func (v *memoryVehicle) Proxy() string      { return "" }
func (v *memoryVehicle) Type() P.VehicleType {
	return P.Compatible
}

func TestClosedFetcherDoesNotPublish(t *testing.T) {
	parsed, published := 0, 0
	parser := func(buf []byte) (string, error) {
		parsed++
		return string(buf), nil
	}
	f := NewFetcher("test", 0, &memoryVehicle{buf: []byte("updated")}, nil, parser, func(string) { published++ })
	_ = f.Close()

	// an update queued or still reading when the fetcher was closed
	if _, _, err := f.Update(); err == nil {
		t.Fatal("closed fetcher accepted an update")
	}
	if _, _, err := f.SideUpdate([]byte("side")); err == nil {
		t.Fatal("closed fetcher accepted a side update")
	}
	if parsed != 0 || published != 0 {
		t.Fatalf("closed fetcher parsed %d and published %d updates", parsed, published)
	}
}
