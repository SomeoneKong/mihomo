package process

import (
	"errors"
	"net/netip"

	"github.com/metacubex/mihomo/component/iface"
	C "github.com/metacubex/mihomo/constant"
)

var (
	ErrInvalidNetwork     = errors.New("invalid network")
	ErrPlatformNotSupport = errors.New("not support on this platform")
	ErrNotFound           = errors.New("process not found")
)

const (
	TCP = "tcp"
	UDP = "udp"
)

func FindProcessName(network string, srcIP netip.Addr, srcPort int) (uint32, string, error) {
	if !isLocalSource(srcIP) {
		return 0, "", ErrNotFound
	}
	return findProcessName(network, srcIP, srcPort)
}

// isLocalSource reports whether srcIP may belong to a socket on this host. A
// connection from another host, e.g. forwarded for a LAN device, has none, and
// the lookups, which fall back to matching the port alone for sockets bound to
// a wildcard address, would attribute it to whatever local socket uses the
// same port.
func isLocalSource(srcIP netip.Addr) bool {
	srcIP = srcIP.Unmap().WithZone("")
	if !srcIP.IsValid() || srcIP.IsLoopback() {
		return true
	}
	local, err := iface.IsLocalIp(srcIP)
	return err != nil || local
}

// PackageNameResolver
// never change type traits because it's used in CMFA
type PackageNameResolver func(metadata *C.Metadata) (string, error)

// DefaultPackageNameResolver
// never change type traits because it's used in CMFA
var DefaultPackageNameResolver PackageNameResolver

func FindPackageName(metadata *C.Metadata) (string, error) {
	if resolver := DefaultPackageNameResolver; resolver != nil {
		return resolver(metadata)
	}
	return "", ErrPlatformNotSupport
}
