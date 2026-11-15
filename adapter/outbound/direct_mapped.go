package outbound

import (
	"errors"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
)

// mappedHostUnresolvable reports whether a direct dial failed only because the
// host has no address. A host restored from the DNS mapping is just the name an
// earlier answer gave for the IP the client dialed, and the mapping keeps it
// after the record is gone or when the direct nameservers do not know it, so
// the IP itself is still the destination to connect to.
func mappedHostUnresolvable(metadata *C.Metadata, err error) bool {
	return metadata.DNSMode == C.DNSMapping && metadata.Host != "" && metadata.DstIP.IsValid() &&
		(errors.Is(err, resolver.ErrIPNotFound) || errors.Is(err, resolver.ErrIPVersion) || errors.Is(err, dialer.ErrorNoIpAddress))
}
