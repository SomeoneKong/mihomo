package outbound

import (
	C "github.com/metacubex/mihomo/constant"
)

// OutboundIdentity reports the type and address of the adapter that made the connection,
// fixed when the connection was built, so usage is credited to the proxy actually used
// even if the configuration has changed since.
func (c *conn) OutboundIdentity() (C.AdapterType, string) {
	return c.adapterType, c.adapterAddr
}

// OutboundIdentity reports the type and address of the adapter that made the connection,
// fixed when the connection was built, so usage is credited to the proxy actually used
// even if the configuration has changed since.
func (c *packetConn) OutboundIdentity() (C.AdapterType, string) {
	return c.adapterType, c.adapterAddr
}
