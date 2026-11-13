package outbound

import (
	C "github.com/metacubex/mihomo/constant"
)

// OutboundAdapter reports the adapter that made the connection, so it can be told apart
// from the one a newer config made for a proxy of the same name.
func (c *conn) OutboundAdapter() C.ProxyAdapter {
	return c.adapter
}

// OutboundAdapter reports the adapter that made the connection, so it can be told apart
// from the one a newer config made for a proxy of the same name.
func (c *packetConn) OutboundAdapter() C.ProxyAdapter {
	return c.adapter
}

// Builtin reports whether the outbound is one a config makes for a name it reserves, as
// DIRECT. Each config makes them anew, and they do the same in each.
func (b *Base) Builtin() bool {
	return b.builtin
}

func markBuiltin[T interface{ markBuiltin() }](outbound T) T {
	outbound.markBuiltin()
	return outbound
}

func (b *Base) markBuiltin() {
	b.builtin = true
}

// WrappedAdapter returns the adapter this one closes automatically. A connection is
// made by the adapter wrapped, or by one wrapped in turn.
func (p *autoCloseProxyAdapter) WrappedAdapter() C.ProxyAdapter {
	return p.ProxyAdapter
}

// WrappedAdapter returns the adapter this one multiplexes over, which makes the
// connections that are not multiplexed.
func (s *SingMux) WrappedAdapter() C.ProxyAdapter {
	return s.ProxyAdapter
}
