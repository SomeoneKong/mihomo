package outbound

import (
	"strings"

	C "github.com/metacubex/mihomo/constant"
	ovpn "github.com/metacubex/mihomo/transport/openvpn"
)

// IPVersion is the address family the node connects to its server over, or prefers, which a
// check of the local network connects over too.
func (b *Base) IPVersion() C.DNSPrefer {
	return b.prefer
}

// FirstHopTCP reports whether the node reaches its server over TCP, which a check of the local
// network connects to without the protocol.
func (v *Vmess) FirstHopTCP() bool {
	switch v.option.Network {
	case "kcp", "mkcp":
		return false
	}
	return true
}

// FirstHopTCP reports whether the node reaches its server over TCP: kcptun runs over UDP.
func (ss *ShadowSocks) FirstHopTCP() bool {
	return ss.kcptunClient == nil
}

// FirstHopTCP reports whether the node reaches its server over TCP: XHTTP may run over QUIC.
func (v *Vless) FirstHopTCP() bool {
	return v.option.Network != "xhttp"
}

// FirstHopTCP reports whether the node reaches its server over TCP: HTTP/2, unless set to QUIC.
func (t *TrustTunnel) FirstHopTCP() bool {
	return !t.option.Quic
}

// FirstHopTCP reports whether the node reaches its server over TCP, as its config sets.
func (o *OpenVPN) FirstHopTCP() bool {
	return o.config != nil && o.config.Proto == ovpn.ProtoTCP
}

// FirstHopTCP reports whether the node reaches its server over TCP, at the one port it has.
func (m *Mieru) FirstHopTCP() bool {
	if !strings.EqualFold(m.option.Transport, "TCP") {
		return false
	}
	if m.option.PortRange == "" {
		return true
	}
	begin, end, err := beginAndEndPortFromPortRange(m.option.PortRange)
	return err == nil && begin == end
}

// FirstHopTCP reports whether the node reaches its server over TCP: over HTTP/2, not HTTP/3.
func (w *Masque) FirstHopTCP() bool {
	return w.h2Transport != nil
}
