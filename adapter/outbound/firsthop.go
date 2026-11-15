package outbound

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
