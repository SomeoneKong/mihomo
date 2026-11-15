package outbound

import "net"

// listenPacketAvoiding listens on a port other than port, the source port of the association.
// A reply written back to a local source that has gone is delivered by the system to the
// socket left on that port: on the same one, the association would relay it to itself again
// and again.
func listenPacketAvoiding(listen func() (net.PacketConn, error), port uint16) (net.PacketConn, error) {
	pc, err := listen()
	if err != nil {
		return nil, err
	}
	if addr, ok := pc.LocalAddr().(*net.UDPAddr); !ok || addr.Port != int(port) {
		return pc, nil
	}
	// listen again while the port is held, so that it is not given out again
	defer pc.Close()
	return listen()
}
