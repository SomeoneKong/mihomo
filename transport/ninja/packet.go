package ninja

import (
	"bytes"
	"errors"
	"net"

	"github.com/metacubex/mihomo/common/pool"

	M "github.com/metacubex/sing/common/metadata"
)

// maxFrame holds the plaintext of any frame, which reads one whole datagram.
const maxFrame = 65535

type packetConn struct{ *ninjaConn }

func (c *Client) PacketConn(conn net.Conn, destination []byte) net.PacketConn {
	return &packetConn{c.newConn(conn, destination, 2)}
}

func (c *packetConn) WriteTo(data []byte, address net.Addr) (int, error) {
	n := len(data)
	var encoded bytes.Buffer
	destination := M.SocksaddrFromNet(address)
	if !destination.IsValid() {
		return 0, errors.New("invalid Ninja datagram destination")
	}
	if err := M.SocksaddrSerializer.WriteAddrPort(&encoded, destination); err != nil {
		return 0, err
	}
	if encoded.Len()+len(data) > 60000 {
		return 0, errors.New("Ninja datagram exceeds frame size")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writer == nil {
		c.addr = append([]byte(nil), encoded.Bytes()...)
	} else {
		data = append(encoded.Bytes(), data...)
	}
	_, err := c.writeLocked(data)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *packetConn) ReadFrom(output []byte) (int, net.Addr, error) {
	buffer := pool.Get(maxFrame)
	defer pool.Put(buffer)
	n, err := c.ninjaConn.Read(buffer)
	if err != nil {
		return 0, nil, err
	}
	reader := bytes.NewReader(buffer[:n])
	address, err := M.SocksaddrSerializer.ReadAddrPort(reader)
	if err != nil {
		return 0, nil, err
	}
	// The tunnel tells sources apart only by *net.UDPAddr.
	var source net.Addr = address
	if address.IsIP() {
		source = address.UDPAddr()
	}
	return copy(output, buffer[n-reader.Len():n]), source, nil
}
