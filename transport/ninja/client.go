package ninja

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/metacubex/blake3"
)

const pskContext = "de6dbaddc4e0dbaaeb65778b44e6fdac"
const subkeyContext = "52386811679ea908c4a2b02f367c472c"
const aesContext = "e14d197ae519bdd2d9a75ae94df7e208"

// Options describes the current Ninja AEAD profile and its optional pass layer.
type Options struct {
	Method       string
	Password     string
	NodePassword string
	Pass         bool
	PassOpts     PassOptions
}

type PassOptions struct {
	Method      string `proxy:"method,omitempty"`
	Password    string `proxy:"password"`
	PaddingMode string `proxy:"padding_mode,omitempty"`
	PaddingMin  int    `proxy:"padding_min,omitempty"`
	PaddingMax  int    `proxy:"padding_random_max,omitempty"`
}

type Client struct{ option Options }

func NewClient(option Options) (*Client, error) {
	if option.Method != "aes-128-gcm" {
		return nil, fmt.Errorf("unsupported Ninja method %q", option.Method)
	}
	if option.Password == "" || option.NodePassword == "" {
		return nil, errors.New("Ninja requires password and node_password")
	}
	if option.Pass {
		if option.PassOpts.Method == "" {
			option.PassOpts.Method = "aes-128-gcm"
		}
		if option.PassOpts.Method != "aes-128-gcm" {
			return nil, fmt.Errorf("unsupported Ninja pass method %q", option.PassOpts.Method)
		}
		if option.PassOpts.Password == "" {
			return nil, errors.New("Ninja pass requires password")
		}
		if option.PassOpts.PaddingMode != "" && option.PassOpts.PaddingMode != "min_random" {
			return nil, fmt.Errorf("unsupported Ninja padding mode %q", option.PassOpts.PaddingMode)
		}
		if option.PassOpts.PaddingMin < 0 || option.PassOpts.PaddingMax < 0 || option.PassOpts.PaddingMin > 65535-option.PassOpts.PaddingMax {
			return nil, errors.New("invalid Ninja pass padding bounds")
		}
	}
	return &Client{option: option}, nil
}

func (c *Client) StreamConn(conn net.Conn, destination []byte) (net.Conn, error) {
	// Send the destination before application data so server-first protocols work.
	wrapped := c.newConn(conn, destination, 1)
	if _, err := wrapped.Write(nil); err != nil {
		return nil, err
	}
	return wrapped, nil
}
func (c *Client) newConn(conn net.Conn, destination []byte, command byte) *ninjaConn {
	if c.option.Pass {
		conn = newPass(conn, c.option)
	}
	return &ninjaConn{Conn: conn, node: c.option, addr: append([]byte(nil), destination...), command: command, passPSK: derive(pskContext, []byte(c.option.Password)), salt: random(16)}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func derive(context string, material []byte) []byte {
	out := make([]byte, 16)
	blake3.DeriveKey(out, context, material)
	return out
}

func aead(key []byte) cipher.AEAD {
	b, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		panic(err)
	}
	return g
}

func random(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}
func randomN(n int) int {
	if n <= 0 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		panic(err)
	}
	return int(v.Int64())
}
func increment(n []byte) {
	for i := range n {
		n[i]++
		if n[i] != 0 {
			return
		}
	}
}
func readN(r io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

// readRest reads the remainder of a frame that has begun, so even an immediate EOF cuts it short.
func readRest(r io.Reader, n int) ([]byte, error) {
	b, err := readN(r, n)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return b, err
}

// readError names the part a read failed on. A clean EOF between frames stays bare, since
// readers of the stream compare it by identity.
func readError(part string, err error) error {
	if err == io.EOF {
		return err
	}
	return fmt.Errorf("%s: %w", part, err)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

type ninjaConn struct {
	net.Conn
	node                  Options
	addr                  []byte
	command               byte
	readMu, writeMu       sync.Mutex
	passPSK, salt         []byte
	writer, reader        cipher.AEAD
	writeNonce, readNonce []byte
	pending               []byte
	// A failed read or write leaves the frame state of its direction unknown; the error is kept
	// and returned from then on.
	readErr, writeErr error
}

func (c *ninjaConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(b)
}
func (c *ninjaConn) writeLocked(b []byte) (written int, err error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	defer func() {
		if err != nil {
			c.writeErr = err
		}
	}()
	if len(b) == 0 && c.writer != nil {
		return 0, nil
	}
	var packet []byte
	if c.writer == nil {
		nodePSK := derive(pskContext, []byte(c.node.NodePassword))
		check := blake3.Sum512(c.passPSK)
		block, _ := aes.NewCipher(derive(aesContext, append(nodePSK, c.salt...)))
		auth := make([]byte, 16)
		block.Encrypt(auth, check[:16])
		c.writer = aead(derive(subkeyContext, append(append([]byte(nil), c.passPSK...), c.salt...)))
		c.writeNonce = make([]byte, 12)
		count := minInt(len(b), 60000)
		pad := randomN(900) + 1
		payload := append(append([]byte(nil), c.addr...), byte(pad>>8), byte(pad))
		payload = append(payload, random(pad)...)
		payload = append(payload, b[:count]...)
		// The first header and payload have independent AEAD tags and nonces.
		header := []byte{0, c.command}
		header = binary.BigEndian.AppendUint64(header, uint64(time.Now().UnixMilli()))
		header = binary.BigEndian.AppendUint16(header, uint16(len(payload)))
		packet = append(append(append([]byte(nil), c.salt...), auth...), c.writer.Seal(nil, c.writeNonce, header, nil)...)
		increment(c.writeNonce)
		packet = append(packet, c.writer.Seal(nil, c.writeNonce, payload, nil)...)
		increment(c.writeNonce)
		if e := writeAll(c.Conn, packet); e != nil {
			return 0, e
		}
		packet = nil
		written += count
		b = b[count:]
	}
	for len(b) > 0 {
		count := minInt(len(b), 60000)
		pad := randomN(900) + 1
		header := binary.BigEndian.AppendUint16(nil, uint16(count+pad))
		header = binary.BigEndian.AppendUint16(header, uint16(pad))
		packet = append(packet, c.writer.Seal(nil, c.writeNonce, header, nil)...)
		increment(c.writeNonce)
		payload := append(append([]byte(nil), b[:count]...), random(pad)...)
		packet = append(packet, c.writer.Seal(nil, c.writeNonce, payload, nil)...)
		increment(c.writeNonce)
		if err := writeAll(c.Conn, packet); err != nil {
			return written, err
		}
		packet = nil
		written += count
		b = b[count:]
	}
	return written, nil
}
func (c *ninjaConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		if c.readErr != nil {
			return 0, c.readErr
		}
		if c.readErr = c.readFrame(); c.readErr != nil {
			return 0, c.readErr
		}
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *ninjaConn) readFrame() error {
	var size, pad int
	if c.reader == nil {
		salt, e := readN(c.Conn, 16)
		if e != nil {
			return readError("ninja salt", e)
		}
		c.reader = aead(derive(subkeyContext, append(append([]byte(nil), c.passPSK...), salt...)))
		c.readNonce = make([]byte, 12)
		head, e := readRest(c.Conn, 45)
		if e != nil {
			return e
		}
		head, e = c.reader.Open(nil, c.readNonce, head, nil)
		if e != nil {
			return fmt.Errorf("ninja first header AEAD: %w", e)
		}
		increment(c.readNonce)
		validSalt := bytes.Equal(head[9:25], c.salt)
		if head[0] != 1 || !validSalt {
			return errors.New("ninja invalid response header or echoed salt")
		}
		size = int(binary.BigEndian.Uint16(head[25:27]))
		pad = int(binary.BigEndian.Uint16(head[27:29]))
	} else {
		head, e := readN(c.Conn, 20)
		if e != nil {
			return e
		}
		head, e = c.reader.Open(nil, c.readNonce, head, nil)
		if e != nil {
			return fmt.Errorf("ninja header AEAD: %w", e)
		}
		increment(c.readNonce)
		size = int(binary.BigEndian.Uint16(head))
		pad = int(binary.BigEndian.Uint16(head[2:]))
	}
	if pad > size {
		return errors.New("ninja invalid padding length")
	}
	data, e := readRest(c.Conn, size+16)
	if e != nil {
		return e
	}
	data, e = c.reader.Open(nil, c.readNonce, data, nil)
	if e != nil {
		return fmt.Errorf("ninja payload AEAD: %w", e)
	}
	increment(c.readNonce)
	c.pending = data[:size-pad]
	return nil
}
