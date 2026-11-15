package obfs

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/pool"
)

func TestHTTPObfsResponseHeaderInPieces(t *testing.T) {
	header := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	for _, testCase := range []struct {
		name    string
		writes  []string
		wantErr error
	}{
		{"whole", []string{header + "abcd"}, nil},
		{"split header", []string{header[:10], header[10:] + "abcd"}, nil},
		{"split delimiter", []string{header[:len(header)-3], header[len(header)-3:] + "abcd"}, nil},
		{"header before payload", []string{header, "abcd"}, nil},
		{"truncated header", []string{header[:10]}, io.EOF},
		{"header too long", []string{strings.Repeat("x", pool.RelayBufferSize)}, errHTTPResponseHeaderTooLong},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
			go func() {
				defer server.Close()
				for _, w := range testCase.writes {
					if _, err := io.WriteString(server, w); err != nil {
						return
					}
				}
			}()

			payload := make([]byte, 4)
			_, err := io.ReadFull(NewHTTPObfs(client, "example.com", "80"), payload)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("got error %v, want %v", err, testCase.wantErr)
			}
			if err == nil && string(payload) != "abcd" {
				t.Fatalf("got payload %q", payload)
			}
		})
	}
}
