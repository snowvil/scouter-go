package transport

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
)

// A stack body has no frame marker: only its declared Size separates it from
// the next command. Both commands are deliberately sent in one socket write.
func TestTCPStackPipeliningAndTruncatedBody(t *testing.T) {
	raw := fixture(t, "tcp-stack")
	for _, truncated := range []bool{false, true} {
		name := "pipelined"
		if truncated {
			name = "truncated"
		}
		t.Run(name, func(t *testing.T) {
			bodies := make(chan []byte, 2)
			server, listener, cancel, done := startTCP(t, TCPConfig{}, TCPHandlers{
				Stack: func(ctx context.Context, meta StackUpload, body io.Reader) error {
					data, err := io.ReadAll(body)
					bodies <- data
					return err
				},
			})
			defer cancel()
			conn := dialTCP(t, listener)
			input := append(bytes.Clone(raw), raw[8:]...) // only the first upload carries handshake+objHash
			if truncated {
				input = raw[:len(raw)-1]
			}
			if _, err := conn.Write(input); err != nil {
				t.Fatal(err)
			}
			if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			ack, err := io.ReadAll(conn)
			if err != nil {
				t.Fatal(err)
			}
			stopTCP(t, cancel, done)
			if truncated {
				if len(ack) != 0 || server.Stats().StackUploads != 0 || server.Stats().Errors != 1 {
					t.Fatalf("partial upload acknowledged: %x %+v", ack, server.Stats())
				}
				return
			}
			if !bytes.Equal(ack, []byte{OK, OK}) || server.Stats().StackUploads != 2 {
				t.Fatalf("pipeline acknowledgements: %x %+v", ack, server.Stats())
			}
			for i := 0; i < 2; i++ {
				select {
				case body := <-bodies:
					if !bytes.Equal(body, []byte{'P', 'K', 3, 4, 0, 1, 2, 3}) {
						t.Fatal("stack body consumed next command", body)
					}
				default:
					t.Fatal("missing upload body")
				}
			}
		})
	}
}
