package transport

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// flakyListener fails Accept with EMFILE a few times before passing through.
type flakyListener struct {
	net.Listener
	failures atomic.Int32
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if l.failures.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", syscall.EMFILE)}
	}
	return l.Listener.Accept()
}

// Running out of descriptors pauses accepting instead of stopping the TCP
// server: connected Agents keep their sessions and new ones get in later.
func TestTCPAcceptRetriesTemporaryErrors(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &flakyListener{Listener: inner}
	listener.failures.Store(3)
	agents := make(chan *AgentConn, 1)
	var events atomic.Int32
	server, err := NewTCPServer(listener, TCPConfig{}, budgetHandlers(agents), func(ErrorEvent) { events.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	conn, err := net.Dial("tcp", inner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(agentHeader(TCPAgentV2, 77)); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-agents:
		if a.ObjectHash != 77 {
			t.Fatal(a.ObjectHash)
		}
	case err := <-done:
		t.Fatal("Serve stopped on a temporary Accept error:", err)
	case <-time.After(5 * time.Second):
		t.Fatal("agent not accepted after the Accept errors")
	}
	if st := server.Stats(); st.AcceptRetries != 3 {
		t.Fatal("AcceptRetries", st.AcceptRetries)
	}
	if n := events.Load(); n != 1 {
		t.Fatal("accept retry log lines", n, "(rate-limited to one)")
	}
	cancel()
	server.Close()
	<-done
}
