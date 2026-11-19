package server

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/transport"
)

// With the production defaults (Client progress limit RequestTimeout
// 30s, Agent so_timeout 60s): a Blob stream relay waits for each Agent chunk
// as long as the Agent so_timeout allows, like Java's
// HeapDumpService.downloadHepDump handler (HeapDumpService.scala:58-71,
// TcpAgentWorker.scala:69-77), instead of ending at the Client progress limit.

// The first chunk comes 35s after the request, past the 30s Client progress
// limit and inside the Agent so_timeout: the Client gets the whole stream and
// the Agent session goes back to the pool.
func TestE2EHeapDumpFirstChunkAfterClientProgressLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("35s production-timing reproduction")
	}
	t.Parallel()
	const hash = 4260
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_DOWNLOAD_HEAP_DUMP", client.session, e2eObjHashParam(t, hash))
	client.conn.SetDeadline(time.Now().Add(2 * time.Minute))
	started := time.Now()
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	ended := make(chan error, 1)
	go func() {
		_, err := client.r.Peek(1)
		ended <- err
	}()
	time.Sleep(35*time.Second - time.Since(started))
	select {
	case err := <-ended:
		t.Fatalf("Client command ended before the first chunk: %v", err)
	default:
	}
	chunks := [][]byte{chunkData(1, 2<<20), chunkData(2, 4096)}
	for _, c := range chunks {
		if _, err := agent.conn.Write(encodeBlobItem(t, c)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := agent.conn.Write([]byte{transport.NoNext}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Client did not get the first chunk")
	}
	for i, want := range chunks {
		if flag, err := client.r.ReadByte(); err != nil || flag != transport.HasNext {
			t.Fatalf("chunk %d flag %x %v", i, flag, err)
		}
		if got := e2eReadBlob(t, client.r); !bytes.Equal(got, want) {
			t.Fatalf("chunk %d differs", i)
		}
	}
	if flag, err := client.r.ReadByte(); err != nil || flag != transport.NoNext {
		t.Fatalf("terminator %x %v", flag, err)
	}
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 1 && !list[0].busy && !list[0].conn.Closed() }, hash)
}

// An Agent slower than its so_timeout (35s here, past the 30s Client progress
// limit) ends the stream like Java's read timeout: the Agent session is
// closed. The Client keeps the chunks already sent and, unlike Java (which
// swallows the handler error and writes NoNEXT), gets no NoNEXT (intended
// difference, TestAgentReadPartialBlobBeforeUpstreamFailure).
func TestE2EHeapDumpPastAgentSoTimeoutClosesSession(t *testing.T) {
	if testing.Short() {
		t.Skip("35s production-timing reproduction")
	}
	t.Parallel()
	const hash = 4261
	r, addr := e2eRuntime(t)
	if err := r.tcp.SetAgentSoTimeout(35 * time.Second); err != nil {
		t.Fatal(err)
	}
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_DOWNLOAD_HEAP_DUMP", client.session, e2eObjHashParam(t, hash))
	client.conn.SetDeadline(time.Now().Add(2 * time.Minute))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	first := chunkData(3, 4096)
	if _, err := agent.conn.Write(encodeBlobItem(t, first)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if flag, err := client.r.ReadByte(); err != nil || flag != transport.HasNext {
		t.Fatalf("first chunk flag %x %v", flag, err)
	}
	if got := e2eReadBlob(t, client.r); !bytes.Equal(got, first) {
		t.Fatal("first chunk differs")
	}
	_, err := client.r.ReadByte()
	after := time.Since(started)
	if !errors.Is(err, io.EOF) || after < 34*time.Second || after > 45*time.Second {
		t.Fatalf("Client end after %v: %v", after, err)
	}
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, hash)
}

// A Client that closes its connection while the relay waits for the first
// chunk ends the stream at once and closes the Agent session (Blob streams are
// not orphaned: the rest of an unbounded stream must not be read for nobody).
func TestE2EHeapDumpClientLeavesClosesAgentSession(t *testing.T) {
	const hash = 4262
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, hash)
	client := moLogin(t, addr)
	moSend(t, client.conn, "OBJECT_DOWNLOAD_HEAP_DUMP", client.session, e2eObjHashParam(t, hash))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	left := time.Now()
	client.conn.Close()
	agent.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := agent.reader.ReadByte(); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("Client exit not noticed while waiting for the first chunk")
	} else if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if elapsed := time.Since(left); elapsed > time.Second {
		t.Fatalf("Agent session closed %v after the Client left", elapsed)
	}
	h := &poolTCPHarness{pool: &r.pool}
	h.wait(t, func(list []*agentSlot) bool { return len(list) == 0 }, hash)
	if s := r.pool.orphans.snapshot(); s.Orphaned != 0 {
		t.Fatalf("orphan stats %+v", s)
	}
	// The relay's chunk permit (agentBlobChunkBytes) is returned as well.
	for s := r.pool.budget.snapshot(); s.Bytes != 0 || s.Granted == 0; s = r.pool.budget.snapshot() {
		if time.Since(left) > 2*time.Second {
			t.Fatalf("relay budget after the Client left %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
