package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

type recordedCall struct {
	hash  int32
	cmd   string
	param []byte
}

type fakeAgent struct {
	mu    sync.Mutex
	calls []recordedCall
	fail  bool
	block chan struct{}
}

func (f *fakeAgent) call(ctx context.Context, hash int32, cmd string, p *protocol.MapPack) ([]protocol.Pack, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	b, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedCall{hash, cmd, b})
	if f.fail {
		return nil, errors.New("agent down")
	}
	return []protocol.Pack{&protocol.MapPack{}}, nil
}

func (f *fakeAgent) snapshot() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedCall(nil), f.calls...)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRunAgentCallsBytesAndShutdown(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	agent := &fakeAgent{}
	e, _ := testEngineWith(t, &now, func(c *Config) { c.AgentCall = agent.call; c.ObjectType = javaeeType })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.RunAgentCalls(ctx) }()
	if err := e.Accept(ctx, kubePack("/k", kubeTags{pod: "p", host: "h", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	if err := e.Accept(ctx, cpuAgent("/was", "tomcat", true, decimal(0), decimal(-1))); err != nil {
		t.Fatal(err)
	}
	high := decimal(1)
	if err := e.Accept(ctx, cpuCounter("/was", now.UnixMilli(), &high)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(agent.snapshot()) == 2 })
	byCmd := map[string]recordedCall{}
	for _, c := range agent.snapshot() {
		byCmd[c.cmd] = c
	}
	kubeWant, _ := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{{Key: "seq", Value: decimal(0)}}}, protocol.Limits{})
	if c := byCmd["OBJECT_SET_KUBE_SEQ"]; c.hash != storage.CounterHash("/k") || !bytes.Equal(c.param, kubeWant) {
		t.Fatalf("kube call %+v", c)
	}
	if c := byCmd["TRIGGER_THREAD_DUMPS_FROM_CONDITIONS"]; c.hash != storage.CounterHash("/was") {
		t.Fatalf("dump call %+v", c)
	}
	if st := e.Stats(); st.KubeSeqCalls().Done != 1 || st.CPUDumpCalls().Done != 1 {
		t.Fatalf("done %+v", st)
	}
	cancel()
	<-done
	// After the worker stopped, offers are abandoned, not queued forever.
	if err := e.Accept(context.Background(), kubePack("/k2", kubeTags{pod: "p", host: "h2", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	if st := e.Stats().KubeSeqCalls(); st.Abandoned != 1 || st.Pending != 0 || st.Enqueued != 1 {
		t.Fatalf("after stop %+v", st)
	}
}

func TestRunAgentCallsFailuresAndClose(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	agent := &fakeAgent{fail: true, block: make(chan struct{})}
	e, _ := testEngineWith(t, &now, func(c *Config) { c.AgentCall = agent.call })
	done := make(chan struct{})
	go func() { defer close(done); e.RunAgentCalls(context.Background()) }()
	for i := 0; i < 3; i++ {
		if err := e.Accept(context.Background(), kubePack(fmt.Sprint("/k", i), kubeTags{pod: "p", host: fmt.Sprint("h", i), useKube: true, seq: -1})); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return e.Stats().KubeSeqCalls().Pending == 2 }) // one taken, blocked
	agent.block <- struct{}{}
	waitFor(t, func() bool { return e.Stats().KubeSeqCalls().Failed == 1 })
	// Close ends the worker and cancels the call blocked on the agent.
	waitFor(t, func() bool { return e.Stats().KubeSeqCalls().Pending == 1 })
	e.Close()
	<-done
	st := e.Stats().KubeSeqCalls()
	if st.Enqueued != 3 || st.Done+st.Failed+st.Abandoned != 3 || st.Pending != 0 {
		t.Fatalf("accounting %+v", st)
	}
}

func TestRunAgentCallsWithoutCaller(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.RunAgentCalls(ctx) }()
	if err := e.Accept(ctx, kubePack("/k", kubeTags{pod: "p", host: "h", useKube: true, seq: -1})); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return e.Stats().KubeSeqCalls().Failed == 1 })
	cancel()
	<-done
}

// When the worker's ctx is already done, a queued call that select happens to
// pick is not performed: it is abandoned like the rest of the queue, never
// called with a cancelled ctx and counted as failed.
func TestAgentCallQueueCancelledBeforeCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 200; i++ { // select picks a ready case at random
		q := newAgentCallQueue("test", 2)
		q.offer(agentCall{hash: 1, cmd: cmdSetKubeSeq})
		q.offer(agentCall{hash: 2, cmd: cmdSetKubeSeq})
		called := 0
		q.run(ctx, func(context.Context, int32, string, *protocol.MapPack) ([]protocol.Pack, error) {
			called++
			return nil, nil
		})
		if st := q.stats(); called != 0 || st.Abandoned != 2 || st.Failed != 0 || st.Done != 0 || st.Pending != 0 {
			t.Fatalf("iteration %d: called=%d %+v", i, called, st)
		}
	}
}

// Concurrent ingest, ticks and the worker under -race.
func TestKubeCPUConcurrent(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	agent := &fakeAgent{}
	var clockMu sync.Mutex
	clock := now
	e, _ := testEngineWith(t, &now, func(c *Config) {
		c.AgentCall = agent.call
		c.ObjectType = javaeeType
		c.Clock = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.RunAgentCalls(ctx)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				name := fmt.Sprintf("/o%d-%d", w, i%10)
				_ = e.Accept(ctx, kubePack(name, kubeTags{pod: "p", host: name, useKube: true, seq: int64(i % 3)}))
				_ = e.Accept(ctx, cpuAgent(name, "tomcat", true, decimal(0), decimal(-1)))
				high := decimal(1)
				_ = e.Accept(ctx, cpuCounter(name, now.UnixMilli()+int64(i), &high))
				if i%50 == 0 {
					clockMu.Lock()
					clock = clock.Add(5 * time.Second)
					clockMu.Unlock()
					_ = e.Tick(ctx)
					_ = e.RemoveInactiveObjects(ctx)
				}
				_ = e.Stats()
			}
		}(w)
	}
	wg.Wait()
	e.Close()
	st := e.Stats()
	for _, q := range []AgentCallStats{st.KubeSeqCalls(), st.CPUDumpCalls()} {
		if q.Enqueued != q.Done+q.Failed+q.Abandoned+uint64(q.Pending) {
			// Pending is 0 after Close; a call in flight at Close finishes as Done.
			waitFor(t, func() bool {
				s := e.Stats()
				return s.KubeSeqCalls().Enqueued == s.KubeSeqCalls().Done+s.KubeSeqCalls().Failed+s.KubeSeqCalls().Abandoned &&
					s.CPUDumpCalls().Enqueued == s.CPUDumpCalls().Done+s.CPUDumpCalls().Failed+s.CPUDumpCalls().Abandoned
			})
		}
	}
}
