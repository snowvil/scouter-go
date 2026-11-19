package sqltable

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type added struct {
	date, textType string
	hash           int32
	text           string
}

type fakeSink struct {
	mu      sync.Mutex
	calls   []added
	entered chan int32    // optional: signalled on each call
	release chan struct{} // optional: each call waits for a value or ctx end
	err     error
}

func (s *fakeSink) AddText(ctx context.Context, date, textType string, hash int32, text string) error {
	if s.entered != nil {
		s.entered <- hash
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, added{date, textType, hash, text})
	return s.err
}

func (s *fakeSink) snapshot() []added {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]added(nil), s.calls...)
}

type fakeSource struct {
	mu    sync.Mutex
	texts map[int32]string
	errs  map[int32]error
	calls []string
}

func (s *fakeSource) Text(_ context.Context, date, textType string, hash int32) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, date+"/"+textType)
	if err := s.errs[hash]; err != nil {
		return "", false, err
	}
	v, ok := s.texts[hash]
	return v, ok, nil
}

type logLine struct {
	id  string
	sec int
	msg string
}

type fakeLog struct {
	mu    sync.Mutex
	lines []logLine
}

func (l *fakeLog) log(id string, sec int, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, logLine{id, sec, msg})
}

func (l *fakeLog) snapshot() []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logLine(nil), l.lines...)
}

func always(v bool) func() bool { return func() bool { return v } }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewRequiresOptions(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ErrOptions) {
		t.Fatalf("err=%v", err)
	}
}

func TestAddReadsEnabledEveryCall(t *testing.T) {
	var enabled atomic.Bool
	sink := &fakeSink{}
	tb, err := New(Options{Sink: sink, Source: &fakeSource{}, Enabled: enabled.Load})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close(context.Background())
	tb.Add("20261015", 1, "SELECT a FROM t1")
	enabled.Store(true)
	tb.Add("20261015", 2, "SELECT a FROM t2")
	waitFor(t, "stored", func() bool { return tb.Stats().Stored == 1 })
	st := tb.Stats()
	if st.Disabled != 1 || st.Enqueued1 != 1 {
		t.Fatalf("stats %+v", st)
	}
	if got := sink.snapshot(); len(got) != 1 || got[0] != (added{"20261015", "table", 2, "t2(R)"}) {
		t.Fatalf("sink %+v", got)
	}
}

func TestProcessDedupAndDates(t *testing.T) {
	sink := &fakeSink{}
	tb, err := newTables(Options{Sink: sink, Source: &fakeSource{}, Enabled: always(true)})
	if err != nil {
		t.Fatal(err)
	}
	in := func(date string, hash int32) item {
		return item{date: date, hash: hash, sql: "DELETE FROM t9", hasText: true}
	}
	tb.process(in("20261014", 7))
	tb.process(in("20261014", 7)) // already parsed for that date
	tb.process(in("20261015", 7)) // per-date set: parsed again
	tb.process(in("20261016", 7)) // third date evicts 20261014 (setMax(2), oldest inserted)
	tb.process(in("20261014", 7)) // new set for 20261014 evicts 20261015
	tb.process(in("20261016", 7))
	tb.process(in("20261015", 7))
	st := tb.Stats()
	if st.Stored != 5 || st.AlreadyParsed != 2 {
		t.Fatalf("stats %+v", st)
	}
	if got := sink.snapshot(); got[0] != (added{"20261014", "table", 7, "t9(D)"}) {
		t.Fatalf("sink %+v", got)
	}
}

func TestProcessStoresEmptyResult(t *testing.T) {
	// getCrudInfo returns "" (Java Exception path or no table) and SqlTables
	// still calls TextWR.add with it; the sink applies the empty-text rule.
	sink := &fakeSink{}
	tb, _ := newTables(Options{Sink: sink, Source: &fakeSource{}, Enabled: always(true)})
	tb.process(item{date: "20261015", hash: 3, sql: "MERGE", hasText: true})
	tb.process(item{date: "20261015", hash: 3, sql: "MERGE", hasText: true})
	st := tb.Stats()
	if st.Stored != 1 || st.Empty != 1 || st.AlreadyParsed != 1 || st.ParseFailures != 0 {
		t.Fatalf("stats %+v", st)
	}
	if got := sink.snapshot(); len(got) != 1 || got[0].text != "" {
		t.Fatalf("sink %+v", got)
	}
}

func TestParseFailureMasksSQL(t *testing.T) {
	const marker = "SECRET_LITERAL_MARKER"
	sql := "SELECT a FROM t1 WHERE b = '" + marker + strings.Repeat("x", MaxSQLBytes) + "'"
	for _, logOn := range []bool{false, true} {
		sink := &fakeSink{}
		logs := &fakeLog{}
		tb, _ := newTables(Options{Sink: sink, Source: &fakeSource{}, Enabled: always(true), LogFailEnabled: always(logOn), Log: logs.log})
		tb.process(item{date: "20261015", hash: -5, sql: sql, hasText: true})
		tb.process(item{date: "20261015", hash: -5, sql: sql, hasText: true})
		tb.process(item{date: "20261016", hash: -5, sql: "SELECT a FROM t1", hasText: true}) // failSet is not per date
		st := tb.Stats()
		if st.ParseFailures != 1 || st.AlreadyFailed != 2 || st.FailSetLen != 1 || len(sink.snapshot()) != 0 {
			t.Fatalf("log=%v stats %+v", logOn, st)
		}
		lines := logs.snapshot()
		if !logOn {
			if len(lines) != 0 {
				t.Fatalf("log disabled but got %+v", lines)
			}
			continue
		}
		if len(lines) != 1 || lines[0].id != "S112" || lines[0].sec != 0 {
			t.Fatalf("lines %+v", lines)
		}
		if strings.Contains(lines[0].msg, marker) || strings.Contains(lines[0].msg, "SELECT") {
			t.Fatalf("SQL text leaked into the log: %.200q", lines[0].msg)
		}
		if !strings.Contains(lines[0].msg, "hash=-5") || !strings.Contains(lines[0].msg, "length=") {
			t.Fatalf("missing hash/length: %q", lines[0].msg)
		}
	}
}

func TestStorageErrorKeepsParsedMark(t *testing.T) {
	sink := &fakeSink{err: errors.New("disk")}
	tb, _ := newTables(Options{Sink: sink, Source: &fakeSource{}, Enabled: always(true)})
	tb.process(item{date: "20261015", hash: 1, sql: "SELECT a FROM t1", hasText: true})
	tb.process(item{date: "20261015", hash: 1, sql: "SELECT a FROM t1", hasText: true})
	st := tb.Stats()
	if st.StorageErrors != 1 || st.AlreadyParsed != 1 || st.Stored != 0 || st.ParseFailures != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestQueueOverflowPriorityAndSource(t *testing.T) {
	sink := &fakeSink{entered: make(chan int32, 1), release: make(chan struct{})}
	src := &fakeSource{texts: map[int32]string{}, errs: map[int32]error{}}
	logs := &fakeLog{}
	tb, err := New(Options{Sink: sink, Source: src, Enabled: always(true), Log: logs.log})
	if err != nil {
		t.Fatal(err)
	}
	const date = "20261015"
	tb.Add(date, 0, "SELECT a FROM t0")
	if h := <-sink.entered; h != 0 {
		t.Fatalf("first sink call %d", h)
	}
	for i := 1; i <= Queue1Capacity; i++ {
		tb.Add(date, int32(i), "SELECT a FROM q1")
	}
	for i := 0; i < Queue2Capacity; i++ {
		h := int32(100000 + i)
		switch i % 4 {
		case 0, 1:
			src.texts[h] = "UPDATE q2 SET a=1"
		case 2:
			src.errs[h] = errors.New("read")
		}
		tb.Add(date, h, "SELECT a FROM dropped_text")
	}
	tb.Add(date, -1, "SELECT a FROM rejected")
	tb.Add(date, -2, "SELECT a FROM rejected")
	st := tb.Stats()
	if st.Enqueued1 != Queue1Capacity+1 || st.Enqueued2 != Queue2Capacity || st.Rejected != 2 || st.Queue1Len != Queue1Capacity || st.Queue2Len != Queue2Capacity {
		t.Fatalf("stats %+v", st)
	}
	lines := logs.snapshot()
	if len(lines) != 2 || lines[0] != (logLine{"S111", 10, "queue exceeded!!"}) {
		t.Fatalf("S111 lines %+v (interval suppression is the logger's job)", lines)
	}
	go func() {
		for range sink.entered {
		}
	}()
	close(sink.release)
	want := uint64(1 + Queue1Capacity + Queue2Capacity/2)
	waitFor(t, "drain", func() bool {
		s := tb.Stats()
		return s.Stored+s.SourceErrors+s.TextMissing == want+Queue2Capacity/2
	})
	st = tb.Stats()
	if st.Stored != want || st.SourceErrors != Queue2Capacity/4 || st.TextMissing != Queue2Capacity/4 {
		t.Fatalf("stats %+v", st)
	}
	calls := sink.snapshot()
	for i, c := range calls[:1+Queue1Capacity] {
		if c.hash != int32(i) || c.text != map[bool]string{true: "t0(R)", false: "q1(R)"}[i == 0] {
			t.Fatalf("queue1 order/result at %d: %+v", i, c)
		}
	}
	for _, c := range calls[1+Queue1Capacity:] {
		if c.hash < 100000 || c.text != "q2(U)" {
			t.Fatalf("queue2 item used the dropped text or wrong order: %+v", c)
		}
	}
	src.mu.Lock()
	if src.calls[0] != date+"/sql" {
		t.Fatalf("source lookup %q", src.calls[0])
	}
	src.mu.Unlock()
	if err := tb.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(sink.entered)
}

func TestCloseCountsRemainingAndRejects(t *testing.T) {
	sink := &fakeSink{entered: make(chan int32, 1), release: make(chan struct{})}
	tb, err := New(Options{Sink: sink, Source: &fakeSource{}, Enabled: always(true)})
	if err != nil {
		t.Fatal(err)
	}
	tb.Add("20261015", 1, "SELECT a FROM t1")
	<-sink.entered
	for i := 2; i <= 11; i++ {
		tb.Add("20261015", int32(i), "SELECT a FROM t1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := tb.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close err %v", err)
	}
	tb.Add("20261015", 99, "SELECT a FROM t1")
	if err := tb.Close(context.Background()); err != nil {
		t.Fatalf("second close %v", err)
	}
	st := tb.Stats()
	if st.RemainingAtClose != 10 || st.RejectedClosed != 1 || st.StorageErrors != 1 || st.Queue1Len != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestCloseIdleWorker(t *testing.T) {
	tb, err := New(Options{Sink: &fakeSink{}, Source: &fakeSource{}, Enabled: always(true)})
	if err != nil {
		t.Fatal(err)
	}
	if err := tb.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := tb.Stats(); st.RemainingAtClose != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestConcurrentAddAndClose(t *testing.T) {
	tb, err := New(Options{Sink: &fakeSink{}, Source: &fakeSource{}, Enabled: always(true)})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				tb.Add("20261015", int32(g*10000+i), "SELECT a FROM t1 JOIN t2 ON 1=1")
			}
		}(g)
	}
	time.Sleep(5 * time.Millisecond)
	if err := tb.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	st := tb.Stats()
	in := st.Enqueued1 + st.Enqueued2
	out := st.Stored + st.AlreadyParsed + st.AlreadyFailed + st.TextMissing + st.SourceErrors + st.ParseFailures + st.StorageErrors + st.RemainingAtClose
	if in != out || in+st.Rejected+st.RejectedClosed != 8000 {
		t.Fatalf("accounting %+v", st)
	}
}

func TestIntLinkedSetEvictsOldest(t *testing.T) {
	s := newIntLinkedSet(3)
	for _, k := range []int32{1, 2, 3, 1, 4} { // re-put of 1 keeps its position
		s.put(k)
	}
	if s.contains(1) || !s.contains(2) || !s.contains(3) || !s.contains(4) || s.len() != 3 {
		t.Fatalf("set %+v", s.keys)
	}
	s.put(5)
	if s.contains(2) || !s.contains(5) {
		t.Fatalf("set %+v", s.keys)
	}
}
