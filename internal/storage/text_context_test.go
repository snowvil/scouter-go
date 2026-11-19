package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// PutTextContext does not wait for the Store gate: a held gate only delays
// the write, and TextContext sees the text before it reaches the file. A full
// queue makes PutTextContext wait; ctx ends that wait and the text is not
// stored.
func TestPutTextContextQueueAndCancel(t *testing.T) {
	s, root := liveStore(t, Config{})
	const date = "20261013"
	pack := &protocol.TextPack{XType: "table", Hash: 7, Text: "EMP(S)"}
	s.mu.Lock() // a long query or a hung disk write holding the gate
	done := make(chan error, 1)
	go func() { done <- s.PutTextContext(context.Background(), date, pack) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PutTextContext waited for the gate")
	}
	if _, ok := s.pendingText(textRef{s.permTextPath("table"), string(textKey("table", 7, false))}); !ok {
		t.Fatal("queued text not visible")
	}
	// Once the writer holds that text and waits for the gate, fill the queue
	// behind it, then one more with a deadline.
	for deadline := time.Now().Add(5 * time.Second); len(s.texts.queue) != 0; {
		if time.Now().After(deadline) {
			t.Fatal("writer did not take the first text")
		}
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < textQueueMax; i++ {
		if err := s.PutTextContext(context.Background(), date, &protocol.TextPack{XType: "sql", Hash: int32(i + 1), Text: "q"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.PutTextContext(ctx, date, &protocol.TextPack{XType: "sql", Hash: -5, Text: "late"})
	s.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("full queue", err)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if err := s.PutTextContext(cancelled, date, pack); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "00000000/text/text_table.kfile")); err != nil {
		t.Fatal("queued text not written", err)
	}
	wantText(t, s, date, "table", 7, "EMP(S)", true)
	wantText(t, s, date, "sql", int32(textQueueMax), "q", true)
	wantText(t, s, date, "sql", -5, "", false)
}

// Concurrent cancellable writers, plain writers and readers: every call
// either stores its text or returns a context error, and every stored text
// reads back whole (-race).
func TestPutTextContextConcurrent(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261013"
	var wg sync.WaitGroup
	var mu sync.Mutex
	stored := map[int32]bool{}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				hash := int32(w*1000 + i)
				timeout := time.Duration(i%3) * time.Microsecond // 1-2us: mostly cancelled
				if i%3 == 0 {
					timeout = 10 * time.Second
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				err := s.PutTextContext(ctx, date, &protocol.TextPack{XType: "table", Hash: hash, Text: "t"})
				cancel()
				switch {
				case err == nil:
					mu.Lock()
					stored[hash] = true
					mu.Unlock()
				case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
				default:
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := s.PutText(date, &protocol.TextPack{XType: "sql", Hash: int32(i), Text: "select 1"}); err != nil {
				t.Error(err)
				return
			}
			if _, _, err := s.Text(date, "table", int32(i)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	for hash := range stored {
		wantText(t, s, date, "table", hash, "t", true)
	}
	if len(stored) == 0 {
		t.Fatal("no cancellable write succeeded")
	}
}

// Queued text bytes are bounded: past textQueueMaxBytes PutText waits until
// the writer makes room.
func TestPutTextQueueByteBound(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261013"
	big := string(bytes.Repeat([]byte("x"), 4<<20))
	s.mu.Lock() // the writer cannot write
	put := func(ctx context.Context, h int32) error {
		return s.PutTextContext(ctx, date, &protocol.TextPack{XType: "sql", Hash: h, Text: big})
	}
	queued := 0
	for h := int32(1); ; h++ {
		// A deadline shorter than textQueueWait ends the wait before it turns
		// into a drop (which would saturate the queue).
		ctx, cancel := context.WithTimeout(context.Background(), textQueueWait/4)
		err := put(ctx, h)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break // the byte bound was reached
		}
		if err != nil {
			t.Fatal(err)
		}
		if queued++; queued > textQueueMaxBytes/(4<<20)+2 {
			t.Fatal("no byte bound", queued)
		}
	}
	waiting := make(chan error, 1)
	go func() { waiting <- put(context.Background(), -1) }()
	time.Sleep(textQueueWait / 5) // within the wait: room comes before a drop
	s.mu.Unlock()
	if err := <-waiting; err != nil || s.TextDropped() != 0 { // the writer made room
		t.Fatal(err, s.TextDropped())
	}
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantText(t, s, date, "sql", -1, big, true)
}

// A text the writer already took but has not written when a purge discards
// it under the gate is skipped: the removed directory is not recreated.
func TestTextWriterSkipsTextDiscardedUnderGate(t *testing.T) {
	s, root := liveStore(t, Config{TextDailyService: true})
	const date = "20261013"
	day := filepath.Join(root, date)
	s.mu.Lock()
	if err := s.PutTextContext(context.Background(), date, &protocol.TextPack{XType: "service", Hash: 3, Text: "svc"}); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); len(s.texts.queue) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			s.mu.Unlock()
			t.Fatal("writer did not take the text")
		}
	}
	s.discardTextsLocked([]string{day}) // what retention and delete do under the gate
	s.mu.Unlock()
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(day); !os.IsNotExist(err) {
		t.Fatal("discarded text recreated its directory", err)
	}
	wantText(t, s, date, "service", 3, "", false)
}

// A stalled text writer costs PutText one short wait per saturation; then
// texts are dropped at once and counted (TextWR drops on a full queue), and
// progress of the writer ends the saturation.
func TestTextQueueStalledDropsWithoutSlowingIngest(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261013"
	put := func(h int32) {
		if err := s.PutTextContext(context.Background(), date, &protocol.TextPack{XType: "sql", Hash: h, Text: "q"}); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock() // the writer cannot write
	put(1)
	for deadline := time.Now().Add(5 * time.Second); len(s.texts.queue) != 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			s.mu.Unlock()
			t.Fatal("writer did not take the first text")
		}
	}
	for h := int32(2); h <= textQueueMax+1; h++ {
		put(h)
	}
	start := time.Now()
	put(-1) // waits for room, then drops
	if d := time.Since(start); d < textQueueWait/2 {
		s.mu.Unlock()
		t.Fatal("no wait before the first drop", d)
	}
	start = time.Now()
	for h := int32(-2); h > -1000; h-- {
		put(h)
	}
	if d := time.Since(start); d > textQueueWait {
		s.mu.Unlock()
		t.Fatal("PutText slowed while saturated", d)
	}
	if n := s.TextDropped(); n != 999 {
		s.mu.Unlock()
		t.Fatal("dropped", n)
	}
	s.mu.Unlock()
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	put(-5000) // room again: stored
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantText(t, s, date, "sql", -5000, "q", true)
	wantText(t, s, date, "sql", -1, "q", true) // dropped: from the cache, not the file
	wantText(t, s, date, "sql", textQueueMax+1, "q", true)
	s.texts.mu.Lock()
	cached := len(s.texts.recent)
	s.texts.mu.Unlock()
	if cached != 999 {
		t.Fatal("dropped texts cached", cached)
	}
}

// Dropped texts stay readable (TextCache) within a bounded cache: the oldest
// go first, and a purge discards those under its roots.
func TestDroppedTextCacheBoundAndPurge(t *testing.T) {
	s, root := liveStore(t, Config{TextDailyService: true})
	const date = "20261013"
	drop := func(div string, hash int32, text string) {
		daily := div == "service"
		path := s.permTextPath(div)
		if daily {
			path = s.dailyTextPath(date)
		}
		s.dropText(&textItem{ref: textRef{path, string(textKey(div, hash, daily))}, date: date, daily: daily,
			pack: protocol.TextPack{XType: div, Hash: hash, Text: text}})
	}
	drop("service", 1, "svc")
	drop("sql", 1, "q1")
	wantText(t, s, date, "service", 1, "svc", true)
	wantText(t, s, date, "sql", 1, "q1", true)
	s.mu.Lock()
	s.discardTextsLocked([]string{filepath.Join(root, date)})
	s.mu.Unlock()
	wantText(t, s, date, "service", 1, "", false) // purged with its day
	wantText(t, s, date, "sql", 1, "q1", true)
	for h := int32(2); h <= textRecentMax+1; h++ {
		drop("sql", h, "q")
	}
	wantText(t, s, date, "sql", 1, "", false) // the oldest was pushed out
	wantText(t, s, date, "sql", textRecentMax+1, "q", true)
	big := string(bytes.Repeat([]byte("x"), textRecentMaxBytes/3))
	for h := int32(-1); h >= -4; h-- {
		drop("sql", h, big)
	}
	s.texts.mu.Lock()
	n, size := len(s.texts.recent), s.texts.recentBytes
	s.texts.mu.Unlock()
	if n > textRecentMax || size > textRecentMaxBytes {
		t.Fatal("cache past its bound", n, size)
	}
	wantText(t, s, date, "sql", -4, big, true)
	wantText(t, s, date, "sql", -1, "", false)
}

// Saturation ends only once the writer has brought the queue to half
// (hysteresis), not at the first text it writes.
func TestTextSaturationEndsAtHalf(t *testing.T) {
	s, _ := liveStore(t, Config{})
	q := &s.texts
	q.saturated.Store(true)
	q.mu.Lock()
	q.bytes = textQueueMaxBytes/2 + 200
	q.mu.Unlock()
	s.releaseText(&textItem{size: 100})
	if !q.saturated.Load() {
		t.Fatal("saturation ended above half")
	}
	s.releaseText(&textItem{size: 100})
	if q.saturated.Load() {
		t.Fatal("saturation kept at half")
	}
}

// Close wakes a PutText waiting for room with ErrClosed.
func TestTextCloseEndsEnqueueWait(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261013"
	big := string(bytes.Repeat([]byte("x"), 4<<20))
	s.mu.Lock() // the writer cannot write
	for h := int32(1); h <= textQueueMaxBytes/(4<<20)+1; h++ {
		ctx, cancel := context.WithTimeout(context.Background(), textQueueWait/4)
		err := s.PutTextContext(ctx, date, &protocol.TextPack{XType: "sql", Hash: h, Text: big})
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
	}
	waiting := make(chan error, 1)
	go func() {
		waiting <- s.PutTextContext(context.Background(), date, &protocol.TextPack{XType: "sql", Hash: -1, Text: big})
	}()
	time.Sleep(textQueueWait / 10)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	time.Sleep(textQueueWait / 10)
	s.mu.Unlock()
	if err := <-waiting; err != nil && !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

// A text read from the file is cached (TextCache): the next lookup does not
// wait for the Store gate. The cache keeps the lookup order (daily before
// permanent, per path) and a purge of the day drops its entries.
func TestTextReadCacheSkipsGate(t *testing.T) {
	s, root := liveStore(t, Config{TextDailyService: true})
	const date = "20261013"
	putText(t, s, date, "service", 3, "/svc")
	putText(t, s, date, "sql", 4, "select 1")
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantText(t, s, date, "service", 3, "/svc", true) // read from the file, cached
	wantText(t, s, date, "sql", 4, "select 1", true)
	s.mu.Lock() // a writer holding the gate
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, c := range []struct {
			div  string
			hash int32
			want string
		}{{"service", 3, "/svc"}, {"sql", 4, "select 1"}} {
			if got, ok, err := s.Text(date, c.div, c.hash); err != nil || !ok || got != c.want {
				t.Error(c.div, got, ok, err)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.mu.Unlock()
		t.Fatal("cached lookup waited for the gate")
	}
	s.mu.Unlock()
	if _, err := s.DeleteDatabasePaths(context.Background(), []string{"/" + date}); err != nil { // drops the day's cache entries
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, date)); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	wantText(t, s, date, "service", 3, "", false) // the daily entry went with the day
	wantText(t, s, date, "sql", 4, "select 1", true)
}

// A text the files do not have is remembered as missing, so a repeated
// lookup (ingest group lookup per XLog) does not wait for the Store gate; a
// text that arrives later is found, queued and once written, for daily and
// permanent divs.
func TestTextMissingCache(t *testing.T) {
	for _, daily := range []bool{false, true} {
		s, _ := liveStore(t, Config{TextDailyService: daily})
		const date = "20261013"
		wantText(t, s, date, "service", 9, "", false) // read the files: missing
		s.mu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			if got, ok, err := s.Text(date, "service", 9); err != nil || ok || got != "" {
				t.Error(got, ok, err)
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.mu.Unlock()
			t.Fatal("missing lookup waited for the gate")
		}
		s.mu.Unlock()
		putText(t, s, date, "service", 9, "/later") // arrives now
		wantText(t, s, date, "service", 9, "/later", true)
		if err := s.flushTexts(context.Background()); err != nil {
			t.Fatal(err)
		}
		s.texts.mu.Lock()
		s.texts.read = nil // force the file read path
		s.texts.mu.Unlock()
		wantText(t, s, date, "service", 9, "/later", true)
		if daily {
			// Missing in the day's table and the permanent one: both are
			// remembered, and a permanent text written later is found.
			wantText(t, s, date, "service", 10, "", false)
			s.SetTextDaily(false, false, false)
			putText(t, s, date, "service", 10, "/perm")
			if err := s.flushTexts(context.Background()); err != nil {
				t.Fatal(err)
			}
			s.SetTextDaily(true, false, false)
			wantText(t, s, date, "service", 10, "/perm", true)
		}
	}
}

// A text the writer stored is remembered (read cache): resubmitting it does
// not queue it again (TextDupCheck), and a different text for a stored key
// keeps the first value, as the file does.
func TestTextStoredNotQueuedAgain(t *testing.T) {
	s, _ := liveStore(t, Config{})
	const date = "20261013"
	putText(t, s, date, "group", 5, "/a/*")
	if err := s.flushTexts(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()                                       // the writer cannot write: a queued text would stay pending
	for _, text := range []string{"/a/*", "/other"} { // "/other": a hash collision, the first value wins
		if err := s.PutText(date, &protocol.TextPack{XType: "group", Hash: 5, Text: text}); err != nil {
			s.mu.Unlock()
			t.Fatal(err)
		}
	}
	s.texts.mu.Lock()
	pending := len(s.texts.pending)
	s.texts.mu.Unlock()
	s.mu.Unlock()
	if pending != 0 {
		t.Fatal("stored text queued again", pending)
	}
	wantText(t, s, date, "group", 5, "/a/*", true)
}

// Texts written in batches (a hold of queued texts, mixed dictionaries,
// daily and permanent, repeated keys) leave the files one-at-a-time writes
// leave.
func TestTextBatchMatchesSingleWrites(t *testing.T) {
	const date = "20261013"
	type text struct {
		div  string
		hash int32
		v    string
	}
	var texts []text
	for n := 0; n < 1000; n++ {
		div := []string{"service", "sql", "method", "group"}[n%4]
		texts = append(texts, text{div, int32(n % 300), fmt.Sprintf("%s-%d-%d", div, n%300, n)})
	}
	write := func(dir string, batched bool) {
		s, err := Open(Config{Directory: dir, Location: time.UTC, TextDailyService: true})
		if err != nil {
			t.Fatal(err)
		}
		if batched {
			s.mu.Lock() // queue them all; the writer takes them in holds
		}
		for _, x := range texts {
			if err := s.PutText(date, &protocol.TextPack{XType: x.div, Hash: x.hash, Text: x.v}); err != nil {
				t.Fatal(err)
			}
			if !batched {
				if err := s.flushTexts(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}
		if batched {
			s.mu.Unlock()
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	a, b := t.TempDir(), t.TempDir()
	write(a, false)
	write(b, true)
	ta, tb := treeBytes(t, a), treeBytes(t, b)
	dropRuntimeState(ta)
	dropRuntimeState(tb)
	if len(ta) != len(tb) {
		t.Fatal(len(ta), len(tb))
	}
	for name, want := range ta {
		if !bytes.Equal(tb[name], want) {
			t.Fatal(name, "differs")
		}
	}
}
