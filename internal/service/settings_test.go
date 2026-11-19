package service

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// loginEntries returns the V1 entries of one successful LOGIN.
func loginV1(t *testing.T, ok *protocol.MapPack) (serverID string, tagCount bool, soTimeOut int64, name, pattern string) {
	t.Helper()
	menu := mapValue(ok, "menu")
	if menu.Type != protocol.ValueMap || len(menu.Entries) != 1 || menu.Entries[0].Key != "tag_count" || menu.Entries[0].Value.Type != protocol.ValueBoolean {
		t.Fatalf("menu %#v", menu)
	}
	for _, key := range []string{"server_id", "ext_link_name", "ext_link_url_pattern"} {
		if mapValue(ok, key).Type != protocol.ValueText {
			t.Fatalf("%s type %d", key, mapValue(ok, key).Type)
		}
	}
	if mapValue(ok, "so_time_out").Type != protocol.ValueDecimal {
		t.Fatal("so_time_out type")
	}
	return mapValue(ok, "server_id").Text, menu.Entries[0].Value.Bool, mapValue(ok, "so_time_out").Int, mapValue(ok, "ext_link_name").Text, mapValue(ok, "ext_link_url_pattern").Text
}

func TestLoginJavaDefaultsAndLiveSettings(t *testing.T) {
	s, conn, r := startService(t, Config{Username: "synthetic", Password: "password", ServerID: "7"}, &fakeBackend{})
	send(t, conn, "LOGIN", 0, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
	id, tag, so, name, pattern := loginV1(t, readPacks(t, r)[0].(*protocol.MapPack))
	// Configure.java defaults: tagcnt_enabled=true, 8000, scouter-paper link.
	if id != "7" || !tag || so != 8000 || name != DefaultExtLinkName || pattern != DefaultExtLinkURLPattern {
		t.Fatal(id, tag, so, name, pattern)
	}
	if got := s.Settings(); got != DefaultSettings("7") {
		t.Fatalf("%#v", got)
	}
	// The next LOGIN after a reload uses the new values (empty strings stay empty).
	s.SetSettings(Settings{ServerID: "", ExtLinkName: "", ExtLinkURLPattern: "x", ClientSoTimeoutMillis: -1, TagCountEnabled: false, SearchXLogMaxCount: 500})
	send(t, conn, "LOGIN", 0, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
	id, tag, so, name, pattern = loginV1(t, readPacks(t, r)[0].(*protocol.MapPack))
	if id != "" || tag || so != -1 || name != "" || pattern != "x" {
		t.Fatal(id, tag, so, name, pattern)
	}
	s.SetSettings(Settings{ServerID: "s2", ExtLinkName: "n2", ExtLinkURLPattern: "p2", ClientSoTimeoutMillis: 5000, TagCountEnabled: true})
	send(t, conn, "LOGIN", 0, mp(entry("id", text("synthetic")), entry("pass", text(encrypted("password")))))
	id, tag, so, name, pattern = loginV1(t, readPacks(t, r)[0].(*protocol.MapPack))
	if id != "s2" || !tag || so != 5000 || name != "n2" || pattern != "p2" {
		t.Fatal(id, tag, so, name, pattern)
	}
}

func TestAccountLoginLiveSettings(t *testing.T) {
	s, _, _, conn, r, _ := accountService(t)
	s.SetSettings(Settings{ServerID: "acc", ExtLinkName: "n", ExtLinkURLPattern: "p", ClientSoTimeoutMillis: 1234, TagCountEnabled: false})
	id, tag, so, name, pattern := loginV1(t, accountLoginSession(t, conn, r))
	if id != "acc" || tag || so != 1234 || name != "n" || pattern != "p" {
		t.Fatal(id, tag, so, name, pattern)
	}
}

// boundFake records the MinElapsed the service passes to the backend.
type boundFake struct {
	*fakeBackend
	mu       sync.Mutex
	realtime []int32
	history  []int32
}

func (b *boundFake) XLogsRealtime(_ context.Context, q RealtimeQuery) (RealtimeResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.realtime = append(b.realtime, q.MinElapsed)
	return RealtimeResult{}, nil
}
func (b *boundFake) XLogsHistory(_ context.Context, q HistoryQuery, _ func(*protocol.XLogPack) error) (HistoryResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.history = append(b.history, q.MinElapsed)
	return HistoryResult{}, nil
}

func TestXLogLowerBoundsAreLive(t *testing.T) {
	b := &boundFake{fakeBackend: &fakeBackend{}}
	s, conn, r := startService(t, queryConfig(), b)
	session := loginSession(t, conn, r)
	objHash := entry("objHash", list([]protocol.Value{number(1)}))
	cases := []struct{ bound, limit, want int32 }{
		{0, 0, 0}, {0, -5, 0}, {0, 499, 499}, {500, 0, 500}, {500, 499, 500}, {500, 500, 500}, {500, 501, 501}, {1, 0, 1}, {-10, -5, -5}, {-10, -20, -10},
	}
	for _, tc := range cases {
		live := s.Settings()
		live.XLogRealtimeLowerBoundMillis, live.XLogPasttimeLowerBoundMillis = tc.bound, tc.bound
		s.SetSettings(live)
		send(t, conn, "TRANX_REAL_TIME_GROUP", session, mp(entry("loop", number(0)), entry("index", number(0)), entry("limit", number(int64(tc.limit))), objHash))
		readPacks(t, r)
		send(t, conn, "TRANX_LOAD_TIME_GROUP", session, mp(entry("date", text("20261013")), entry("stime", number(1)), entry("etime", number(2)), entry("limit", number(int64(tc.limit))), entry("max", number(0)), objHash))
		readPacks(t, r)
		b.mu.Lock()
		gotRT, gotH := b.realtime[len(b.realtime)-1], b.history[len(b.history)-1]
		b.mu.Unlock()
		if gotRT != tc.want || gotH != tc.want {
			t.Fatalf("bound %d limit %d: realtime %d history %d, want %d", tc.bound, tc.limit, gotRT, gotH, tc.want)
		}
	}
	// The two bounds are independent keys.
	live := s.Settings()
	live.XLogRealtimeLowerBoundMillis, live.XLogPasttimeLowerBoundMillis = 300, 0
	s.SetSettings(live)
	send(t, conn, "TRANX_REAL_TIME_GROUP", session, mp(entry("loop", number(0)), entry("index", number(0)), entry("limit", number(10)), objHash))
	readPacks(t, r)
	send(t, conn, "TRANX_LOAD_TIME_GROUP", session, mp(entry("date", text("20261013")), entry("stime", number(1)), entry("etime", number(2)), entry("limit", number(10)), entry("max", number(0)), objHash))
	readPacks(t, r)
	if b.realtime[len(b.realtime)-1] != 300 || b.history[len(b.history)-1] != 10 {
		t.Fatal(b.realtime, b.history)
	}
}

// TestSearchXLogMaxCountIsLive checks req_search_xlog_max_count at the
// V2 boundaries: XLOG_LOAD_BY_TXIDS answers at most max-1 ids (Java checks
// after incrementing), SEARCH_XLOG_LIST at most max.
func TestSearchXLogMaxCountIsLive(t *testing.T) {
	const n = 502
	stamp := queryConfig().Clock().UnixMilli()
	byID := map[int64]*protocol.XLogPack{}
	raw := []*protocol.XLogPack{}
	ids := make([]protocol.Value, 0, n)
	for i := int64(1); i <= n; i++ {
		x := &protocol.XLogPack{TxID: i, EndTime: stamp + i}
		byID[i] = x
		raw = append(raw, x)
		ids = append(ids, number(i))
	}
	b := &traceFake{fakeBackend: &fakeBackend{}, byID: byID, raw: map[string][]*protocol.XLogPack{"20261013": raw}}
	s, conn, r := startTraceService(t, b)
	session := loginSession(t, conn, r)
	for _, tc := range []struct{ max, txids, search int }{
		{-1, 0, 0}, {0, 0, 0}, {1, 0, 1}, {2, 1, 2}, {499, 498, 499}, {500, 499, 500}, {501, 500, 501}, {503, 502, 502},
	} {
		live := s.Settings()
		live.SearchXLogMaxCount = int32(tc.max)
		s.SetSettings(live)
		send(t, conn, "XLOG_LOAD_BY_TXIDS", session, mp(entry("date", text("20261013")), entry("txid", list(ids))))
		got := readPacks(t, r)
		if len(got) != tc.txids {
			t.Fatalf("max %d: TXIDS %d, want %d", tc.max, len(got), tc.txids)
		}
		for i, p := range got {
			if p.(*protocol.XLogPack).TxID != int64(i+1) {
				t.Fatal("TXIDS order")
			}
		}
		send(t, conn, "SEARCH_XLOG_LIST", session, mp(entry("stime", number(stamp)), entry("etime", number(stamp+n+1))))
		if got := readPacks(t, r); len(got) != tc.search {
			t.Fatalf("max %d: SEARCH %d, want %d", tc.max, len(got), tc.search)
		}
	}
}

func TestServerLogDirIsLive(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(first, "server-1.log"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(second, "server-2.log"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	meta, err := NewFileMetadata(FileMetadataConfig{LogDir: first})
	if err != nil {
		t.Fatal(err)
	}
	cfg := queryConfig()
	cfg.Metadata = meta
	_, conn, r := startService(t, cfg, &fakeBackend{})
	session := loginSession(t, conn, r)
	names := func() []string {
		send(t, conn, "SERVER_LOG_LIST", session, nil)
		packs := readPacks(t, r)
		if len(packs) == 0 {
			return nil
		}
		var out []string
		for _, v := range lookup(packs[0].(*protocol.MapPack), "name").Values {
			out = append(out, v.Text)
		}
		return out
	}
	detail := func(name string) []protocol.Value {
		send(t, conn, "SERVER_LOG_DETAIL", session, mp(entry("name", text(name))))
		return readValues(t, r)
	}
	if got := names(); len(got) != 1 || got[0] != "server-1.log" {
		t.Fatal(got)
	}
	if got := detail("server-1.log"); len(got) != 1 || got[0].Text != "one" {
		t.Fatal(got)
	}
	meta.SetLogDir(second)
	if got := names(); len(got) != 1 || got[0] != "server-2.log" {
		t.Fatal(got)
	}
	if got := detail("server-1.log"); len(got) != 0 {
		t.Fatal(got)
	}
	if got := detail("server-2.log"); len(got) != 1 || got[0].Text != "two" {
		t.Fatal(got)
	}
	// A missing directory and an empty log_dir answer nothing (File.exists false).
	meta.SetLogDir(filepath.Join(second, "missing"))
	if got := names(); got != nil {
		t.Fatal(got)
	}
	meta.SetLogDir("")
	if got := names(); got != nil {
		t.Fatal(got)
	}
	if got := detail("server-2.log"); len(got) != 0 {
		t.Fatal(got)
	}
}

// TestSettingsConcurrentWithLogin runs under -race: a reload storing new
// settings while LOGINs and searches read them.
func TestSettingsConcurrentWithLogin(t *testing.T) {
	s, err := New(Config{Username: "synthetic", Password: "password"}, &fakeBackend{})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := NewFileMetadata(FileMetadataConfig{LogDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	dirs := []string{t.TempDir(), t.TempDir()}
	s.SetSettings(Settings{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s.SetSettings(Settings{ServerID: "a", ClientSoTimeoutMillis: int32(i), SearchXLogMaxCount: int32(i)})
			meta.SetLogDir(dirs[i%2])
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			v := s.Settings()
			if v.ClientSoTimeoutMillis != v.SearchXLogMaxCount {
				t.Error("torn settings")
				return
			}
			_ = s.traceLimit()
			if _, _, err := meta.Logs(context.Background()); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
}
