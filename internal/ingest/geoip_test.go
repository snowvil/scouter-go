package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/geoip"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
)

type fakeGeo struct {
	calls int
	table map[[4]byte][2]string
	all   *[2]string // answer for addresses not in table
	err   error
}

func (f *fakeGeo) Lookup(ip [4]byte) (geoip.Location, bool, error) {
	f.calls++
	if f.err != nil {
		return geoip.Location{}, false, f.err
	}
	if v, ok := f.table[ip]; ok {
		return geoip.Location{CountryCode: v[0], City: v[1]}, true, nil
	}
	if f.all != nil {
		return geoip.Location{CountryCode: f.all[0], City: f.all[1]}, true, nil
	}
	return geoip.Location{}, false, nil
}

func geoXLog(txid int64, ip []byte) *protocol.XLogPack {
	return &protocol.XLogPack{TxID: txid, Service: 1, ObjHash: 1, Elapsed: 1, IPAddress: ip, TailVersion: protocol.XLogCurrentTail}
}

func TestPrivateIPSignedByte(t *testing.T) {
	cases := map[[2]byte]bool{
		{10, 0}: true, {10, 255}: true, {127, 0}: true, {127, 200}: true,
		{172, 15}: false, {172, 16}: true, {172, 31}: true, {172, 32}: false,
		{172, 200}: false, // signed -56
		{192, 168}: false, // "ip(1) == 168" never holds for a Java byte
		{192, 0}:   false, {8, 8}: false, {0, 0}: false,
	}
	for k, want := range cases {
		if got := privateIP([]byte{k[0], k[1], 1, 1}); got != want {
			t.Errorf("%d.%d private=%v", k[0], k[1], got)
		}
	}
}

func TestGeoIPAppliedBeforeHookAndStore(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	geo := &fakeGeo{table: map[[4]byte][2]string{{8, 8, 8, 8}: {"US", "Mountain View"}, {1, 1, 1, 1}: {"AU", ""}, {9, 9, 9, 9}: {"", "Nowhere"}}}
	var hooked []string
	e, s := testEngineWith(t, &now, func(c *Config) {
		c.GeoIP = geo
		c.OnXLog = func(p *protocol.XLogPack) { hooked = append(hooked, p.CountryCode) }
	})
	ctx := context.Background()
	accept := func(p *protocol.XLogPack) *protocol.XLogPack {
		t.Helper()
		if err := e.Accept(ctx, p); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil { // the XLog writer stores it (XLogWR)
			t.Fatal(err)
		}
		stored, err := s.XLog("20261015", p.TxID)
		if err != nil || stored == nil {
			t.Fatal(stored, err)
		}
		return stored
	}
	got := accept(geoXLog(1, []byte{8, 8, 8, 8}))
	if got.CountryCode != "US" || got.City != storage.CounterHash("Mountain View") || hooked[0] != "US" {
		t.Fatalf("applied %q %d hook=%v", got.CountryCode, got.City, hooked)
	}
	if text, ok, err := s.Text("20261015", "city", storage.CounterHash("Mountain View")); err != nil || !ok || text != "Mountain View" {
		t.Fatalf("city text %q %v %v", text, ok, err)
	}
	// Cached: no second lookup.
	accept(geoXLog(2, []byte{8, 8, 8, 8}))
	if geo.calls != 1 {
		t.Fatalf("location cache: %d lookups", geo.calls)
	}
	// Empty city becomes "unknown"; empty country stays empty (Java null).
	if got = accept(geoXLog(3, []byte{1, 1, 1, 1})); got.CountryCode != "AU" || got.City != storage.CounterHash("unknown") {
		t.Fatalf("unknown city %+v", got)
	}
	if got = accept(geoXLog(4, []byte{9, 9, 9, 9})); got.CountryCode != "" || got.City != storage.CounterHash("Nowhere") {
		t.Fatalf("null country %+v", got)
	}
	// Skips: preset country, private, zero address, wrong length, not found,
	// disabled, APP_SERVICE is processed but other types are not.
	calls := geo.calls
	preset := geoXLog(5, []byte{8, 8, 4, 4})
	preset.CountryCode = "KR"
	if got = accept(preset); got.CountryCode != "KR" || got.City != 0 {
		t.Fatal("preset country")
	}
	for i, ip := range [][]byte{{10, 1, 1, 1}, {0, 0, 0, 0}, {8, 8, 8}, nil} {
		if got = accept(geoXLog(int64(10+i), ip)); got.CountryCode != "" || got.City != 0 {
			t.Fatalf("skip %v", ip)
		}
	}
	if geo.calls != calls {
		t.Fatal("skipped addresses must not be looked up")
	}
	if got = accept(geoXLog(20, []byte{2, 2, 2, 2})); got.City != 0 || geo.calls != calls+1 {
		t.Fatal("not found")
	}
	accept(geoXLog(21, []byte{2, 2, 2, 2}))
	if geo.calls != calls+2 {
		t.Fatal("a miss is not cached")
	}
	off := DefaultSettings()
	off.GeoIP = false
	e.SetSettings(off)
	if got = accept(geoXLog(22, []byte{8, 8, 8, 8})); got.CountryCode != "" {
		t.Fatal("geoip_enabled=false")
	}
	e.SetSettings(DefaultSettings())
	other := geoXLog(23, []byte{8, 8, 8, 8})
	other.XType = 2 // APP_SQL is not calc'ed
	if got = accept(other); got.CountryCode != "" {
		t.Fatal("only WEB_SERVICE/APP_SERVICE")
	}
	app := geoXLog(24, []byte{8, 8, 8, 8})
	app.XType = 1
	if got = accept(app); got.CountryCode != "US" {
		t.Fatal("APP_SERVICE")
	}
	// 192.168.x.x is looked up (signed byte quirk).
	if accept(geoXLog(25, []byte{192, 168, 0, 1})); geo.calls != calls+3 {
		t.Fatal("192.168 must be looked up")
	}
	// A legacy tail-0 pack gains append group 1 so the values are stored.
	old := geoXLog(26, []byte{8, 8, 8, 8})
	old.TailVersion = 0
	if got = accept(old); got.CountryCode != "US" || got.TailVersion != 1 {
		t.Fatalf("tail bump %+v", got)
	}
}

func TestGeoIPNilLookupAndFIFOCaches(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	e, _ := testEngineWith(t, &now, nil)
	p := geoXLog(1, []byte{8, 8, 8, 8})
	e.mu.Lock()
	if e.applyGeoIPLocked(p, "20261015"); p.CountryCode != "" {
		t.Fatal("nil lookup is a no-op")
	}
	all := [2]string{"ZZ", "c"}
	geo := &fakeGeo{all: &all}
	e.config.GeoIP = geo
	ip := func(i int) []byte {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(0x08000000+i))
		return b
	}
	for i := 0; i <= geoCacheMax; i++ {
		e.applyGeoIPLocked(geoXLog(int64(i), ip(i)), "20261015")
	}
	if geo.calls != geoCacheMax+1 || len(e.geo.locations) != geoCacheMax || len(e.geo.cities) != 1 {
		t.Fatalf("calls=%d locations=%d", geo.calls, len(e.geo.locations))
	}
	// The head was evicted (FIFO), the second entry is still cached.
	e.applyGeoIPLocked(geoXLog(1, ip(1)), "20261015")
	if geo.calls != geoCacheMax+1 {
		t.Fatal("ip(1) must be cached")
	}
	e.applyGeoIPLocked(geoXLog(0, ip(0)), "20261015")
	if geo.calls != geoCacheMax+2 {
		t.Fatal("ip(0) must have been evicted")
	}
	e.mu.Unlock()
}

// A failed city write keeps the XLog without country/city, is counted, and
// leaves both caches untouched so the next miss writes the text again.
func TestGeoIPCityWriteFailure(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	geo := &fakeGeo{table: map[[4]byte][2]string{{8, 8, 8, 8}: {"US", "x"}}}
	e, s := testEngineWith(t, &now, func(c *Config) { c.GeoIP = geo })
	writeErr := errors.New("disk full")
	var writes []string
	e.geo.putText = func(_ string, p *protocol.TextPack) error {
		writes = append(writes, p.Text)
		return writeErr
	}
	ctx := context.Background()
	if err := e.Accept(ctx, geoXLog(1, []byte{8, 8, 8, 8})); err != nil {
		t.Fatalf("a failed city write must not fail the XLog: %v", err)
	}
	if err := s.Sync(); err != nil { // the XLog writer stores it (XLogWR)
		t.Fatal(err)
	}
	x, err := s.XLog("20261015", 1)
	if err != nil || x == nil || x.CountryCode != "" || x.City != 0 {
		t.Fatalf("xlog must be stored unenriched: %+v %v", x, err)
	}
	if st := e.Stats(); st.GeoIPErrors != 1 || st.GeoIPTextErrors != 1 || st.Rejected != 0 || len(e.geo.locations) != 0 || len(e.geo.cities) != 0 {
		t.Fatalf("stats %+v locations=%d cities=%d", st, len(e.geo.locations), len(e.geo.cities))
	}
	writeErr = nil
	if err := e.Accept(ctx, geoXLog(2, []byte{8, 8, 8, 8})); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // the XLog writer stores it (XLogWR)
		t.Fatal(err)
	}
	x, err = s.XLog("20261015", 2)
	if err != nil || x == nil || x.CountryCode != "US" || x.City != storage.CounterHash("x") {
		t.Fatalf("retry must enrich: %+v %v", x, err)
	}
	if len(writes) != 2 || len(e.geo.locations) != 1 || len(e.geo.cities) != 1 || e.Stats().GeoIPErrors != 1 {
		t.Fatalf("retry writes=%v stats=%+v", writes, e.Stats())
	}
}

// A failing lookup keeps the XLog (Java drops it from the XLogCore loop),
// skips only the enrichment and is counted.
func TestGeoIPLookupErrorKeepsXLog(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	geo := &fakeGeo{err: errors.New("corrupt record")}
	e, s := testEngineWith(t, &now, func(c *Config) { c.GeoIP = geo })
	if err := e.Accept(context.Background(), geoXLog(1, []byte{8, 8, 8, 8})); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil { // the XLog writer stores it (XLogWR)
		t.Fatal(err)
	}
	x, err := s.XLog("20261015", 1)
	if err != nil || x == nil || x.CountryCode != "" || x.City != 0 {
		t.Fatalf("xlog %+v %v", x, err)
	}
	if st := e.Stats(); st.GeoIPErrors != 1 || st.Rejected != 0 || len(e.geo.locations) != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// SetGeoIP (file reload) clears the IP->Location cache, keeps the city set.
func TestSetGeoIPClearsLocationCache(t *testing.T) {
	now := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	old := &fakeGeo{table: map[[4]byte][2]string{{8, 8, 8, 8}: {"US", "A"}}}
	e, _ := testEngineWith(t, &now, func(c *Config) { c.GeoIP = old })
	p := geoXLog(1, []byte{8, 8, 8, 8})
	e.mu.Lock()
	e.applyGeoIPLocked(p, "20261015")
	e.mu.Unlock()
	next := &fakeGeo{table: map[[4]byte][2]string{{8, 8, 8, 8}: {"CA", "A"}}}
	e.SetGeoIP(next)
	if len(e.geo.locations) != 0 || len(e.geo.cities) != 1 {
		t.Fatal("reload must clear only the location cache")
	}
	p = geoXLog(2, []byte{8, 8, 8, 8})
	e.mu.Lock()
	e.applyGeoIPLocked(p, "20261015")
	e.mu.Unlock()
	if p.CountryCode != "CA" || next.calls != 1 {
		t.Fatalf("after reload %q", p.CountryCode)
	}
	e.SetGeoIP(nil)
	p = geoXLog(3, []byte{8, 8, 8, 8})
	e.mu.Lock()
	e.applyGeoIPLocked(p, "20261015")
	e.mu.Unlock()
	if p.CountryCode != "" {
		t.Fatal("nil database disables the step")
	}
}
