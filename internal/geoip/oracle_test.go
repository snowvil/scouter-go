package geoip

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// golden.json은 scripts/java-geoip.py가 geoip-api 1.3.1 LookupService(GEOIP_MEMORY_CACHE)로 만든 결과다.
const oracleDir = "testdata/oracle"

type goldenFile struct {
	CountryCodes []string   `json:"countryCodes"`
	Databases    []goldenDB `json:"databases"`
}

type goldenDB struct {
	File    string         `json:"file"`
	Open    goldenOutcome  `json:"open"`
	Lookups []goldenLookup `json:"lookups"`
}

type goldenOutcome struct {
	OK           bool    `json:"ok"`
	Type         int     `json:"type"`
	Segments     *int32  `json:"segments"`
	RecordLength int32   `json:"recordLength"`
	Info         string  `json:"info"`
	Class        string  `json:"class"`
	Message      *string `json:"message"`
	Cause        *string `json:"cause"`
}

type goldenLookup struct {
	IP            string         `json:"ip"`
	Result        string         `json:"result"`
	CountryCode   *string        `json:"countryCode"`
	Region        *string        `json:"region"`
	City          *string        `json:"city"`
	PostalCode    *string        `json:"postalCode"`
	LatitudeBits  string         `json:"latitudeBits"`
	LongitudeBits string         `json:"longitudeBits"`
	MetroCode     int32          `json:"metroCode"`
	DmaCode       int32          `json:"dmaCode"`
	AreaCode      int32          `json:"areaCode"`
	Exception     *goldenOutcome `json:"exception"`
}

func loadGolden(t testing.TB) goldenFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(oracleDir, "golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var g goldenFile
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Databases) == 0 {
		t.Fatal("golden has no databases")
	}
	return g
}

func TestOracleCountryCodes(t *testing.T) {
	g := loadGolden(t)
	if len(g.CountryCodes) != len(countryCodes) {
		t.Fatalf("golden has %d country codes, Go table %d", len(g.CountryCodes), len(countryCodes))
	}
	for i, c := range g.CountryCodes {
		if countryCodes[i] != c {
			t.Errorf("countryCodes[%d] = %q, java %q", i, countryCodes[i], c)
		}
	}
}

// javaString은 Java null을 Go ""로 바꾼다. 원본은 길이 0 문자열 대신 null을 만들므로 ""가 golden에 있으면 대응이 깨진다.
func javaString(t *testing.T, where string, s *string) string {
	t.Helper()
	if s == nil {
		return ""
	}
	if *s == "" {
		t.Fatalf("%s: golden has a non-null empty string; Go cannot distinguish it from null", where)
	}
	return *s
}

func checkJavaError(t *testing.T, where string, err error, want goldenOutcome) {
	t.Helper()
	var ge *Error
	if !errors.As(err, &ge) {
		t.Errorf("%s: err = %v, want *Error java %s", where, err, want.Class)
		return
	}
	if ge.Java != want.Class {
		t.Errorf("%s: java exception = %q, golden %q (%v)", where, ge.Java, want.Class, err)
	}
	if want.Message != nil && !strings.HasPrefix(ge.Msg, *want.Message) {
		t.Errorf("%s: message = %q, golden %q", where, ge.Msg, *want.Message)
	}
}

func TestOracleLookups(t *testing.T) {
	g := loadGolden(t)
	total := map[string]int{}
	for _, gd := range g.Databases {
		t.Run(gd.File, func(t *testing.T) {
			db, err := Open(filepath.Join(oracleDir, gd.File))
			if !gd.Open.OK {
				if err == nil {
					t.Fatalf("Open succeeded, java threw %s", gd.Open.Class)
				}
				checkJavaError(t, "open", err, gd.Open)
				for _, l := range gd.Lookups {
					if l.Result != "not-open" {
						t.Fatalf("golden lookup %s on unopened db: %s", l.IP, l.Result)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Open: %v (java opened type %d)", err, gd.Open.Type)
			}
			if db.Type() != gd.Open.Type || db.recordLength != gd.Open.RecordLength || db.Info() != gd.Open.Info {
				t.Errorf("open: type/rl/info = %d/%d/%q, java %d/%d/%q", db.Type(), db.recordLength, db.Info(), gd.Open.Type, gd.Open.RecordLength, gd.Open.Info)
			}
			if (gd.Open.Segments == nil) != !db.hasSegments || (gd.Open.Segments != nil && *gd.Open.Segments != db.segments) {
				t.Errorf("open: segments = %d (has %v), java %v", db.segments, db.hasSegments, gd.Open.Segments)
			}
			for _, l := range gd.Lookups {
				total[l.Result]++
				addr, err := netip.ParseAddr(l.IP)
				if err != nil || !addr.Is4() {
					t.Fatalf("bad golden ip %q", l.IP)
				}
				loc, found, err := db.Lookup(addr.As4())
				where := l.IP
				switch l.Result {
				case "null":
					if err != nil || found {
						t.Errorf("%s: got found=%v err=%v loc=%+v, java null", where, found, err, loc)
					}
				case "exception":
					if err == nil {
						t.Errorf("%s: got found=%v loc=%+v, java threw %s", where, found, loc, l.Exception.Class)
						continue
					}
					if found {
						t.Errorf("%s: found=true with error", where)
					}
					checkJavaError(t, where, err, *l.Exception)
				case "location":
					if err != nil || !found {
						t.Errorf("%s: got found=%v err=%v, java location", where, found, err)
						continue
					}
					if l.MetroCode != l.DmaCode {
						t.Fatalf("%s: golden metro %d != dma %d", where, l.MetroCode, l.DmaCode)
					}
					want := Location{
						CountryCode: javaString(t, where, l.CountryCode),
						Region:      javaString(t, where, l.Region),
						City:        javaString(t, where, l.City),
						PostalCode:  javaString(t, where, l.PostalCode),
						Latitude:    floatBits(t, l.LatitudeBits),
						Longitude:   floatBits(t, l.LongitudeBits),
						MetroCode:   l.MetroCode,
						AreaCode:    l.AreaCode,
					}
					if loc != want || math.Float32bits(loc.Latitude) != math.Float32bits(want.Latitude) ||
						math.Float32bits(loc.Longitude) != math.Float32bits(want.Longitude) {
						t.Errorf("%s:\n got  %+v\n java %+v", where, loc, want)
					}
				default:
					t.Fatalf("%s: unexpected golden result %q", where, l.Result)
				}
			}
		})
	}
	// golden이 의도한 경로를 모두 포함하는지(재생성 시 case가 사라지면 실패).
	for _, r := range []string{"location", "null", "exception"} {
		if total[r] == 0 {
			t.Errorf("golden has no %q lookups", r)
		}
	}
}

func floatBits(t *testing.T, hex string) float32 {
	t.Helper()
	v, err := strconv.ParseUint(hex, 16, 32)
	if err != nil {
		t.Fatalf("bad float bits %q", hex)
	}
	return math.Float32frombits(uint32(v))
}

// TestOracleCoverage는 golden이 과제에서 요구한 경계(예외 class별, 비ASCII city, 빈 city, 0.0.0.0, 255.255.255.255)를
// 실제로 담고 있는지 확인한다.
func TestOracleCoverage(t *testing.T) {
	g := loadGolden(t)
	classes := map[string]bool{}
	var latin1City, emptyCity, zero, ones, metro bool
	for _, gd := range g.Databases {
		if !gd.Open.OK {
			classes["open:"+gd.Open.Class] = true
		}
		for _, l := range gd.Lookups {
			if l.Exception != nil {
				classes["lookup:"+l.Exception.Class] = true
			}
			if gd.File != "city_rev1.dat" || l.Result != "location" {
				continue
			}
			if l.City == nil {
				emptyCity = true
			} else if strings.ContainsFunc(*l.City, func(r rune) bool { return r >= 0x80 }) {
				latin1City = true
			}
			zero = zero || l.IP == "0.0.0.0"
			ones = ones || l.IP == "255.255.255.255"
			metro = metro || l.MetroCode != 0
		}
	}
	for _, c := range []string{"open:" + JavaIOException, "open:" + JavaEOFException, "open:" + JavaInvalidDatabase,
		"lookup:" + JavaArrayIndexOutOfBounds, "lookup:" + JavaIndexOutOfBounds, "lookup:" + JavaBufferUnderflow,
		"lookup:" + JavaNullPointer, "lookup:" + JavaInvalidDatabase} {
		if !classes[c] {
			t.Errorf("golden lacks %s", c)
		}
	}
	if !latin1City || !emptyCity || !zero || !ones || !metro {
		t.Errorf("golden coverage: latin1City=%v emptyCity=%v 0.0.0.0=%v 255.255.255.255=%v metro=%v", latin1City, emptyCity, zero, ones, metro)
	}
}

// 비ASCII city는 byte마다 U+0000-U+00FF 한 문자가 된다(UTF-8 원문도 mojibake 그대로).
func TestLatin1CityExamples(t *testing.T) {
	db, err := Open(filepath.Join(oracleDir, "city_rev1.dat"))
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[[4]byte]string{
		{3, 0, 0, 1}: "Z\u00fcrich",
		{3, 1, 0, 1}: "S\u00c3\u00a3o Paulo",
		{3, 2, 0, 1}: string([]rune{0xec, 0x84, 0x9c, 0xec, 0x9a, 0xb8}),
	} {
		loc, found, err := db.Lookup(ip)
		if err != nil || !found || loc.City != want {
			t.Errorf("%v: city %q found=%v err=%v, want %q", ip, loc.City, found, err, want)
		}
	}
}

func ExampleDB_Lookup() {
	db, err := Open(filepath.Join(oracleDir, "city_rev1.dat"))
	if err != nil {
		fmt.Println(err)
		return
	}
	loc, found, err := db.Lookup([4]byte{1, 2, 3, 4})
	fmt.Println(loc.CountryCode, loc.City, loc.MetroCode, loc.AreaCode, found, err)
	_, found, err = db.Lookup([4]byte{4, 0, 0, 1})
	fmt.Println(found, err)
	// Output:
	// US Mountain View 807 650 true <nil>
	// false <nil>
}
