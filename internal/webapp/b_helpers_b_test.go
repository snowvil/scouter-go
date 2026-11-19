package webapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Test helpers for the B controllers. They only use A's fixed test helpers
// (newFakeCollector, testConfig, newTestServer, doHTTP, resultOf).

var bKST = time.FixedZone("KST", 9*3600)

func bSetup(t *testing.T, tune func(*Config)) (*fakeCollector, *Server, *httptest.Server) {
	t.Helper()
	f := newFakeCollector(t)
	cfg := testConfig(f)
	cfg.Location = bKST
	if tune != nil {
		tune(&cfg)
	}
	s, hs := newTestServer(t, cfg)
	return f, s, hs
}

func bHTTP(t *testing.T, hs *httptest.Server, method, path, body string) httpResult {
	t.Helper()
	h := map[string]string{}
	if body != "" {
		h["Content-Type"] = "application/json"
	}
	return doHTTP(t, hs.Client(), method, hs.URL+path, body, h)
}

// bExpect checks status and, when want != "", the exact "result" JSON.
func bExpect(t *testing.T, res httpResult, status int, want string) {
	t.Helper()
	if res.status != status {
		t.Fatalf("status %d, want %d: %s", res.status, status, res.body)
	}
	if want != "" {
		if got := resultOf(t, res.body); got != want {
			t.Fatalf("result\n got %s\nwant %s", got, want)
		}
	}
}

// bExpectError checks an error wrapper (status, resultCode, message).
func bExpectError(t *testing.T, res httpResult, status int, message string) {
	t.Helper()
	if res.status != status {
		t.Fatalf("status %d, want %d: %s", res.status, status, res.body)
	}
	var w struct {
		Status     string  `json:"status"`
		ResultCode string  `json:"resultCode"`
		Message    string  `json:"message"`
		Result     *string `json:"result"`
	}
	if err := json.Unmarshal([]byte(res.body), &w); err != nil {
		t.Fatalf("bad error body %q: %v", res.body, err)
	}
	if w.Status != strconv.Itoa(status) || w.ResultCode != strconv.Itoa(status) || w.Result != nil {
		t.Fatalf("error wrapper %s", res.body)
	}
	if message != "" && w.Message != message {
		t.Fatalf("message %q, want %q", w.Message, message)
	}
}

// bStreamPrefix is the CommonResultView.jsonStream/jsonArrayStream head.
const bStreamPrefix = `{"status":"200","resultCode":"0","message":"","result":`

// bPackString renders a request MapPack as key=type:value pairs in order.
func bPackString(p *protocol.MapPack) string {
	if p == nil {
		return "<nil>"
	}
	parts := make([]string, len(p.Entries))
	for i, e := range p.Entries {
		parts[i] = e.Key + "=" + bValueDebug(e.Value)
	}
	return strings.Join(parts, ",")
}

func bValueDebug(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueText:
		return "T:" + v.Text
	case protocol.ValueDecimal:
		return "D:" + strconv.FormatInt(v.Int, 10)
	case protocol.ValueBoolean:
		return "B:" + strconv.FormatBool(v.Bool)
	case protocol.ValueList:
		s := make([]string, len(v.Values))
		for i := range v.Values {
			s[i] = bValueDebug(v.Values[i])
		}
		return "L[" + strings.Join(s, " ") + "]"
	case protocol.ValueMap:
		s := make([]string, len(v.Entries))
		for i, e := range v.Entries {
			s[i] = e.Key + "=" + bValueDebug(e.Value)
		}
		return "M{" + strings.Join(s, " ") + "}"
	case protocol.ValueNull:
		return "null"
	}
	return "?" + strconv.Itoa(int(v.Type))
}

func bLastReq(t *testing.T, f *fakeCollector, cmd string) fakeReq {
	t.Helper()
	reqs := f.requests(cmd)
	if len(reqs) == 0 {
		t.Fatalf("no %s request", cmd)
	}
	return reqs[len(reqs)-1]
}

// bTexts serves a dictionary ("type:hash" -> text) through both text commands
// the framework may use (GET_TEXT_ANY_TYPE TextPacks, GET_TEXT_100 MapPack).
func bTexts(f *fakeCollector, dict map[string]string) {
	f.handle("GET_TEXT_ANY_TYPE", bodyPack, func(r fakeReq) ([]any, error) {
		types, hashes := bMapList(r.Pack, "type"), bMapList(r.Pack, "hash")
		var out []any
		for i := range hashes {
			typ := ""
			if i < len(types) {
				typ = types[i].Text
			}
			h := bInt(hashes[i])
			if txt, ok := dict[typ+":"+strconv.Itoa(int(h))]; ok {
				out = append(out, &protocol.TextPack{XType: typ, Hash: h, Text: txt})
			}
		}
		return out, nil
	})
	f.handle("GET_TEXT_100", bodyPack, func(r fakeReq) ([]any, error) {
		typ := ""
		if v := bMapText(r.Pack, "type"); v != nil {
			typ = *v
		}
		m := bMap()
		for _, hv := range bMapList(r.Pack, "hash") {
			h := bInt(hv)
			if txt, ok := dict[typ+":"+strconv.Itoa(int(h))]; ok {
				m.Entries = append(m.Entries, bEntry(bHexa32(int64(h)), bTextV(txt)))
			}
		}
		return []any{m}, nil
	})
}

type bOracle struct {
	Timezone string `json:"timezone"`
	Steps    []struct {
		Hex   string `json:"hex"`
		JSON  string `json:"json"`
		SSpan string `json:"sspan"`
	} `json:"steps"`
	XLog struct {
		Hex      string `json:"hex"`
		SXLog    string `json:"sxlog"`
		XLogData string `json:"xlogData"`
	} `json:"xlog"`
	Doubles []struct {
		Bits, Double, DecimalValue string
	} `json:"doubles"`
	Floats []struct {
		Bits  string `json:"bits"`
		Float string `json:"float"`
	} `json:"floats"`
	IntSets []struct {
		In    []int32 `json:"in"`
		Order []int32 `json:"order"`
	} `json:"intSets"`
	StringSets []struct {
		In    []string `json:"in"`
		Order []string `json:"order"`
	} `json:"stringSets"`
	Formats []struct {
		Format string   `json:"format"`
		Args   []string `json:"args"`
		Result string   `json:"result"`
	} `json:"formats"`
	SimpleDate []struct {
		In     string `json:"in"`
		Millis string `json:"millis"`
		Error  string `json:"error"`
	} `json:"simpleDate"`
	LocalDateTime []struct {
		Pattern string `json:"pattern"`
		In      string `json:"in"`
		Millis  string `json:"millis"`
		Error   string `json:"error"`
	} `json:"localDateTime"`
	Shortener []struct {
		URL    string `json:"url"`
		Hash   int32  `json:"hash"`
		Hexa32 string `json:"hexa32"`
	} `json:"shortener"`
	Hexa32 []struct {
		N string `json:"n"`
		S string `json:"s"`
	} `json:"hexa32"`
}

func bLoadOracle(t *testing.T) bOracle {
	t.Helper()
	data, err := os.ReadFile("testdata/b_java_oracle.json")
	if err != nil {
		t.Fatal(err)
	}
	var o bOracle
	if err := json.Unmarshal(data, &o); err != nil {
		t.Fatal(err)
	}
	if o.Timezone != "Asia/Seoul" {
		t.Fatalf("oracle timezone %q", o.Timezone)
	}
	return o
}

func bMustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := appendJSON(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var _ = http.MethodGet
