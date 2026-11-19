package pulse

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
)

// Java oracle fixture schema. The golden was generated from the private Pulse
// plugin source (pinned by digest); the generator is not part of this
// repository, so the committed files are fixed reference data. Cases of one
// config_profile ran in one JVM in `sequence` order and share the static
// filtered-txid map and TextCache, so they are replayed the same way here.
type goldenFile struct {
	PluginSourceDigest string       `json:"plugin_source_digest"`
	CaseCount          int          `json:"case_count"`
	Cases              []goldenCase `json:"cases"`
}

type goldenMetadata struct {
	Timezone string `json:"timezone"`
	Profiles map[string]struct {
		Conf map[string]string `json:"conf"`
	} `json:"profiles"`
}

type goldenCase struct {
	ID            string   `json:"id"`
	Sequence      int      `json:"sequence"`
	Description   string   `json:"description"`
	ConfigProfile string   `json:"config_profile"`
	Kind          string   `json:"kind"` // "trace" or "profile"
	Notes         []string `json:"notes"`
	Input         struct {
		Dictionary       []goldenText `json:"dictionary"`        // TextCache seeds (date-agnostic)
		FiledbDictionary []goldenText `json:"filedb_dictionary"` // date-specific text DB rows
		Pack             struct {
			Class  string          `json:"class"`
			Fields json.RawMessage `json:"fields"`
		} `json:"pack"`
	} `json:"input"`
	Result struct {
		Sent          bool            `json:"sent"`
		RecordCount   int             `json:"record_count"`
		FilteredDelta int             `json:"filtered_records_delta"`
		FilteredStage *string         `json:"filtered_stage"`
		Exception     json.RawMessage `json:"exception"`
		Record        *struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Key       int64  `json:"key"`
			KeyHex    string `json:"key_hex"`
			Headers   []struct {
				Name     string `json:"name"`
				ValueHex string `json:"value_hex"`
			} `json:"headers"`
			Application  string `json:"application"`
			ValueHex     string `json:"value_hex"`
			ProfileSteps []struct {
				StepHex   string   `json:"step_hex"`
				MainValue *string  `json:"main_value"`
				Addl      []string `json:"additional_value_list"`
			} `json:"profile_steps"`
		} `json:"record"`
	} `json:"result"`
}

type goldenText struct {
	Date string `json:"date"` // yyyymmdd or "today"
	Type string `json:"type"`
	Hash int32  `json:"hash"`
	Text string `json:"text"`
}

type goldenXLogFields struct {
	EndTime                        int64   `json:"endTime"`
	ObjHash                        int32   `json:"objHash"`
	Service                        int32   `json:"service"`
	TxID                           int64   `json:"txid"`
	ThreadNameHash                 int32   `json:"threadNameHash"`
	Caller                         int64   `json:"caller"`
	GxID                           int64   `json:"gxid"`
	Elapsed                        int32   `json:"elapsed"`
	Error                          int32   `json:"error"`
	CPU                            int32   `json:"cpu"`
	SQLCount                       int32   `json:"sqlCount"`
	SQLTime                        int32   `json:"sqlTime"`
	IPAddrHex                      *string `json:"ipaddr_hex"`
	KBytes                         int32   `json:"kbytes"`
	Status                         int32   `json:"status"`
	APICallCount                   int32   `json:"apicallCount"`
	APICallTime                    int32   `json:"apicallTime"`
	XType                          int8    `json:"xType"`
	ProfileCount                   int32   `json:"profileCount"`
	B3Mode                         bool    `json:"b3Mode"`
	ProfileSize                    int32   `json:"profileSize"`
	DiscardType                    int8    `json:"discardType"`
	IgnoreGlobalConsequentSampling bool    `json:"ignoreGlobalConsequentSampling"`
}

type goldenProfileFields struct {
	Time       int64   `json:"time"`
	ObjHash    int32   `json:"objHash"`
	Service    int32   `json:"service"`
	TxID       int64   `json:"txid"`
	Elapsed    int32   `json:"elapsed"` // in-memory only; never on the wire
	ProfileHex *string `json:"profile_hex"`
}

func loadGolden(t *testing.T) (*goldenFile, *goldenMetadata) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "pulse-golden.json"))
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("golden not generated")
	}
	if err != nil {
		t.Fatal(err)
	}
	var g goldenFile
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("golden schema: %v", err)
	}
	meta, err := os.ReadFile(filepath.Join("testdata", "pulse-metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m goldenMetadata
	if err := json.Unmarshal(meta, &m); err != nil {
		t.Fatalf("metadata schema: %v", err)
	}
	if g.CaseCount != len(g.Cases) || len(g.Cases) == 0 {
		t.Fatalf("case_count %d, cases %d", g.CaseCount, len(g.Cases))
	}
	return &g, &m
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex %q: %v", s, err)
	}
	return b
}

// TestJavaGoldenRecords replays every oracle case through the dispatcher's
// worker logic and compares the outcome (sent/filtered stage/not sent on Java
// exception), topic, partition, key, headers, value bytes and decoded profile
// step bytes. A JSON key-order mismatch is reported separately from a semantic
// mismatch so neither is hidden by normalization.
func TestJavaGoldenRecords(t *testing.T) {
	g, m := loadGolden(t)
	loc, err := time.LoadLocation(m.Timezone)
	if err != nil {
		t.Fatal(err)
	}
	// The oracle ran on a day other than the event date; any such day works.
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, loc)
	today := scoutertime.Date(now.UnixMilli(), loc)
	byProfile := map[string][]goldenCase{}
	for _, c := range g.Cases {
		byProfile[c.ConfigProfile] = append(byProfile[c.ConfigProfile], c)
	}
	profiles := make([]string, 0, len(byProfile))
	for p := range byProfile {
		profiles = append(profiles, p)
	}
	sort.Strings(profiles)
	var passed, failed int
	for _, profile := range profiles {
		cases := byProfile[profile]
		sort.Slice(cases, func(i, j int) bool { return cases[i].Sequence < cases[j].Sequence })
		pm, ok := m.Profiles[profile]
		if !ok {
			t.Fatalf("profile %q missing from metadata", profile)
		}
		cfg, err := ParseJavaConfig(pm.Conf)
		if err != nil {
			t.Fatal(err)
		}
		dict := newDict()
		sink := &memSink{}
		disp, err := New(Options{Config: cfg, Sink: sink, Lookup: dict.lookup, Location: loc, Clock: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			ok := t.Run(profile+"/"+c.ID, func(t *testing.T) {
				for _, e := range c.Input.Dictionary {
					dict.put("", e.Type, e.Hash, e.Text)
				}
				for _, e := range c.Input.FiledbDictionary {
					date := e.Date
					if date == "today" {
						date = today
					}
					dict.put(date, e.Type, e.Hash, e.Text)
				}
				before := len(sink.all())
				var q *queue
				var statsBefore QueueStats
				switch c.Kind {
				case "trace":
					var x goldenXLogFields
					if err := json.Unmarshal(c.Input.Pack.Fields, &x); err != nil {
						t.Fatal(err)
					}
					p := &protocol.XLogPack{EndTime: x.EndTime, ObjHash: x.ObjHash, Service: x.Service, TxID: x.TxID, ThreadNameHash: x.ThreadNameHash, Caller: x.Caller, GxID: x.GxID,
						Elapsed: x.Elapsed, Error: x.Error, CPU: x.CPU, SQLCount: x.SQLCount, SQLTime: x.SQLTime, KBytes: x.KBytes, Status: x.Status, APICallCount: x.APICallCount,
						APICallTime: x.APICallTime, XType: byte(x.XType), ProfileCount: x.ProfileCount, B3Mode: x.B3Mode, ProfileSize: x.ProfileSize, DiscardType: byte(x.DiscardType),
						IgnoreGlobalConsequentSampling: x.IgnoreGlobalConsequentSampling, TailVersion: 12}
					if x.IPAddrHex != nil {
						p.IPAddress = mustHex(t, *x.IPAddrHex)
					}
					q = disp.xlog
					statsBefore = q.stats.snapshot(0)
					err = disp.processXLog(q, p)
				case "profile":
					var x goldenProfileFields
					if err := json.Unmarshal(c.Input.Pack.Fields, &x); err != nil {
						t.Fatal(err)
					}
					p := &protocol.ProfilePack{Time: x.Time, ObjHash: x.ObjHash, Service: x.Service, TxID: x.TxID}
					if x.ProfileHex != nil {
						p.Profile = mustHex(t, *x.ProfileHex)
					}
					q = disp.profile
					statsBefore = q.stats.snapshot(0)
					err = disp.processProfile(q, p, x.Elapsed)
				default:
					t.Fatalf("unknown kind %q", c.Kind)
				}
				records := sink.all()[before:]
				stats := q.stats.snapshot(0)
				if len(c.Result.Exception) > 0 && string(c.Result.Exception) != "null" {
					// Java task failure: nothing sent. Go reports it as an explicit
					// filtered (application extraction) or worker error outcome.
					if len(records) != 0 {
						t.Fatalf("Java threw %s but Go sent a record", c.Result.Exception)
					}
					if err == nil && stats.Filtered == statsBefore.Filtered {
						t.Fatalf("Java threw %s but Go neither filtered nor failed", c.Result.Exception)
					}
					return
				}
				if err != nil {
					t.Fatalf("worker error: %v", err)
				}
				if c.Result.FilteredStage != nil {
					if len(records) != 0 || stats.Filtered-statsBefore.Filtered != 1 {
						t.Fatalf("expected filtered at %s; records=%d filtered delta=%d", *c.Result.FilteredStage, len(records), stats.Filtered-statsBefore.Filtered)
					}
					for stage := FilterStage(0); stage < filterStageCount; stage++ {
						if stats.FilteredByStage[stage] != statsBefore.FilteredByStage[stage] && stage.String() != *c.Result.FilteredStage {
							t.Fatalf("filtered at %s, Java filtered at %s", stage, *c.Result.FilteredStage)
						}
					}
					return
				}
				if !c.Result.Sent || c.Result.Record == nil {
					t.Fatalf("unexpected result shape: %+v", c.Result)
				}
				if len(records) != 1 {
					t.Fatalf("expected one record, got %d (stats %+v)", len(records), stats)
				}
				r, want := records[0], c.Result.Record
				if r.Topic != want.Topic || r.Partition != want.Partition {
					t.Errorf("topic/partition %s/%d want %s/%d", r.Topic, r.Partition, want.Topic, want.Partition)
				}
				if !bytes.Equal(r.Key, mustHex(t, want.KeyHex)) {
					t.Errorf("key %x want %s (txid %d)", r.Key, want.KeyHex, want.Key)
				}
				if len(r.Headers) != len(want.Headers) {
					t.Fatalf("headers %+v want %+v", r.Headers, want.Headers)
				}
				for i, h := range want.Headers {
					if r.Headers[i].Key != h.Name || !bytes.Equal(r.Headers[i].Value, mustHex(t, h.ValueHex)) {
						t.Errorf("header %d %s=%x want %s=%s", i, r.Headers[i].Key, r.Headers[i].Value, h.Name, h.ValueHex)
					}
				}
				if string(r.Headers[2].Value) != want.Application {
					t.Errorf("application %q want %q", r.Headers[2].Value, want.Application)
				}
				wantValue := mustHex(t, want.ValueHex)
				if !bytes.Equal(r.Value, wantValue) {
					var gotAny, wantAny any
					if json.Unmarshal(r.Value, &gotAny) == nil && json.Unmarshal(wantValue, &wantAny) == nil && fmt.Sprint(gotAny) == fmt.Sprint(wantAny) {
						t.Errorf("JSON key order differs (semantic match)\n got  %s\n want %s", r.Value, wantValue)
					} else {
						t.Errorf("JSON bytes differ\n got  %s\n want %s", r.Value, wantValue)
					}
				}
				if c.Kind == "profile" && want.ProfileSteps != nil {
					var decoded struct {
						Profile []struct {
							MainValue *string  `json:"mainValue"`
							Addl      []string `json:"additionalValueList"`
							Step      string   `json:"step"`
						} `json:"profile"`
					}
					if err := json.Unmarshal(r.Value, &decoded); err != nil {
						t.Fatal(err)
					}
					if len(decoded.Profile) != len(want.ProfileSteps) {
						t.Fatalf("%d steps want %d", len(decoded.Profile), len(want.ProfileSteps))
					}
					for i, s := range want.ProfileSteps {
						got, err := base64.StdEncoding.DecodeString(decoded.Profile[i].Step)
						if err != nil || !bytes.Equal(got, mustHex(t, s.StepHex)) {
							t.Errorf("step %d bytes %x want %s (%v)", i, got, s.StepHex, err)
						}
						if (s.MainValue == nil) != (decoded.Profile[i].MainValue == nil) || (s.MainValue != nil && *s.MainValue != *decoded.Profile[i].MainValue) {
							t.Errorf("step %d mainValue %v want %v", i, decoded.Profile[i].MainValue, s.MainValue)
						}
						if fmt.Sprint(s.Addl) != fmt.Sprint(decoded.Profile[i].Addl) {
							t.Errorf("step %d additionalValueList %v want %v", i, decoded.Profile[i].Addl, s.Addl)
						}
					}
				}
			})
			if ok {
				passed++
			} else {
				failed++
			}
		}
		if err := disp.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("golden cases: %d passed, %d failed (plugin %s)", passed, failed, g.PluginSourceDigest)
}
