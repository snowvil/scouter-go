package telegraf

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/golden_packs.json")

// goldenStep is one HTTP request body applied with the pinned sample
// configuration, no registrar, and a clock at start+OffsetMs.
type goldenStep struct {
	Name     string       `json:"name"`
	OffsetMs int64        `json:"offsetMs"`
	Remote   string       `json:"remote"`
	Body     string       `json:"body"`
	Packs    []goldenPack `json:"packs"`
}

// goldenPack is a stable JSON view of the packs handed to ingest. Float and
// double values are strconv 'g' strings of the exact float32/float64.
type goldenPack struct {
	Type     string        `json:"type"`
	Peer     string        `json:"peer"`
	ObjHash  int32         `json:"objHash,omitempty"`
	ObjName  string        `json:"objName"`
	ObjType  string        `json:"objType,omitempty"`
	Address  string        `json:"address,omitempty"`
	Time     int64         `json:"time,omitempty"`
	TimeType byte          `json:"timetype,omitempty"`
	Values   []goldenValue `json:"values"`
}

type goldenValue struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

const goldenStart = 1700000000000 // ms

func goldenValues(v protocol.Value) []goldenValue {
	out := []goldenValue{}
	for _, e := range v.Entries {
		g := goldenValue{Key: e.Key}
		switch e.Value.Type {
		case protocol.ValueDecimal:
			g.Type, g.Value = "decimal", strconv.FormatInt(e.Value.Int, 10)
		case protocol.ValueFloat:
			g.Type, g.Value = "float", strconv.FormatFloat(float64(e.Value.Float), 'g', -1, 32)
		case protocol.ValueDouble:
			g.Type, g.Value = "double", strconv.FormatFloat(e.Value.Double, 'g', -1, 64)
		default:
			g.Type = strconv.Itoa(int(e.Value.Type))
		}
		out = append(out, g)
	}
	return out
}

func runGolden(t *testing.T, steps []goldenStep) []goldenStep {
	clock := &fakeClock{time.UnixMilli(goldenStart)}
	cfg := sampleConfig(t, clock.t)
	acc := &fakeAcceptor{}
	in := NewInput(cfg, nil, acc, Options{Clock: clock.now, Logf: quietLogf})
	defer in.Close()
	out := make([]goldenStep, len(steps))
	for i, s := range steps {
		clock.t = time.UnixMilli(goldenStart + s.OffsetMs)
		if err := in.Handle(context.Background(), s.Body, s.Remote); err != nil {
			t.Fatal(err)
		}
		s.Packs = []goldenPack{}
		for _, a := range acc.take() {
			switch p := a.pack.(type) {
			case *protocol.ObjectPack:
				s.Packs = append(s.Packs, goldenPack{Type: "object", Peer: a.peer, ObjHash: p.ObjHash, ObjName: p.ObjName, ObjType: p.ObjType, Address: p.Address, Values: goldenValues(p.Tags)})
			case *protocol.PerfCounterPack:
				s.Packs = append(s.Packs, goldenPack{Type: "perf", Peer: a.peer, ObjName: p.ObjName, Time: p.Time, TimeType: p.TimeType, Values: goldenValues(p.Data)})
			}
		}
		out[i] = s
	}
	return out
}

// TestGoldenPacks replays testdata/golden_packs.json. The file is the
// reference for independent golden comparison (e.g. a Java oracle run of the
// same bodies); regenerate with go test -run TestGoldenPacks -update.
func TestGoldenPacks(t *testing.T) {
	path := filepath.Join("testdata", "golden_packs.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want []goldenStep
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	got := runGolden(t, want)
	encoded, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if *updateGolden {
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if !bytes.Equal(encoded, data) {
		t.Fatalf("golden mismatch; got:\n%s", encoded)
	}
}
