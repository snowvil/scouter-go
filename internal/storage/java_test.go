package storage

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

const oracleTime int64 = 1767193200125

func TestJavaStorageInterop(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned Java source unavailable; DB oracle not run")
	}
	work := t.TempDir()
	classes := filepath.Join(work, "compiled")
	command := exec.Command("python3", filepath.Join(repo, "scripts", "java-storage.py"), "--classes-dir", classes, "--source", source)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java/Scala storage compilation: %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	javaDB := filepath.Join(work, "java-db")
	runOracle := func(mode, root string) {
		t.Helper()
		output, err := exec.Command("java", "-Duser.timezone=Asia/Seoul", "-cp", classpath, "StorageOracle", mode, root).CombinedOutput()
		if err != nil {
			t.Fatalf("Java storage %s: %v\n%s", mode, err, output)
		}
		t.Log(strings.TrimSpace(string(output)))
	}
	runOracle("generate", javaDB)
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"plain", "gzip"} {
		t.Run("Java-to-Go/"+mode, func(t *testing.T) { verifyJavaStore(t, filepath.Join(javaDB, mode), loc) })
	}
	goDB := filepath.Join(work, "go-db")
	for _, mode := range []string{"plain", "gzip"} {
		t.Run("Go-to-Java/"+mode, func(t *testing.T) {
			writeJavaStore(t, filepath.Join(goDB, mode), loc, mode == "gzip")
			verifyJavaStore(t, filepath.Join(goDB, mode), loc)
		})
	}
	runOracle("verify", goDB)
}
func verifyJavaStore(t *testing.T, root string, loc *time.Location) {
	t.Helper()
	s, err := Open(Config{Directory: root, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := OpenCounters(root, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	objects, err := s.Objects("20260101")
	if err != nil || len(objects) != 1 || objects[0].ObjHash != 42 || objects[0].ObjName != "oracle-last" || objects[0].Wakeup != oracleTime {
		t.Fatalf("object %v %v", objects, err)
	}
	for _, v := range []struct {
		div  string
		hash int32
		text string
	}{{"service", 7, "첫 서비스"}, {"error", 8, "합성 오류"}, {"error", 9, ""}} {
		text, ok, err := s.Text("20260101", v.div, v.hash)
		if err != nil || !ok || text != v.text {
			t.Fatal(v, text, ok, err)
		}
	}
	logs, err := s.XLogs("20260101", oracleTime, oracleTime+1000, false)
	if err != nil || len(logs) != 3 || logs[0].TxID != 102 || logs[1].TxID != 101 || logs[2].TxID != 103 {
		t.Fatalf("xlogs %v %v", logs, err)
	}
	reverse, err := s.XLogs("20260101", oracleTime, oracleTime+1000, true)
	if err != nil || len(reverse) != 3 || reverse[0].TxID != 103 || reverse[2].TxID != 102 {
		t.Fatal(reverse, err)
	}
	x, err := s.XLog("20260101", 101)
	if err != nil || x == nil || x.GxID != 9001 || x.EndTime != oracleTime+200 {
		t.Fatal(x, err)
	}
	chunks, err := s.ProfileChunks("20260101", 101)
	if err != nil || len(chunks) != 2 || !bytes.Equal(chunks[0], []byte{1, 2}) || !bytes.Equal(chunks[1], []byte{3, 4}) {
		t.Fatal(chunks, err)
	}
	hash := CounterHash("/oracle/app")
	for kind := byte(2); kind <= 6; kind++ {
		for _, prefix := range []string{"b", "f", "d", "n"} {
			values, err := c.Daily("20260101", hash, fmt.Sprintf("%s%d", prefix, kind), kind)
			if err != nil || len(values) == 0 {
				t.Fatal(prefix, kind, err)
			}
			v := values[0]
			switch prefix {
			case "b":
				if v.Type != 10 || !v.Bool {
					t.Fatal(v)
				}
			case "f":
				if v.Type != 30 || v.Float != 1.25 {
					t.Fatal(v)
				}
			case "d":
				if v.Type != 40 || v.Double != 2.5 {
					t.Fatal(v)
				}
			case "n":
				if v.Type != 20 || v.Int != math.MinInt64+1 {
					t.Fatal(v)
				}
			}
			if len(values) > 1 && values[1].Type != 0 {
				t.Fatal("sparse bucket", values[1])
			}
		}
	}
	for _, name := range []string{"cpu", "errors"} {
		points, err := c.Realtime("20260101", hash, name, oracleTime, oracleTime+500)
		if err != nil || len(points) != 2 {
			t.Fatal(points, err)
		}
		if points[0].Time != oracleTime || points[1].Time != oracleTime+500 {
			t.Fatal(points)
		}
		if name == "cpu" && (points[0].Value.Double != 12.5 || points[1].Value.Double != 13.5) {
			t.Fatal(points)
		}
		if name == "errors" && (points[0].Value.Int != 7 || points[1].Value.Int != 8) {
			t.Fatal(points)
		}
	}
}
func writeJavaStore(t *testing.T, root string, loc *time.Location, compressed bool) {
	t.Helper()
	s, err := Open(Config{Directory: root, Location: loc, CompressXLog: compressed, CompressProfile: compressed})
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	object := &protocol.ObjectPack{ObjHash: 42, ObjType: "java", ObjName: "oracle-first", Address: "127.0.0.1", Version: "oracle", Alive: true, Wakeup: oracleTime, Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "region", Value: protocol.Value{Type: protocol.ValueText, Text: "synthetic"}}}}}
	if err = s.PutObject("20260101", object); err != nil {
		t.Fatal(err)
	}
	object.ObjName = "oracle-last"
	if err = s.PutObject("20260101", object); err != nil {
		t.Fatal(err)
	}
	for _, v := range []*protocol.TextPack{{XType: "service", Hash: 7, Text: "첫 서비스"}, {XType: "error", Hash: 8, Text: "합성 오류"}, {XType: "error", Hash: 9, Text: ""}} {
		if err = s.PutText("20260101", v); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []struct{ tx, offset, gx int64 }{{101, 200, 9001}, {102, 100, 9001}, {103, 550, 0}} {
		if err = s.PutXLog(&protocol.XLogPack{TxID: v.tx, EndTime: oracleTime + v.offset, GxID: v.gx, ObjHash: 42, Service: 7, Elapsed: 10, TailVersion: 12}); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range [][]byte{{1, 2}, {3, 4}} {
		if err = s.PutProfile(&protocol.ProfilePack{Time: oracleTime, TxID: 101, Profile: b}); err != nil {
			t.Fatal(err)
		}
	}
	for kind := byte(2); kind <= 6; kind++ {
		entries := []protocol.Entry{{Key: fmt.Sprintf("b%d", kind), Value: protocol.Value{Type: 10, Bool: true}}, {Key: fmt.Sprintf("f%d", kind), Value: protocol.Value{Type: 30, Float: 1.25}}, {Key: fmt.Sprintf("d%d", kind), Value: protocol.Value{Type: 40, Double: 2.5}}, {Key: fmt.Sprintf("n%d", kind), Value: protocol.Value{Type: 20, Int: math.MinInt64 + 1}}}
		if err = c.Put(&protocol.PerfCounterPack{Time: oracleTime, ObjName: "/oracle/app", TimeType: kind, Data: protocol.Value{Type: 80, Entries: entries}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(0); i < 2; i++ {
		if err = c.Put(&protocol.PerfCounterPack{Time: oracleTime + i*500, ObjName: "/oracle/app", TimeType: 1, Data: protocol.Value{Type: 80, Entries: []protocol.Entry{{Key: "cpu", Value: protocol.Value{Type: 40, Double: 12.5 + float64(i)}}, {Key: "errors", Value: protocol.Value{Type: 20, Int: 7 + i}}}}}); err != nil {
			t.Fatal(err)
		}
	}
}
