package kafka

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestJavaLZ4Oracle checks LZ4 interoperability with the Pulse plugin's Java
// client in both directions:
//
//   - RecordBatches and bare LZ4 frames written by this package are read by
//     kafka-clients 4.0.0 (MemoryRecords + ensureValid + the Java LZ4 codec)
//     and must yield the exact records/bytes.
//   - Frames and an lz4 RecordBatch written by kafka-clients are read by the
//     test decoders used for the round-trip tests, and lz4-java's XXH32 must
//     match xxhash32.
//
// It needs SCOUTER_KAFKA_JAVA_CP (a classpath with kafka-clients 4.0.0,
// lz4-java and slf4j-api jars) and a JDK (SCOUTER_JAVA_HOME, JAVA_HOME or
// PATH). Without them it is skipped; a skip is not a pass.
func TestJavaLZ4Oracle(t *testing.T) {
	cp := os.Getenv("SCOUTER_KAFKA_JAVA_CP")
	if cp == "" {
		t.Skip("SCOUTER_KAFKA_JAVA_CP not set")
	}
	javac, java := "javac", "java"
	for _, env := range []string{"SCOUTER_JAVA_HOME", "JAVA_HOME"} {
		if home := os.Getenv(env); home != "" {
			javac, java = filepath.Join(home, "bin", "javac"), filepath.Join(home, "bin", "java")
			break
		}
	}
	work := t.TempDir()
	classes := filepath.Join(work, "classes")
	goDir := filepath.Join(work, "go")
	javaDir := filepath.Join(work, "java")
	for _, d := range []string{classes, goDir, javaDir} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	run := func(name string, args ...string) []byte {
		t.Helper()
		cmd := exec.Command(name, args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, stderr.String())
		}
		return out
	}
	run(javac, "-encoding", "UTF-8", "-d", classes, "-cp", cp, filepath.Join("testdata", "LZ4Oracle.java"))
	runCP := cp + string(os.PathListSeparator) + classes

	// Go -> Java
	expected := map[string]string{}
	var lz lz4Compressor
	for name, recs := range javaOracleBatches() {
		b := newBatch(topicPartition{"t", 0}, time.UnixMilli(1_700_000_000_000))
		for i, rec := range recs {
			ts := int64(1_700_000_000_000 + i*3)
			b.appendRecord(encodeRecord(nil, rec, ts-1_700_000_000_000, int32(i)), ts)
		}
		var comp *lz4Compressor
		if !strings.HasSuffix(name, "-none") {
			comp = &lz
		}
		wire := b.appendEncoded(nil, comp)
		codec := "none"
		if binary.BigEndian.Uint16(wire[21:])&7 == uint16(batchCodecLZ4) {
			codec = "lz4"
		}
		if want := strings.HasSuffix(name, "-lz4"); want != (codec == "lz4") {
			t.Fatalf("%s: codec %s", name, codec)
		}
		if err := os.WriteFile(filepath.Join(goDir, name+".batch"), wire, 0o644); err != nil {
			t.Fatal(err)
		}
		expected[name+".batch"] = javaBatchJSON(name+".batch", codec, recs, 1_700_000_000_000)
	}
	for name, in := range lz4Inputs() {
		frame := lz.appendLZ4Frame(nil, in)
		if err := os.WriteFile(filepath.Join(goDir, name+".frame"), frame, 0o644); err != nil {
			t.Fatal(err)
		}
		expected[name+".frame"] = fmt.Sprintf(`{"file":%q,"plain":%q}`, name+".frame", hex.EncodeToString(in))
	}
	out := run(java, "-cp", runCP, "LZ4Oracle", "decode", goDir)
	seen := 0
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var head struct{ File string }
		if err := json.Unmarshal(sc.Bytes(), &head); err != nil {
			t.Fatalf("java output %q: %v", sc.Text(), err)
		}
		want, ok := expected[head.File]
		if !ok {
			t.Fatalf("unexpected java output for %s", head.File)
		}
		if got := sc.Text(); got != want {
			t.Fatalf("%s:\n java %s\n want %s", head.File, truncate(got), truncate(want))
		}
		seen++
	}
	if seen != len(expected) {
		t.Fatalf("java decoded %d of %d files", seen, len(expected))
	}
	t.Logf("java decoded %d Go batches/frames identically", seen)

	// Java -> Go
	run(java, "-cp", runCP, "LZ4Oracle", "encode", javaDir)
	for _, name := range []string{"empty", "hello", "json", "random"} {
		in, err := os.ReadFile(filepath.Join(javaDir, name+".in"))
		if err != nil {
			t.Fatal(err)
		}
		jframe, err := os.ReadFile(filepath.Join(javaDir, name+".jframe"))
		if err != nil {
			t.Fatal(err)
		}
		plain, err := lz4DecodeFrame(jframe)
		if err != nil || !bytes.Equal(plain, in) {
			t.Fatalf("%s: java frame decode %v", name, err)
		}
		ours := lz.appendLZ4Frame(nil, in)
		if !bytes.Equal(jframe[:lz4FrameHeaderLen], ours[:lz4FrameHeaderLen]) {
			t.Fatalf("%s: frame header java %x go %x", name, jframe[:7], ours[:7])
		}
		// no-match inputs give the same bytes from any LZ4 encoder
		if (name == "empty" || name == "hello") && !bytes.Equal(jframe, ours) {
			t.Fatalf("%s: java %x go %x", name, jframe, ours)
		}
		t.Logf("%s: %d bytes, java frame %d, go frame %d", name, len(in), len(jframe), len(ours))
	}
	jb, err := os.ReadFile(filepath.Join(javaDir, "java.batch"))
	if err != nil {
		t.Fatal(err)
	}
	meta, recs, err := decodeBatch(jb)
	if err != nil {
		t.Fatalf("java lz4 batch: %v", err)
	}
	if meta.attributes&7 != batchCodecLZ4 || len(recs) != 20 || !recs[3].valueNull || recs[0].headers[0].Key != "apm_type" || !recs[0].headers[1].null || recs[19].timestamp != 1019 {
		t.Fatalf("java batch meta %+v records %d", meta, len(recs))
	}
	vectors, err := os.ReadFile(filepath.Join(javaDir, "xxh32.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimRight(string(vectors), "\n"), "\n") {
		hexIn, sum, _ := strings.Cut(line, " ")
		in, _ := hex.DecodeString(hexIn)
		want, _ := strconv.ParseUint(sum, 10, 32)
		if got := xxhash32(in, 0); got != uint32(want) {
			t.Fatalf("xxh32(%x) go %08x java %08x", in, got, want)
		}
	}
}

func truncate(s string) string {
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}

// javaOracleBatches names each case with the codec the batch must end up with.
func javaOracleBatches() map[string][]Record {
	r := rand.New(rand.NewSource(11))
	pulse := make([]Record, 0, 60)
	for i := 0; i < 60; i++ {
		key := binary.BigEndian.AppendUint64(nil, uint64(int64(-1_234_567_890_123)+int64(i)*7_777_777_777))
		pulse = append(pulse, Record{Topic: "t", Key: key, Value: []byte(fmt.Sprintf(`{"traceId":%d,"seq":%d,"text":"한글 \"quote\" \\ /","service":"/api/v1/orders/{id}"}`, i*31, i)), Headers: []Header{
			{Key: "apm_type", Value: []byte("SCOUTER")},
			{Key: "xlog_serializer_version", Value: []byte("V1")},
			{Key: "xlog_application", Value: []byte(fmt.Sprintf("app-%d", i%4))},
		}})
	}
	var multi []Record
	for i := 0; i < 8; i++ {
		v := []byte(strings.Repeat(fmt.Sprintf("profile-step-%d;", i), 2500)) // ~40KB each
		if i == 3 {
			v = make([]byte, 70_000) // a raw (incompressible) block inside a compressed frame
			r.Read(v)
		}
		multi = append(multi, Record{Topic: "t", Key: []byte{byte(i)}, Value: v})
	}
	noise := make([]Record, 3)
	for i := range noise {
		v := make([]byte, 3000)
		r.Read(v)
		noise[i] = Record{Topic: "t", Value: v}
	}
	nulls := []Record{
		{Topic: "t", Key: nil, Value: nil, Headers: []Header{{Key: "h", Value: nil}}},
		{Topic: "t", Key: []byte{}, Value: []byte{}, Headers: []Header{{Key: "", Value: []byte{}}}},
		{Topic: "t", Key: []byte("k"), Value: []byte(strings.Repeat("v", 100))},
	}
	return map[string][]Record{
		"pulse-lz4":           pulse,
		"multiblock-lz4":      multi,
		"incompressible-none": noise,
		"nulls-lz4":           nulls,
		"pulse-none":          pulse[:5],
	}
}

func javaBatchJSON(file, codec string, recs []Record, base int64) string {
	q := func(b []byte) string {
		if b == nil {
			return "null"
		}
		return `"` + hex.EncodeToString(b) + `"`
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"file":%q,"compression":%q,"count":%d,"records":[`, file, codec, len(recs))
	for i, rec := range recs {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"offset":%d,"timestamp":%d,"key":%s,"value":%s,"headers":[`, i, base+int64(i*3), q(rec.Key), q(rec.Value))
		for j, h := range rec.Headers {
			if j > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"key":"%x","value":%s}`, h.Key, q(h.Value))
		}
		sb.WriteString("]}")
	}
	sb.WriteString("]}")
	return sb.String()
}
