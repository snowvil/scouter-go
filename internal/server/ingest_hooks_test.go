package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/geoip"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

type hookGeo struct{}

func (hookGeo) Lookup(ip [4]byte) (geoip.Location, bool, error) {
	if ip == [4]byte{8, 8, 8, 8} {
		return geoip.Location{CountryCode: "US", City: "Mountain View"}, true, nil
	}
	return geoip.Location{}, false, nil
}

func TestFormatServerStat(t *testing.T) {
	entries := []ServerStatEntry{{"agentcall.cpu-dump.queue", 3}, {"pulse.xlog.queue", 0}}
	if got := FormatServerStat(entries, "", ""); got != "agentcall.cpu-dump.queue=3\npulse.xlog.queue=0\n" {
		t.Fatalf("%q", got)
	}
	if got := FormatServerStat(entries, "pulse", "> "); got != "> pulse.xlog.queue=0\n" {
		t.Fatalf("%q", got)
	}
	if got := FormatServerStat(nil, "", ""); got != "" {
		t.Fatalf("%q", got)
	}
}

// The ingest hooks over the real listeners: the kube seq push and the CPU
// thread-dump trigger reach a connected Agent through the agent pool, GeoIP
// runs on the XLog path, and ServerStat lists the Go queues.
func TestRuntimeIngestAgentHooks(t *testing.T) {
	counterXML, err := os.ReadFile(filepath.Join("..", "..", "cmd", "scouter-go", "counters.xml"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready.json")
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", ReadyFile: ready,
		Username: "admin", Password: "admin", CounterXML: counterXML, GeoIP: hookGeo{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	b, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err = json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}
	kubeHash, wasHash := storage.CounterHash("/k8s/pod-1"), storage.CounterHash("/was")
	kubeAgent := dialRuntimeAgent(t, r, endpoints.TCP, kubeHash)
	wasAgent := dialRuntimeAgent(t, r, endpoints.TCP, wasHash)
	udp, err := net.Dial("udp", endpoints.UDP)
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	send := func(p protocol.Pack) {
		t.Helper()
		payload, err := protocol.EncodePack(p, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = udp.Write(append([]byte("CAFE"), payload...)); err != nil {
			t.Fatal(err)
		}
	}
	text := func(s string) protocol.Value { return protocol.Value{Type: protocol.ValueText, Text: s} }
	send(&protocol.ObjectPack{ObjName: "/k8s/pod-1", ObjType: "tomcat", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "podName", Value: text("app")}, {Key: "hostName", Value: text("pod-1")},
		{Key: "useKubeSeq", Value: protocol.Value{Type: protocol.ValueBoolean, Bool: true}},
		{Key: "kubeSeq", Value: protocol.Value{Type: protocol.ValueDecimal, Int: -1}}}}})
	cmd, param := kubeAgent.readRequest(t)
	if cmd != "OBJECT_SET_KUBE_SEQ" || len(param.Entries) != 1 || param.Entries[0].Key != "seq" || param.Entries[0].Value.Int != 0 {
		t.Fatalf("kube push %s %+v", cmd, param)
	}
	kubeAgent.packResponse(t, 1)

	send(&protocol.ObjectPack{ObjName: "/was", ObjType: "tomcat", Tags: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "ADC", Value: protocol.Value{Type: protocol.ValueBoolean, Bool: true}},
		{Key: "ADC_TH", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 50}},
		{Key: "ADC_D", Value: protocol.Value{Type: protocol.ValueDecimal, Int: -1}}}}})
	deadline := time.Now().Add(5 * time.Second)
	for {
		obj, err := r.engine.Object(ctx, "", wasHash)
		if err != nil {
			t.Fatal(err)
		}
		if obj != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("object not registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	send(&protocol.PerfCounterPack{ObjName: "/was", Time: time.Now().UnixMilli(), TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{
		{Key: "ProcCpu", Value: protocol.Value{Type: protocol.ValueFloat, Float: 75}}}}})
	cmd, param = wasAgent.readRequest(t)
	if cmd != "TRIGGER_THREAD_DUMPS_FROM_CONDITIONS" || len(param.Entries) != 2 || param.Entries[0].Key != "objHash" || param.Entries[0].Value.Int != int64(wasHash) ||
		param.Entries[1].Key != "TRIGGER_DUMP_REASON" || param.Entries[1].Value.Text != "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED" {
		t.Fatalf("dump trigger %s %+v", cmd, param)
	}
	wasAgent.packResponse(t, 2)

	end := time.Now().UnixMilli()
	send(&protocol.XLogPack{EndTime: end, ObjHash: wasHash, Service: 5, TxID: 99, Elapsed: 10, IPAddress: []byte{8, 8, 8, 8}, TailVersion: protocol.XLogCurrentTail})
	date := storage.JavaDate(end, time.UTC)
	for {
		x, err := r.engine.XLog(ctx, date, 99)
		if err == nil && x != nil {
			if x.CountryCode != "US" || x.City != storage.CounterHash("Mountain View") {
				t.Fatalf("geoip %q %d", x.CountryCode, x.City)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("xlog not stored", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		st := r.engine.Stats()
		// Completed covers the held kube object, the WAS object, the counter and
		// the XLog (whose tag writes may still run after it is readable).
		if st.KubeSeqCalls().Done == 1 && st.CPUDumpCalls().Done == 1 && st.KubeHeld == 1 && st.Completed+st.Rejected == 4 {
			if st.Rejected != 0 {
				t.Fatalf("rejected %+v", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent call stats %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stat := r.ServerStat()
	if len(stat) != 12 || stat[0].Name != "agentcall.cpu-dump.queue" || stat[1].Name != "agentcall.kube-seq.queue" || stat[2].Name != "ingest.writer.alert.queue" || stat[11].Name != "ingest.writer.visitor.queue" {
		t.Fatalf("server stat %+v", stat)
	}
}

func dialRuntimeAgent(t *testing.T, r *Runtime, address string, hash int32) *poolFakeAgent {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], transport.TCPAgentV1)
	binary.BigEndian.PutUint32(header[4:], uint32(hash))
	if _, err = conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.pool.mu.Lock()
		n := len(r.pool.agents[hash])
		r.pool.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent not pooled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &poolFakeAgent{conn: conn, reader: bufio.NewReader(conn)}
}

// geoip_data_city_file: a missing file keeps GeoIP off (S145), a later
// reload with a readable file turns it on (S146), and a reload to a missing
// file keeps the loaded database. The synthetic city_rev1.dat maps
// 192.168.0.1 to KR/Seoul, which GeoIpUtil does look up (signed byte quirk).
func TestRuntimeGeoIPFileReload(t *testing.T) {
	root := t.TempDir()
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", Username: "admin", Password: "admin",
		CounterXML: []byte("<Counters/>"), GeoIPFile: filepath.Join(root, "missing.dat")})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()
	txid := int64(0)
	accept := func() *protocol.XLogPack {
		t.Helper()
		txid++
		end := time.Now().UnixMilli()
		if err := r.engine.Accept(ctx, &protocol.XLogPack{EndTime: end, ObjHash: 1, Service: 5, TxID: txid, Elapsed: 1, IPAddress: []byte{192, 168, 0, 1}, TailVersion: protocol.XLogCurrentTail}); err != nil {
			t.Fatal(err)
		}
		if err := r.store.Sync(); err != nil { // the XLog writer stores it (XLogWR)
			t.Fatal(err)
		}
		x, err := r.engine.XLog(ctx, storage.JavaDate(end, time.UTC), txid)
		if err != nil || x == nil {
			t.Fatal(x, err)
		}
		return x
	}
	if x := accept(); x.CountryCode != "" {
		t.Fatal("missing file must keep GeoIP off")
	}
	r.ReloadGeoIP(filepath.Join("..", "geoip", "testdata", "oracle", "city_rev1.dat"))
	if x := accept(); x.CountryCode != "KR" || x.City != storage.CounterHash("Seoul") {
		t.Fatalf("loaded %q %d", x.CountryCode, x.City)
	}
	r.ReloadGeoIP(filepath.Join(root, "missing.dat"))
	if x := accept(); x.CountryCode != "KR" {
		t.Fatal("a failed reload keeps the previous database")
	}
}
