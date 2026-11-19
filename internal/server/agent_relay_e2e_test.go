package server

// End-to-end Client -> Runtime -> fake Agent relays of large answers.
// They rely on the Client response path without a per-command total and with
// transport.MaxResponseValueBytes (fix-stream).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

func e2eRuntime(t *testing.T) (*Runtime, string) {
	t.Helper()
	root := t.TempDir()
	return moStart(t, Config{Directory: filepath.Join(root, "db"), LogDirectory: filepath.Join(root, "logs"),
		PluginDirectory: filepath.Join(root, "plugin"), TelegrafConfigurationPath: filepath.Join(root, "conf", "scouter-telegraf.xml"),
		ConfDir: filepath.Join(root, "conf"), CounterXML: []byte("<Counters/>"), GeoIPFile: filepath.Join(root, "missing.dat")})
}

// e2eAgent registers object hash with the engine and connects a V1 agent.
func e2eAgent(t *testing.T, r *Runtime, addr string, hash int32) *poolFakeAgent {
	t.Helper()
	if err := r.engine.Accept(context.Background(), &protocol.ObjectPack{ObjName: "/e2e", ObjType: "tomcat", ObjHash: hash, Alive: true, Tags: protocol.Value{Type: protocol.ValueMap}}); err != nil {
		t.Fatal(err)
	}
	h := &poolTCPHarness{pool: &r.pool, address: addr}
	return h.dial(t, hash)
}

func e2eReadBlob(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var p [1]byte
	if _, err := io.ReadFull(r, p[:]); err != nil {
		t.Fatal(err)
	}
	size := int(p[0])
	switch p[0] {
	case 255:
		var b [2]byte
		io.ReadFull(r, b[:])
		size = int(binary.BigEndian.Uint16(b[:]))
	case 254:
		var b [4]byte
		io.ReadFull(r, b[:])
		size = int(binary.BigEndian.Uint32(b[:]))
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		t.Fatal(err)
	}
	return data
}

// MenuUtil heap dump download: 48MiB in 2MiB chunks reaches the Client intact.
func TestE2EHeapDumpDownloadOverEightMiB(t *testing.T) {
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, 4242)
	client := moLogin(t, addr)
	param, err := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{
		{Key: "objHash", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 4242}},
		{Key: "fileName", Value: protocol.Value{Type: protocol.ValueText, Text: "synthetic.hprof"}}}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	moSend(t, client.conn, "OBJECT_DOWNLOAD_HEAP_DUMP", client.session, param)
	client.conn.SetDeadline(time.Now().Add(60 * time.Second))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_DOWNLOAD_HEAP_DUMP" {
		t.Fatal(cmd)
	}
	want := sha256.New()
	go func() {
		for i := 0; i < 24; i++ {
			data := chunkData(i, 2<<20)
			want.Write(data)
			if _, err := agent.conn.Write(encodeBlobItem(t, data)); err != nil {
				return
			}
		}
		agent.conn.Write([]byte{transport.NoNext})
	}()
	got := sha256.New()
	total := 0
	for {
		flag, err := client.r.ReadByte()
		if err != nil {
			t.Fatalf("after %d bytes: %v", total, err)
		}
		if flag == transport.NoNext {
			break
		}
		if flag != transport.HasNext {
			t.Fatalf("flag %x", flag)
		}
		b := e2eReadBlob(t, client.r)
		total += len(b)
		got.Write(b)
	}
	if total != 48<<20 || !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatalf("Client received %d bytes", total)
	}
}

// ObjectClassListView jar export: a 20MiB jar in one MapPack.
func TestE2EDownloadJarOverFourMiB(t *testing.T) {
	r, addr := e2eRuntime(t)
	agent := e2eAgent(t, r, addr, 4243)
	client := moLogin(t, addr)
	param, err := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{
		{Key: "objHash", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 4243}},
		{Key: "resource", Value: protocol.Value{Type: protocol.ValueText, Text: "jar:file:/synthetic.jar!/A.class"}}}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	moSend(t, client.conn, "OBJECT_DOWNLOAD_JAR", client.session, param)
	client.conn.SetDeadline(time.Now().Add(60 * time.Second))
	if cmd, _ := agent.readRequest(t); cmd != "OBJECT_DOWNLOAD_JAR" {
		t.Fatal(cmd)
	}
	jar := chunkData(3, 20<<20)
	body, err := protocol.EncodePack(&protocol.MapPack{Entries: []protocol.Entry{{Key: "jar", Value: protocol.Value{Type: protocol.ValueBlob, Bytes: jar}}}}, relayCodec)
	if err != nil {
		t.Fatal(err)
	}
	go agent.conn.Write(append(append([]byte{transport.HasNext}, body...), transport.NoNext))
	flag, err := client.r.ReadByte()
	if err != nil || flag != transport.HasNext {
		t.Fatal(flag, err)
	}
	p, err := protocol.ReadPack(client.r, relayCodec)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := p.(*protocol.MapPack)
	if m == nil || !bytes.Equal(moMapGet(m, "jar").Bytes, jar) {
		t.Fatal("jar not intact")
	}
	if flag, err := client.r.ReadByte(); err != nil || flag != transport.NoNext {
		t.Fatal(flag, err)
	}
}
