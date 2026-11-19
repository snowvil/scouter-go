package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/transport"
)

// A Java Agent sends its 5-minute SummaryPack with up to 5000 rows
// (_summary_service_max_count) in six ListValue columns, as UDP fragments.
// It must be received, stored and read back whole: the default collection
// limits (4096 entries, 16384 Values) refused it at the receiver.
func TestAgentSummaryPackOfDefaultMaxRowsIsStored(t *testing.T) {
	const rows = 5000
	now := time.Now().UnixMilli() / 300000 * 300000
	table := protocol.Value{Type: protocol.ValueMap}
	for _, name := range []string{"id", "count", "error", "elapsed", "cpu", "mem"} {
		list := protocol.Value{Type: protocol.ValueList, Values: make([]protocol.Value, rows)}
		for n := range list.Values {
			list.Values[n] = protocol.Value{Type: protocol.ValueDecimal, Int: int64(n)*7919 + int64(len(name))}
		}
		table.Entries = append(table.Entries, protocol.Entry{Key: name, Value: list})
	}
	pack := &protocol.SummaryPack{Time: now, ObjHash: 4242, ObjType: "tomcat", SType: 1, Table: table}
	if _, err := protocol.EncodePack(pack, protocol.Limits{}); !errors.Is(err, protocol.ErrLimit) {
		t.Fatal("the pack must exceed the default limits for this test to mean anything", err)
	}
	payload, err := protocol.EncodePack(pack, protocol.AgentPackLimits)
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready.json")
	r, err := Open(Config{Directory: root, Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", HTTP: "127.0.0.1:0", ReadyFile: ready, Username: "admin", Password: "admin", CounterXML: []byte("<Counters/>")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	b, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints Endpoints
	if err = json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("udp", endpoints.UDP)
	if err != nil {
		t.Fatal(err)
	}
	// DataUdpAgent.writeMTU: magic, objHash, pkid, total, num, blob.
	const part = 1400
	total := (len(payload) + part - 1) / part
	for n := 0; n < total; n++ {
		chunk := payload[n*part : min((n+1)*part, len(payload))]
		e := protocol.NewEncoder(protocol.Limits{})
		e.Int32(int32(transport.UDPCafeFragment))
		e.Int32(pack.ObjHash)
		e.Int64(99)
		e.Int16(int16(total))
		e.Int16(int16(n))
		e.Blob(chunk)
		datagram, err := e.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint32(datagram) != transport.UDPCafeFragment {
			t.Fatal("fragment header")
		}
		if _, err = conn.Write(datagram); err != nil {
			t.Fatal(err)
		}
		if n%32 == 31 {
			time.Sleep(time.Millisecond) // keep the loopback socket buffer from overflowing
		}
	}
	conn.Close()

	date := storage.JavaDate(now, time.UTC)
	var got []*protocol.SummaryPack
	deadline := time.Now().Add(10 * time.Second)
	for len(got) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if err := r.engine.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		got = got[:0]
		err := r.engine.ScanSummaries(ctx, date, now, now, 1, func(p *protocol.SummaryPack) (bool, error) {
			got = append(got, p)
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	udp := r.udp.Stats()
	if len(got) != 1 || !reflect.DeepEqual(got[0], pack) {
		t.Fatalf("stored %d summary packs (udp %+v, ingest %+v)", len(got), udp, r.engine.Stats())
	}
	if udp.Malformed != 0 || udp.Rejected != 0 || r.engine.Stats().Rejected != 0 {
		t.Fatalf("udp %+v ingest %+v", udp, r.engine.Stats())
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
