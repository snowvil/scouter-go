package service

import (
	"bytes"
	"context"
	"errors"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func summaryTestTable(values map[string][]protocol.Value) protocol.Value {
	entries := []protocol.Entry{}
	for k, v := range values {
		entries = append(entries, entry(k, list(v)))
	}
	return protocol.Value{Type: protocol.ValueMap, Entries: entries}
}
func TestSummaryAppAggregateAndEmptyTypeSelector(t *testing.T) {
	record := func(kind byte, typ string, count int64) *protocol.SummaryPack {
		return &protocol.SummaryPack{SType: kind, ObjHash: 42, ObjType: typ, Table: summaryTestTable(map[string][]protocol.Value{"id": {decimal(9)}, "count": {decimal(count)}, "error": {decimal(1)}, "elapsed": {decimal(100)}, "cpu": {decimal(2)}, "mem": {decimal(3)}})}
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: []*protocol.SummaryPack{record(1, "host", 2), record(1, "host", 3), record(2, "host", 9)}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "LOAD_SERVICE_SUMMARY", 1, newMap(entry("date", text("20260101")), entry("objType", text(""))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if lookup(p, "count").Values[0].Int != 5 || lookup(p, "elapsed").Values[0].Int != 200 || lookup(p, "cpu").Values[0].Int != 4 || len(p.Entries) != 6 {
		t.Fatalf("APP %#v", p)
	}
	send(t, c, "LOAD_IP_SUMMARY", 1, newMap(entry("date", text("20260101")), entry("objType", text(""))))
	p = readPacks(t, r)[0].(*protocol.MapPack)
	if len(lookup(p, "id").Values) != 0 || len(p.Entries) != 2 {
		t.Fatal("IP empty text is exact selector")
	}
}
func TestSummaryEndUserLongIDReplacementAndHostQuirk(t *testing.T) {
	id := int64(1<<32) + 7
	records := []*protocol.SummaryPack{}
	for _, count := range []int64{2, 3} {
		records = append(records, &protocol.SummaryPack{SType: 12, ObjType: "host", Table: summaryTestTable(map[string][]protocol.Value{"id": {decimal(id)}, "host": {decimal(99)}, "stacktrace": {decimal(44)}, "userAgent": {decimal(1)}, "count": {decimal(count)}, "uri": {decimal(2)}, "message": {decimal(3)}, "file": {decimal(4)}, "lineNumber": {decimal(5)}, "columnNumber": {decimal(6)}})})
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: records}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "LOAD_ENDUSER_ERROR_SUMMARY", 1, newMap(entry("date", text("20260101")), entry("objType", text("host")), entry("objHash", decimal(999))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if lookup(p, "host").Values[0].Int != 44 || lookup(p, "id").Values[0].Int != id || lookup(p, "count").Type != protocol.ValueNull {
		t.Fatalf("original quirks %#v", p)
	}
}

func TestSummaryAlertSignedByteAndMinuteOverwrite(t *testing.T) {
	record := func(count, level int64) *protocol.SummaryPack {
		return &protocol.SummaryPack{Time: 1767225600000, SType: 3, Table: summaryTestTable(map[string][]protocol.Value{"title": {text("same")}, "count": {decimal(count)}, "level": {decimal(level)}})}
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: []*protocol.SummaryPack{record(2, 255), record(3, 1)}}
	_, c, r, _ := startPacketQuery(t, b)
	p := newMap(entry("date", text("20260101")))
	send(t, c, "LOAD_ALERT_SUMMARY", 1, p)
	got := readPacks(t, r)[0].(*protocol.MapPack)
	if lookup(got, "count").Values[0].Int != 5 || lookup(got, "level").Values[0].Int != -1 {
		t.Fatal("count sum and first signed byte level")
	}
	send(t, c, "ALERT_TITLE_COUNT", 1, p)
	got = readPacks(t, r)[0].(*protocol.MapPack)
	counts := lookup(got, "count")
	if len(counts.Entries) != 1 || counts.Entries[0].Key != "0000" || counts.Entries[0].Value.Int != 3 || lookup(got, "level").Int != -1 {
		t.Fatal("minute overwrite and first level")
	}
}

func TestSummaryErrorNonzeroAndOverflow(t *testing.T) {
	record := func(count, message int64) *protocol.SummaryPack {
		return &protocol.SummaryPack{SType: 9, ObjHash: 42, Table: summaryTestTable(map[string][]protocol.Value{"id": {decimal(7)}, "error": {decimal(8)}, "service": {decimal(9)}, "txid": {decimal(1 << 40)}, "count": {decimal(count)}, "message": {decimal(message)}, "sql": {decimal(0)}, "apicall": {decimal(0)}, "fullstack": {decimal(0)}})}
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: []*protocol.SummaryPack{record(2147483647, 0), record(1, 55), record(0, 66)}}
	_, c, r, _ := startPacketQuery(t, b)
	send(t, c, "LOAD_SERVICE_ERROR_SUMMARY", 1, newMap(entry("date", text("20260101")), entry("objHash", decimal(42))))
	p := readPacks(t, r)[0].(*protocol.MapPack)
	if lookup(p, "count").Values[0].Int != -2147483648 || lookup(p, "message").Values[0].Int != 55 || lookup(p, "txid").Values[0].Int != 1<<40 {
		t.Fatal("int32 overflow, first nonzero, long txid")
	}
}

func TestSummaryAllCommandsEmptyColumnsAndMalformedLists(t *testing.T) {
	for _, cmd := range PacketCommands() {
		schema, ok := summaryCommandSchema(cmd)
		if !ok {
			continue
		}
		t.Run(cmd, func(t *testing.T) {
			b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}}
			_, c, r, _ := startPacketQuery(t, b)
			send(t, c, cmd, 1, newMap(entry("date", text("20260101"))))
			ps := readPacks(t, r)
			if len(ps) != 1 {
				t.Fatal("one empty map")
			}
			p := ps[0].(*protocol.MapPack)
			if len(p.Entries) != len(schema.columns) {
				t.Fatal("empty columns")
			}
			for i, col := range schema.columns {
				if p.Entries[i].Key != col || p.Entries[i].Value.Type != protocol.ValueList || len(p.Entries[i].Value.Values) != 0 {
					t.Fatal("empty parallel lists in source order")
				}
			}
		})
	}
	b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}, summaries: []*protocol.SummaryPack{{SType: 4, Table: summaryTestTable(map[string][]protocol.Value{"id": {decimal(1)}, "count": {}})}}}
	_, c, _, results := startPacketQuery(t, b)
	send(t, c, "LOAD_IP_SUMMARY", 1, newMap(entry("date", text("20260101"))))
	if err := <-results; !errors.Is(err, transport.ErrMalformed) {
		t.Fatalf("parallel list truncation %v", err)
	}
}
func TestSummaryOriginalScalaRawBodies(t *testing.T) {
	if _, err := os.Stat(filepath.Join(scouterSource(), "scouter.server")); err != nil {
		t.Skip("pinned source unavailable; Summary oracle not run")
	}
	if _, err := exec.LookPath("java"); err != nil {
		t.Skip("JDK unavailable; Summary oracle not run")
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "python3", "../../scripts/java-storage.py", "--source", scouterSource(), "--generate-counter-handlers", dir).CombinedOutput(); err != nil {
		t.Fatalf("original Scala oracle %v\n%s", err, out)
	}
	for _, tc := range []struct{ name, cmd string }{{"app", "LOAD_SERVICE_SUMMARY"}, {"ajax", "LOAD_ENDUSER_AJAX_SUMMARY"}, {"nav", "LOAD_ENDUSER_NAV_SUMMARY"}, {"error", "LOAD_ENDUSER_ERROR_SUMMARY"}} {
		t.Run(tc.name, func(t *testing.T) {
			b := &packetQueryBackend{extendedCounterBackend: &extendedCounterBackend{}}
			for i := 0; ; i++ {
				raw, err := os.ReadFile(filepath.Join(dir, "summary-"+tc.name+"-input-"+strconv.Itoa(i)+".bin"))
				if os.IsNotExist(err) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				p, err := protocol.DecodePack(raw, protocol.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				b.summaries = append(b.summaries, p.(*protocol.SummaryPack))
			}
			request, err := os.ReadFile(filepath.Join(dir, "summary-"+tc.name+"-request.bin"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(filepath.Join(dir, "summary-"+tc.name+"-response.bin"))
			if err != nil {
				t.Fatal(err)
			}
			_, c, r, _ := startPacketQuery(t, b)
			e := protocol.NewEncoder(protocol.Limits{})
			e.Text(tc.cmd)
			e.Int64(1)
			header, _ := e.Bytes()
			c.Write(append(header, request...))
			got := make([]byte, len(want))
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("actual Scala vs Go raw body\ngot %x\nwant %x", got, want)
			}
			if f, err := r.ReadByte(); err != nil || f != transport.NoNext {
				t.Fatalf("terminator %x %v", f, err)
			}
		})
	}
}
