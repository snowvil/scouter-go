package pulse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

type dictKey struct {
	date, div string
	hash      int32
}

// memDict is a synthetic dictionary keyed by Java date; "" date entries match
// any date (perm text types). It records every lookup for order/date checks.
type memDict struct {
	mu      sync.Mutex
	entries map[dictKey]string
	calls   []dictKey
	err     error
}

func newDict() *memDict { return &memDict{entries: map[dictKey]string{}} }
func (d *memDict) put(date, div string, hash int32, text string) {
	d.entries[dictKey{date, div, hash}] = text
}
func (d *memDict) lookup(ctx context.Context, date, div string, hash int32) (string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	d.calls = append(d.calls, dictKey{date, div, hash})
	if d.err != nil {
		return "", false, d.err
	}
	if v, ok := d.entries[dictKey{date, div, hash}]; ok {
		return v, true, nil
	}
	v, ok := d.entries[dictKey{"", div, hash}]
	return v, ok, nil
}

var seoul = time.FixedZone("KST", 9*3600)

func TestConvertXLogAndJSON(t *testing.T) {
	// 2026-10-13T23:30:00+09:00 is 2026-10-13 in Seoul and 2026-10-13T14:30Z.
	end := time.Date(2026, 10, 13, 23, 30, 0, 0, seoul).UnixMilli()
	d := newDict()
	d.put("20261013", TextObject, 100, "/host/app@1")
	d.put("20261013", TextService, 200, "/api/v1/users")
	d.put("20261013", TextHashMessage, 300, "http-nio-8080-exec-1")
	d.put("20261013", TextError, 400, "java.lang.RuntimeException: \"boom\"")
	p := &protocol.XLogPack{EndTime: end, ObjHash: 100, Service: 200, TxID: -9223372036854775808, ThreadNameHash: 300, Caller: 7, GxID: 9223372036854775807,
		Elapsed: 12, Error: 400, CPU: 3, SQLCount: 4, SQLTime: 5, IPAddress: []byte{10, 0, 0, 1}, KBytes: 6, Status: 500, APICallCount: 8, APICallTime: 9,
		XType: 0xFF, ProfileCount: 11, B3Mode: true, ProfileSize: 13, DiscardType: 0x80, IgnoreGlobalConsequentSampling: true, TailVersion: 12}
	tr, err := ConvertXLog(context.Background(), d.lookup, seoul, p)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := tr.MarshalJSON()
	want := `{"traceId":-9223372036854775808,"traceEndTime":` + fmt.Sprint(end) + `,"objectDictId":100,"transactionDictId":200,"threadDictId":300,"parentTraceId":7,"globalTraceId":9223372036854775807,` +
		`"elapsed":12,"errorDictId":400,"cpu":3,"sqlCount":4,"sqlTime":5,"ipaddr":"CgAAAQ==","kbytes":6,"apicallCount":8,"apicallTime":9,"profileCount":11,"b3Mode":true,"profileSize":13,"discardType":-128,"ignoreGlobalConsequentSampling":true,` +
		`"objectDictContent":"/host/app@1","transactionDictContent":"/api/v1/users","threadDictContent":"http-nio-8080-exec-1","errorDictContent":"java.lang.RuntimeException: \"boom\"","xtype":-1}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	// Lookups use the event date in the configured zone, in the plugin order.
	wantCalls := []dictKey{{"20261013", TextObject, 100}, {"20261013", TextService, 200}, {"20261013", TextHashMessage, 300}, {"20261013", TextError, 400}}
	if fmt.Sprint(d.calls) != fmt.Sprint(wantCalls) {
		t.Fatalf("calls %v", d.calls)
	}
	// Same instant in UTC is still 2026-10-13; one hour later in Seoul is the 14th.
	utcDate, err := ConvertXLog(context.Background(), d.lookup, time.UTC, p)
	if err != nil || utcDate.ObjectDictContent == nil {
		t.Fatal("UTC date lookup", err)
	}
	p2 := *p
	p2.EndTime = end + 3600000
	next, err := ConvertXLog(context.Background(), d.lookup, seoul, &p2)
	if err != nil || next.ObjectDictContent != nil {
		t.Fatal("cross-midnight lookup must miss the 13th dictionary", err)
	}
	// Zero/false/null fields are written, not omitted; nil ipaddr is null and
	// errorDictContent is null without a lookup when error == 0.
	d.calls = nil
	zero := &protocol.XLogPack{EndTime: end, ObjHash: 100, TailVersion: 12}
	tr, err = ConvertXLog(context.Background(), d.lookup, seoul, zero)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = tr.MarshalJSON()
	want = `{"traceId":0,"traceEndTime":` + fmt.Sprint(end) + `,"objectDictId":100,"transactionDictId":0,"threadDictId":0,"parentTraceId":0,"globalTraceId":0,"elapsed":0,"errorDictId":0,"cpu":0,"sqlCount":0,"sqlTime":0,"ipaddr":null,"kbytes":0,"apicallCount":0,"apicallTime":0,"profileCount":0,"b3Mode":false,"profileSize":0,"discardType":0,"ignoreGlobalConsequentSampling":false,"objectDictContent":"/host/app@1","transactionDictContent":null,"threadDictContent":null,"errorDictContent":null,"xtype":0}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if len(d.calls) != 3 {
		t.Fatal("error lookup must be skipped for error==0", d.calls)
	}
	// Wire-decoded zero-length Blob is Java's new byte[0], serialized as "".
	empty := &protocol.XLogPack{EndTime: end, IPAddress: []byte{}, TailVersion: 12}
	tr, _ = ConvertXLog(context.Background(), d.lookup, seoul, empty)
	if got, _ := tr.MarshalJSON(); !containsBytes(got, `"ipaddr":""`) {
		t.Fatal(string(got))
	}
	// A pack mutated after conversion must not alter the trace (ipaddr clone).
	p.IPAddress[0] = 99
	if tr2, _ := ConvertXLog(context.Background(), d.lookup, seoul, p); tr2.IPAddress[0] != 99 {
		t.Fatal("conversion reads current pack")
	}
	p.IPAddress[0] = 10
	tr3, _ := ConvertXLog(context.Background(), d.lookup, seoul, p)
	p.IPAddress[0] = 99
	if tr3.IPAddress[0] != 10 {
		t.Fatal("ipaddr not cloned")
	}
	d.err = fmt.Errorf("disk")
	if _, err := ConvertXLog(context.Background(), d.lookup, seoul, p); err == nil {
		t.Fatal("lookup error must fail the conversion")
	}
}

func containsBytes(b []byte, s string) bool { return strings.Contains(string(b), s) }
