package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func catalogCounters(t *testing.T) (*Counters, int64) {
	t.Helper()
	c, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
}
func catalogPut(t *testing.T, c *Counters, tm int64, name string, kind byte, entries ...protocol.Entry) {
	t.Helper()
	if err := c.PutContext(context.Background(), &protocol.PerfCounterPack{Time: tm, ObjName: name, TimeType: kind, Data: protocol.Value{Type: protocol.ValueMap, Entries: entries}}); err != nil {
		t.Fatal(err)
	}
}
func TestCounterCatalogRowsOrderEmptyOwnershipAndRestart(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	entry := func(n int64) protocol.Entry {
		return protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{{Type: protocol.ValueDecimal, Int: n}}}}
	}
	catalogPut(t, c, start+550, "/app", 1, entry(1))
	catalogPut(t, c, start+100, "/app", 1, entry(2))
	catalogPut(t, c, start+100, "/other", 1, entry(9))
	catalogPut(t, c, start+100, "/app", 1)
	catalogPut(t, c, start+100, "/app", 1, entry(3))
	root := c.root
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rows, err := c.CounterRowsContext(ctx, "20260101", CounterHash("/app"), start, start+1000)
	if err != nil || len(rows) != 4 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].Time != start+100 || rows[0].Values.Entries[0].Value.Values[0].Int != 2 || len(rows[1].Values.Entries) != 0 || rows[2].Values.Entries[0].Value.Values[0].Int != 3 || rows[3].Time != start+550 {
		t.Fatalf("order=%+v", rows)
	}
	rows[0].Values.Entries[0].Value.Values[0].Int = 99
	again, err := c.CounterRowsContext(ctx, "20260101", CounterHash("/app"), start, start+1000)
	if err != nil || again[0].Values.Entries[0].Value.Values[0].Int != 2 {
		t.Fatalf("borrowed output=%+v %v", again, err)
	}
}
func TestCounterCatalogDailyNamesPhysicalOrder(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	v := protocol.Value{Type: protocol.ValueDecimal, Int: 1}
	for _, item := range []struct {
		name, obj string
		kind      byte
	}{{"zeta", "/app", 3}, {"other", "/other", 3}, {"alpha", "/app", 2}, {"zeta", "/app", 4}, {"final", "/app", 6}} {
		catalogPut(t, c, start, item.obj, item.kind, protocol.Entry{Key: item.name, Value: v})
	}
	names, err := c.DailyCounterNamesContext(ctx, "20260101", CounterHash("/app"))
	if err != nil || !reflect.DeepEqual(names, []string{"zeta", "alpha", "final"}) {
		t.Fatalf("names=%v %v", names, err)
	}
	names[0] = "mutated"
	names, err = c.DailyCounterNamesContext(ctx, "20260101", CounterHash("/app"))
	if err != nil || names[0] != "zeta" {
		t.Fatal("borrowed names")
	}
}
func TestCounterCatalogValidationCancelAndPhysicalBounds(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	for _, p := range []struct {
		date string
		a, b int64
	}{{"../bad", start, start}, {"20260102", start, start}, {"20260101", 0, 0}, {"20260101", start, start - 1}, {"20260101", start, start + 86400001}, {"20260101", start, math.MaxInt64}} {
		if _, err := c.CounterRowsContext(ctx, p.date, 0, p.a, p.b); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.CounterRowsContext(cancelled, "20260101", 0, start, start); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := c.DailyCounterNamesContext(cancelled, "20260101", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	catalogPut(t, c, start, "/app", 1, protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}})
	tab, err := c.table("20260101", true, false)
	if err != nil {
		t.Fatal(err)
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], 0x7fffffff)
	if _, err = tab.data.WriteAt(b[:], 0); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CounterRowsContext(ctx, "20260101", CounterHash("/app"), start, start); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("physical bounds %v", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.DailyCounterNamesContext(ctx, "20260101", 0); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}
func TestCounterCatalogInvalidDailyKey(t *testing.T) {
	c, start := catalogCounters(t)
	catalogPut(t, c, start, "/app", 3, protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}})
	tab, err := c.table("20260101", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tab.index.Put([]byte{1}, fiveBytes(0)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.DailyCounterNamesContext(context.Background(), "20260101", CounterHash("/app")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("invalid key %v", err)
	}
	if _, err = os.Stat(filepath.Join(c.root, "20260101/counter/5m.kfile")); err != nil {
		t.Fatal(err)
	}
}

func TestCounterCatalogDecoderAndReadBudget(t *testing.T) {
	for _, count := range []int64{-1, 4097} {
		e := protocol.NewEncoder(protocol.Limits{})
		e.Decimal(count)
		body, _ := e.Bytes()
		if _, err := counterCatalogMap(body, []string{"cpu"}); err == nil {
			t.Fatalf("count=%d accepted", count)
		}
	}
	e := protocol.NewEncoder(protocol.Limits{})
	e.Decimal(2)
	for _, n := range []int64{1, 2} {
		e.Decimal(1)
		e.Value(protocol.Value{Type: protocol.ValueDecimal, Int: n})
	}
	body, _ := e.Bytes()
	v, err := counterCatalogMap(body, []string{"cpu"})
	if err != nil || len(v.Entries) != 1 || v.Entries[0].Value.Int != 2 {
		t.Fatalf("Java Map replace %+v %v", v, err)
	}
	if _, err = counterCatalogMap(append(body, 0), []string{"cpu"}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err = counterCatalogMap(body, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	c, start := catalogCounters(t)
	catalogPut(t, c, start, "/app", 1, protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 1}})
	tab, err := c.table("20260101", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = counterCatalogBody(context.Background(), tab.data, 0, 4); !errors.Is(err, ErrLimit) {
		t.Fatalf("budget=%v", err)
	}
	if _, err = counterCatalogBody(context.Background(), tab.data, -1, 100); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	c.mu.Lock()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.DailyCounterNamesContext(cancelled, "20260101", 0)
	c.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lock cancellation=%v", err)
	}
}

func TestCounterCatalogRowsLimitAndAbsentTable(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	rows, err := c.CounterRowsContext(ctx, "20260101", 0, start, start+86400000)
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	names, err := c.DailyCounterNamesContext(ctx, "20260101", 0)
	if err != nil || len(names) != 0 {
		t.Fatal(names, err)
	}
	// Each bucket remains small; the complete query must enforce its own cap.
	for n := int64(0); n <= counterCatalogRecords; n++ {
		catalogPut(t, c, start+n, "/app", 1)
	}
	if _, err = c.CounterRowsContext(ctx, "20260101", CounterHash("/app"), start, start+counterCatalogRecords); !errors.Is(err, ErrLimit) {
		t.Fatalf("record limit=%v", err)
	}
	rows, err = c.CounterRowsContext(ctx, "20260101", CounterHash("/app"), start+counterCatalogRecords, start+counterCatalogRecords)
	if err != nil || len(rows) != 1 || rows[0].Time != start+counterCatalogRecords {
		t.Fatalf("narrow query=%+v %v", rows, err)
	}
}

func TestCounterCatalogActualJava(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned source unavailable; actual Java catalog oracle not run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	work := t.TempDir()
	output, err := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts/java-storage.py"), "--classes-dir", filepath.Join(work, "compiled"), "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java compile: %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	root := filepath.Join(work, "java-db")
	for _, mode := range []string{"generate", "verify"} {
		output, err = exec.CommandContext(ctx, "java", "-Duser.timezone=Asia/Seoul", "-cp", classpath, "StorageOracle", mode, root).CombinedOutput()
		if err != nil {
			t.Fatalf("actual Java %s: %v\n%s", mode, err, output)
		}
		t.Log(strings.TrimSpace(string(output)))
	}
	// Compile a temporary caller of the unchanged original endpoint, data and
	// NetData parser classes. No source DB implementation or fixture is patched.
	oracleSource := filepath.Join(work, "CatalogOracle.java")
	if err = os.WriteFile(oracleSource, []byte(catalogJavaBulkOracle), 0600); err != nil {
		t.Fatal(err)
	}
	output, err = exec.CommandContext(ctx, "javac", "--release", "8", "-cp", classpath, "-d", filepath.Join(work, "compiled/classes"), oracleSource).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java bulk caller compile: %v\n%s", err, output)
	}
	for _, mode := range []string{"plain", "gzip"} {
		output, err = exec.CommandContext(ctx, "java", "-Duser.timezone=Asia/Seoul", "-cp", classpath, "CatalogOracle", filepath.Join(root, mode)).CombinedOutput()
		if err != nil {
			t.Fatalf("actual Java bulk oracle: %v\n%s", err, output)
		}
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"plain", "gzip"} {
		t.Run(mode, func(t *testing.T) {
			c, err := OpenCounters(filepath.Join(root, mode), loc)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			rows, err := c.CounterRowsContext(context.Background(), "20260101", CounterHash("/oracle/app"), oracleTime, oracleTime+1000)
			if err != nil || len(rows) != 2 {
				t.Fatalf("actual Java rows %+v %v", rows, err)
			}
			for n, row := range rows {
				if row.Time != oracleTime+int64(n)*500 || len(row.Values.Entries) != 2 {
					t.Fatalf("Java row=%+v", row)
				}
				for _, e := range row.Values.Entries {
					switch e.Key {
					case "cpu":
						if e.Value.Double != 12.5+float64(n) {
							t.Fatal(e)
						}
					case "errors":
						if e.Value.Int != 7+int64(n) {
							t.Fatal(e)
						}
					default:
						t.Fatal("unknown key", e)
					}
				}
			}
			names, err := c.DailyCounterNamesContext(context.Background(), "20260101", CounterHash("/oracle/app"))
			if err != nil {
				t.Fatal(err)
			}
			want := []string{}
			for kind := 2; kind <= 6; kind++ {
				for _, prefix := range []string{"b", "f", "d", "n"} {
					want = append(want, fmt.Sprintf("%s%d", prefix, kind))
				}
			}
			if !reflect.DeepEqual(names, want) {
				t.Fatalf("actual Java physical names=%v want=%v", names, want)
			}
			bulk, err := c.CounterBulkRowsContext(context.Background(), "20260101", oracleTime+60200, oracleTime+61200)
			if err != nil {
				t.Fatal(err)
			}
			golden, err := os.ReadFile(filepath.Join(root, mode, "catalog-bulk.bin"))
			if err != nil {
				t.Fatal(err)
			}
			d := protocol.NewDecoder(golden, protocol.Limits{})
			count := d.Int32()
			if count != 4 || int(count) != len(bulk) {
				t.Fatalf("actual Java bulk count=%d Go=%d", count, len(bulk))
			}
			for _, row := range bulk {
				v := d.Value()
				if d.Err() != nil || v.Type != protocol.ValueMap || row.Time != 0 || len(v.Entries) != len(row.Values.Entries) {
					t.Fatalf("actual Java physical bulk %v %+v", d.Err(), row)
				}
				for _, want := range v.Entries {
					found := false
					for _, got := range row.Values.Entries {
						if got.Key == want.Key {
							found = reflect.DeepEqual(want.Value, got.Value)
						}
					}
					if !found {
						t.Fatalf("actual Java bulk mismatch want=%+v got=%+v", want, row)
					}
				}
			}
			if d.Err() != nil || d.Remaining() != 0 {
				t.Fatal("Java bulk trailing bytes", d.Err())
			}
			bulk, err = c.CounterBulkRowsContext(context.Background(), "20260101", oracleTime, oracleTime+1000)
			if err != nil || len(bulk) != 0 {
				t.Fatalf("actual Java offset-zero gate Go=%+v %v", bulk, err)
			}
		})
	}
}

const catalogJavaBulkOracle = `
import java.nio.file.*;
import scouter.io.*;
import scouter.lang.value.*;
import scouter.server.db.counter.*;
import scouter.util.*;
public class CatalogOracle {
 static final long TIME=1767193200125L;
 static MapValue row(int n,boolean metadata){
  MapValue m=new MapValue();m.put("cpu",new DecimalValue(n));
  if(metadata){m.put("time",new DecimalValue(TIME+n));m.put("objHash",new DecimalValue(42));}
  return m;
 }
 static scala.collection.mutable.ArrayBuffer<MapValue> read(RealtimeCounterDBHeader h,RealtimeCounterData d,RealtimeCounterKeyFile i,long from,long to){
  scala.Tuple2<byte[],byte[]> p=i.getStartEndDataPos(from,to);
  if(p._1()==null||p._2()==null)return new scala.collection.mutable.ArrayBuffer<MapValue>();
  long a=DataInputX.toLong5(p._1(),0),b=DataInputX.toLong5(p._2(),0);
  if(a<=0||b<=0)return new scala.collection.mutable.ArrayBuffer<MapValue>();
  return RealtimeCounterDBHelper.setTagBytesMulti(h.getTagIntStr(),d.readBulk(a,b));
 }
 public static void main(String[]args)throws Exception{
  String file=Paths.get(args[0],"20260101/counter/real").toString();
  RealtimeCounterDBHeader h=new RealtimeCounterDBHeader(file);
  RealtimeCounterData d=new RealtimeCounterData(file);
  RealtimeCounterKeyFile i=new RealtimeCounterKeyFile(file);
  h.intern("time",true);h.intern("objHash",true);
  long[] offsets={60100,61000,60050,61100};
  for(int n=0;n<offsets.length;n++){
   long pos=d.write(RealtimeCounterDBHelper.getTagBytes(h.getTagStrInt(),row(n+2,n!=2)));
   i.write(HashUtil.hash(n==1?"/foreign":"/oracle/app"),TIME+offsets[n],pos);
  }
  scala.collection.mutable.ArrayBuffer<MapValue> rows=read(h,d,i,TIME+60200,TIME+61200);
  if(rows.size()!=4)throw new AssertionError("original physical bulk count "+rows.size());
  if(read(h,d,i,TIME,TIME+1000).size()!=0)throw new AssertionError("original offset-zero gate");
  DataOutputX out=new DataOutputX();out.writeInt(rows.size());
  for(int n=0;n<rows.size();n++)out.writeValue(rows.apply(n));
  Files.write(Paths.get(args[0],"catalog-bulk.bin"),out.toByteArray());
  h.close();d.close();i.close();
 }
}`

func TestCounterCatalogBulkPhysicalMinuteAndMetadata(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	values := func(id int64) []protocol.Entry {
		return []protocol.Entry{{Key: "time", Value: protocol.Value{Type: protocol.ValueDecimal, Int: start + id}}, {Key: "objHash", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 42}}, {Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: id}}}
	}
	// Physical position zero makes the original first chunk silently disappear.
	catalogPut(t, c, start, "/app", 1, values(0)...)
	catalogPut(t, c, start+1000, "/app", 1, values(1)...)
	rows, err := c.CounterBulkRowsContext(ctx, "20260101", start, start+1000)
	if err != nil || len(rows) != 0 {
		t.Fatalf("original posStart>0 gate %+v %v", rows, err)
	}
	// The first endpoint is oldest within its entire 500ms bucket, even before
	// stime. All physical rows in between survive, including a foreign object,
	// out-of-order index time, and a row without embedded metadata.
	catalogPut(t, c, start+60100, "/app", 1, values(2)...)
	catalogPut(t, c, start+61000, "/foreign", 1, values(3)...)
	catalogPut(t, c, start+60050, "/app", 1, protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: 4}})
	catalogPut(t, c, start+61100, "/app", 1, values(5)...)
	rows, err = c.CounterBulkRowsContext(ctx, "20260101", start+60200, start+61200)
	if err != nil || len(rows) != 4 {
		t.Fatalf("physical rows %+v %v", rows, err)
	}
	for n, row := range rows {
		if row.Time != 0 {
			t.Fatal("index timestamp synthesized", row)
		}
		found := false
		for _, entry := range row.Values.Entries {
			if entry.Key == "cpu" {
				found = true
				if entry.Value.Int != int64(n+2) {
					t.Fatal(row)
				}
			}
		}
		if !found {
			t.Fatal(row)
		}
	}
	if len(rows[2].Values.Entries) != 1 {
		t.Fatal("metadata was synthesized", rows[2])
	}
	rows[0].Values.Entries[0].Value.Int = 99
	again, err := c.CounterBulkRowsContext(ctx, "20260101", start+60200, start+61200)
	if err != nil || again[0].Values.Entries[0].Value.Int == 99 {
		t.Fatal("borrowed bulk values", err)
	}
	// A hash filter drops only rows whose embedded objHash is another object's:
	// the row without metadata stays for the caller, in the same order.
	objHash := func(row CounterRow) (int32, bool) {
		for _, entry := range row.Values.Entries {
			if entry.Key == "objHash" {
				return int32(entry.Value.Int), true
			}
		}
		return 0, false
	}
	app, ok := objHash(again[0])
	if !ok {
		t.Fatal("fixture row has no objHash", again[0])
	}
	var want []CounterRow
	for _, row := range again {
		if hash, ok := objHash(row); !ok || hash == app {
			want = append(want, row)
		}
	}
	got, err := c.CounterBulkRowsForContext(ctx, "20260101", start+60200, start+61200, map[int32]bool{app: true})
	if err != nil || len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered rows %+v, want %+v (%v)", got, want, err)
	}
	// Another object's filter selects none of them but the undecidable row.
	if got, err = c.CounterBulkRowsForContext(ctx, "20260101", start+60200, start+61200, map[int32]bool{app + 1: true}); err != nil || len(got) != 1 || len(got[0].Values.Entries) != 1 {
		t.Fatalf("foreign filter keeps only the row without objHash: %+v %v", got, err)
	}
	if got, err = c.CounterBulkRowsForContext(ctx, "20260101", start+60200, start+61200, nil); err != nil || !reflect.DeepEqual(got, again) {
		t.Fatalf("nil filter %+v %v", got, err)
	}
}
func TestCounterCatalogBulkEndpointOvershootAndCancel(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	catalogPut(t, c, start, "/seed", 1)
	catalogPut(t, c, start+1010, "/app", 1)
	// getDataPosFirst increments once beyond etime before checking again.
	rows, err := c.CounterBulkRowsContext(ctx, "20260101", start+750, start+750)
	if err != nil || len(rows) != 0 {
		t.Fatalf("mismatched endpoints should skip posEnd=0 %+v %v", rows, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = c.CounterBulkRowsContext(cancelled, "20260101", start, start); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = c.CounterBulkRowsContext(ctx, "../bad", start, start); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CounterBulkRowsContext(ctx, "20260101", start, start); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestCounterCatalogBulkMinuteOverlap(t *testing.T) {
	c, start := catalogCounters(t)
	ctx := context.Background()
	catalogPut(t, c, start, "/seed", 1)
	for _, offset := range []int64{60100, 120100, 121000} {
		catalogPut(t, c, start+offset, "/app", 1, protocol.Entry{Key: "cpu", Value: protocol.Value{Type: protocol.ValueDecimal, Int: offset}})
	}
	rows, err := c.CounterBulkRowsContext(ctx, "20260101", start+60200, start+121100)
	if err != nil || len(rows) != 4 {
		t.Fatalf("original minute overlap %+v %v", rows, err)
	}
	for n, want := range []int64{60100, 120100, 120100, 121000} {
		if len(rows[n].Values.Entries) != 1 || rows[n].Values.Entries[0].Value.Int != want {
			t.Fatalf("minute physical order %+v", rows)
		}
	}
}
