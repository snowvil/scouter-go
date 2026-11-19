package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// javaHead encodes a .head the way RealtimeCounterDBHeader.saveTags writes it:
// int32 body length at 512, then int32 count and (text name, int32 id) pairs
// in the given order (Java writes StringIntMap hash order), in a file of at
// least 2048 bytes (openHeaderFile padding). ids are 1-based positions.
func javaHead(t testing.TB, names []string, order []int) []byte {
	t.Helper()
	e := protocol.NewEncoder(protocol.Limits{})
	e.Int32(int32(len(names)))
	for _, i := range order {
		e.Text(names[i])
		e.Int32(int32(i + 1))
	}
	body, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, max(2048, 516+len(body)))
	binary.BigEndian.PutUint32(out[512:], uint32(len(body)))
	copy(out[516:], body)
	return out
}

func realtimePack(tm int64, kv ...any) *protocol.PerfCounterPack {
	p := &protocol.PerfCounterPack{Time: tm, ObjName: "/names", TimeType: 1, Data: protocol.Value{Type: protocol.ValueMap}}
	for i := 0; i < len(kv); i += 2 {
		p.Data.Entries = append(p.Data.Entries, protocol.Entry{Key: kv[i].(string), Value: protocol.Value{Type: protocol.ValueDecimal, Int: int64(kv[i+1].(int))}})
	}
	return p
}

// A Java dictionary beyond the former 4096-name limit loads (any entry order)
// and accepts a new name; the result is the Java format with the new id.
func TestRealtimeCounterJavaDictionaryBeyondFormerLimit(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "20261013", "counter")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 5000)
	for i := range names {
		names[i] = fmt.Sprintf("java-%04d", i)
	}
	order := rand.New(rand.NewSource(1)).Perm(len(names))
	if err := os.WriteFile(filepath.Join(dir, "real.head"), javaHead(t, names, order), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "java-4999", 1, "new", 2)); err != nil {
		t.Fatal("put after a Java dictionary of 5000 names:", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if c, err = OpenCounters(root, time.UTC); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for name, want := range map[string]int64{"java-4999": 1, "new": 2} {
		points, err := c.Realtime("20261013", CounterHash("/names"), name, tm, tm)
		if err != nil || len(points) != 1 || points[0].Value.Int != want {
			t.Fatal(name, points, err)
		}
	}
	got, err := os.ReadFile(filepath.Join(dir, "real.head"))
	if err != nil {
		t.Fatal(err)
	}
	// In-place append: the Java entries keep their bytes and order, the new
	// entry follows them and the length and count cover it.
	want := javaHead(t, append(names, "new"), append(order, 5000))
	n := 516 + binary.BigEndian.Uint32(want[512:])
	if len(got) < int(n) || !bytes.Equal(got[:n], want[:n]) {
		t.Fatalf("head bytes differ from the Java layout")
	}
}

// At the dictionary bound only the new names are dropped: the known names of
// the same pack are stored, the drops are counted and logged (S133, at most
// once per 10s), and a pack of new names only writes no record.
func TestRealtimeCounterNamesLimitStoresKnownNames(t *testing.T) {
	root := t.TempDir()
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	c.namesMax = 3
	var logs []string
	c.SetLog(func(id string, sec int, message string) { logs = append(logs, fmt.Sprint(id, " ", sec, " ", message)) })
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "a", 1, "b", 2)); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm+1, "a", 3, "c", 4, "d", 5, "e", 6)); err != nil {
		t.Fatal("pack at the bound:", err)
	}
	if c.NamesLimited() != 2 || len(logs) != 1 || !strings.HasPrefix(logs[0], "S133 10 [warn] realtime counter names of 20261013") || !strings.Contains(logs[0], `first "d"`) {
		t.Fatal(c.NamesLimited(), logs)
	}
	data := filepath.Join(root, "20261013", "counter", "real.data")
	before, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm+2, "x", 1, "y", 2)); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(data)
	if err != nil || after.Size() != before.Size() || c.NamesLimited() != 4 || len(logs) != 1 {
		t.Fatal(before.Size(), after.Size(), err, c.NamesLimited(), logs)
	}
	check := func(c *Counters, name string, want ...int64) {
		t.Helper()
		points, err := c.Realtime("20261013", CounterHash("/names"), name, tm, tm+2)
		if err != nil || len(points) != len(want) {
			t.Fatal(name, points, err)
		}
		for i, p := range points {
			if p.Value.Int != want[i] {
				t.Fatal(name, points)
			}
		}
	}
	check(c, "a", 1, 3)
	check(c, "c", 4)
	check(c, "d")
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	head, err := os.ReadFile(filepath.Join(root, "20261013", "counter", "real.head"))
	want := javaHead(t, []string{"a", "b", "c"}, []int{0, 1, 2})
	if err != nil || !bytes.Equal(head, want) {
		t.Fatalf("head %x %v", head, err)
	}
	if c, err = OpenCounters(root, time.UTC); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.namesMax = 3
	if err = c.Put(realtimePack(tm+2, "b", 7, "d", 8)); err != nil || c.NamesLimited() != 1 {
		t.Fatal(err, c.NamesLimited())
	}
	check(c, "b", 2, 7)
}

// A failed dictionary save stores nothing of the pack and leaves the
// dictionary as before; the next save reuses the ids.
func TestRealtimeCounterNamesSaveFailureRollsBack(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes a read-only file; see TestRealtimeCounterNamesSaveFaults")
	}
	root := t.TempDir()
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "a", 1)); err != nil {
		t.Fatal(err)
	}
	head := filepath.Join(root, "20261013", "counter", "real.head")
	if err = os.Chmod(head, 0400); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm+1, "a", 2, "b", 3)); err == nil {
		t.Fatal("save into a read-only dictionary succeeded")
	}
	if err = os.Chmod(head, 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm+2, "c", 4, "b", 5)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(head)
	if want := javaHead(t, []string{"a", "c", "b"}, []int{0, 1, 2}); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("head %x %v", got, err)
	}
	for name, want := range map[string]int{"a": 1, "b": 1, "c": 1} {
		points, err := c.Realtime("20261013", CounterHash("/names"), name, tm, tm+2)
		if err != nil || len(points) != want {
			t.Fatal(name, points, err)
		}
	}
}

// Entries written after the body by an append that crashed before its length
// write are not part of the dictionary and are overwritten by the next save.
func TestRealtimeCounterNamesTornAppend(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "20261013", "counter")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	head := javaHead(t, []string{"a", "b"}, []int{1, 0})
	torn := append(append([]byte(nil), head[:516+binary.BigEndian.Uint32(head[512:])]...), bytes.Repeat([]byte{0xff}, 4000)...)
	if err := os.WriteFile(filepath.Join(dir, "real.head"), torn, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "b", 1, "c", 2)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "real.head"))
	want := javaHead(t, []string{"a", "b", "c"}, []int{1, 0, 2})
	n := 516 + binary.BigEndian.Uint32(want[512:])
	if err != nil || !bytes.Equal(got[:n], want[:n]) {
		t.Fatalf("head %x %v", got[:n], err)
	}
}

// Read bounds: a count the body cannot hold, a body past realtimeHeadMaxBody
// and more than realtimeNamesReadMax names are explicit errors, not an empty
// dictionary.
func TestRealtimeCounterNamesReadBounds(t *testing.T) {
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	many := make([]string, realtimeNamesReadMax+1)
	order := make([]int, len(many))
	for i := range order {
		order[i] = i
	}
	for _, v := range []struct {
		name string
		head func() []byte
		want error
	}{
		{"count", func() []byte {
			b := javaHead(t, []string{"a"}, []int{0})
			binary.BigEndian.PutUint32(b[516:], 2)
			return b
		}, protocol.ErrInvalid},
		{"body", func() []byte {
			b := javaHead(t, []string{"a"}, []int{0})
			binary.BigEndian.PutUint32(b[512:], realtimeHeadMaxBody+1)
			return b
		}, protocol.ErrLimit},
		{"names", func() []byte { return javaHead(t, many, order) }, protocol.ErrLimit},
	} {
		t.Run(v.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "20261013", "counter")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "real.head"), v.head(), 0600); err != nil {
				t.Fatal(err)
			}
			// An existing day (a query opens only a table with data).
			if err := os.WriteFile(filepath.Join(dir, "real.data"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			c, err := OpenCounters(root, time.UTC)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err = c.Put(realtimePack(tm, "a", 1)); !errors.Is(err, v.want) {
				t.Fatal(err)
			}
			if _, err = c.Realtime("20261013", CounterHash("/names"), "a", tm, tm); !errors.Is(err, v.want) {
				t.Fatal(err)
			}
		})
	}
}

// The dictionary body bound drops a new name the same way as the name count.
func TestRealtimeCounterNamesBodyBound(t *testing.T) {
	c, err := OpenCounters(t.TempDir(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	var kv []any
	for i := 0; i < 5; i++ {
		kv = append(kv, strings.Repeat(fmt.Sprint(i), 4_000_000), i)
	}
	if err = c.Put(realtimePack(tm, kv...)); err != nil || c.NamesLimited() != 1 {
		t.Fatal(err, c.NamesLimited())
	}
	points, err := c.Realtime("20261013", CounterHash("/names"), kv[6].(string), tm, tm)
	if err != nil || len(points) != 1 || points[0].Value.Int != 3 {
		t.Fatal(len(points), err)
	}
}

// A .head removed under the open table is written again with the whole
// dictionary, not only the new names.
func TestRealtimeCounterNamesRecreatedHead(t *testing.T) {
	root := t.TempDir()
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "a", 1, "b", 2)); err != nil {
		t.Fatal(err)
	}
	head := filepath.Join(root, "20261013", "counter", "real.head")
	if err = os.Remove(head); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm+1, "c", 3)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(head)
	if want := javaHead(t, []string{"a", "b", "c"}, []int{0, 1, 2}); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("head %x %v", got, err)
	}
}

// Java saveTags writes the length and then the body, and openHeaderFile pads a
// file shorter than 2048 bytes, so a kill or ENOSPC can leave a .head whose
// length is new while the body is the previous one (or ends early), a file
// shorter than 2048 bytes, or an empty file. The Java reader parses count
// entries of the zero-padded file and ignores the rest; Go does the same and
// appends right after the last parsed entry.
func TestRealtimeCounterNamesJavaTornForms(t *testing.T) {
	ab := javaHead(t, []string{"a", "b"}, []int{1, 0})
	abBody := 516 + int(binary.BigEndian.Uint32(ab[512:]))
	for _, v := range []struct {
		name  string
		head  func() []byte
		names []string
		order []int
	}{
		{"length-new-body-old", func() []byte {
			b := append([]byte(nil), ab[:abBody]...)
			binary.BigEndian.PutUint32(b[512:], uint32(abBody-516+40))
			return b
		}, []string{"a", "b"}, []int{1, 0}},
		{"length-new-body-old-padded", func() []byte {
			b := append([]byte(nil), ab...)
			binary.BigEndian.PutUint32(b[512:], uint32(abBody-516+40))
			return b
		}, []string{"a", "b"}, []int{1, 0}},
		{"short-file-with-body", func() []byte { return append([]byte(nil), ab[:abBody]...) }, []string{"a", "b"}, []int{1, 0}},
		{"short-file-zeros", func() []byte { return make([]byte, 700) }, nil, nil},
		{"short-file-before-length", func() []byte { return make([]byte, 100) }, nil, nil},
		{"empty-file", func() []byte { return nil }, nil, nil},
		{"negative-length", func() []byte {
			b := append([]byte(nil), ab...)
			binary.BigEndian.PutUint32(b[512:], 0xffffff00)
			return b
		}, nil, nil},
		{"length-only", func() []byte {
			b := make([]byte, 516)
			binary.BigEndian.PutUint32(b[512:], 30)
			return b
		}, nil, nil},
	} {
		t.Run(v.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "20261013", "counter")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "real.head"), v.head(), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := OpenCounters(root, time.UTC)
			if err != nil {
				t.Fatal(err)
			}
			tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
			kv := []any{"c", 3}
			if len(v.names) > 0 {
				kv = append(kv, "a", 1)
			}
			if err = c.Put(realtimePack(tm, kv...)); err != nil {
				t.Fatal("put after a torn Java head:", err)
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			names := append(append([]string(nil), v.names...), "c")
			order := append(append([]int(nil), v.order...), len(v.names))
			want := javaHead(t, names, order)
			n := 516 + binary.BigEndian.Uint32(want[512:])
			got, err := os.ReadFile(filepath.Join(dir, "real.head"))
			if err != nil || len(got) < int(n) || !bytes.Equal(got[512:n], want[512:n]) {
				t.Fatalf("head %x %v", got, err)
			}
			if c, err = OpenCounters(root, time.UTC); err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			points, err := c.Realtime("20261013", CounterHash("/names"), "c", tm, tm)
			if err != nil || len(points) != 1 || points[0].Value.Int != 3 {
				t.Fatal(points, err)
			}
		})
	}
}

// A dictionary Go cannot load fails before the index pair is opened, so a
// pack for that date does not walk the index under the Counters gate; the
// failure is remembered until the file changes.
func TestRealtimeCounterNamesLoadFailureBeforeIndex(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "20261013", "counter")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	bad := javaHead(t, []string{"a", "b"}, []int{0, 1})
	binary.BigEndian.PutUint32(bad[516+4+6+2:], 9) // count, ("a", 1), then the id of "b": out of range
	head := filepath.Join(dir, "real.head")
	if err := os.WriteFile(head, bad, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 3; i++ {
		if err = c.Put(realtimePack(tm, "a", 1)); !errors.Is(err, protocol.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if st := c.IndexOpenStats(); st.Created+st.Walked+st.Sealed+st.Checkpointed+st.Failed != 0 {
		t.Fatalf("index opened for an unreadable dictionary: %+v", st)
	}
	if err = os.WriteFile(head, javaHead(t, []string{"a", "b"}, []int{0, 1}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.Put(realtimePack(tm, "a", 1)); err != nil {
		t.Fatal("after the file was fixed:", err)
	}
}

// Each step of an in-place save failing (after it ran) stores nothing of the
// pack; the next save overwrites what reached the file, and the dictionary on
// disk is then exactly the one in memory.
func TestRealtimeCounterNamesSaveFaults(t *testing.T) {
	failed := errors.New("injected")
	for _, step := range []string{"entries", "entries-sync", "length", "length-sync"} {
		t.Run(step, func(t *testing.T) {
			t.Cleanup(func() { counterHeadFault = nil })
			root := t.TempDir()
			c, err := OpenCounters(root, time.UTC)
			if err != nil {
				t.Fatal(err)
			}
			tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
			if err = c.Put(realtimePack(tm, "a", 1)); err != nil {
				t.Fatal(err)
			}
			counterHeadFault = func(s string) error {
				if s == step {
					return failed
				}
				return nil
			}
			long := strings.Repeat("b", 40)
			if err = c.Put(realtimePack(tm+1, "a", 2, long, 3)); !errors.Is(err, failed) {
				t.Fatal(err)
			}
			counterHeadFault = nil
			if err = c.Put(realtimePack(tm+2, "c", 4)); err != nil {
				t.Fatal(err)
			}
			if err = c.Close(); err != nil {
				t.Fatal(err)
			}
			head, err := os.ReadFile(filepath.Join(root, "20261013", "counter", "real.head"))
			want := javaHead(t, []string{"a", "c"}, []int{0, 1})
			n := 516 + binary.BigEndian.Uint32(want[512:])
			if err != nil || !bytes.Equal(head[512:n], want[512:n]) {
				t.Fatalf("head %x %v", head, err)
			}
			if c, err = OpenCounters(root, time.UTC); err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			for name, want := range map[string]int{"a": 1, long: 0, "c": 1} {
				points, err := c.Realtime("20261013", CounterHash("/names"), name, tm, tm+2)
				if err != nil || len(points) != want {
					t.Fatal(name, points, err)
				}
			}
		})
	}
}

// A name on two ids resolves to the entry last in the file, as Java's
// tagStrInt.put; a new name then takes count+1 (Java would take its distinct
// name count + 1, an id already in use).
func TestRealtimeCounterNamesRepeatedName(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "20261013", "counter")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "real.head"), javaHead(t, []string{"x", "x"}, []int{1, 0}), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounters(root, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tm := time.Date(2026, 10, 13, 1, 0, 0, 0, time.UTC).UnixMilli()
	if err = c.Put(realtimePack(tm, "x", 5, "y", 6)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "real.data"))
	// int32 length, Decimal count 2, Decimal id 1 (x, the last entry in the
	// file), DecimalValue 5 (3 bytes), Decimal id 3 (y).
	if err != nil || len(data) < 13 || !bytes.Equal(data[4:8], []byte{1, 2, 1, 1}) || !bytes.Equal(data[11:13], []byte{1, 3}) {
		t.Fatalf("record %x %v", data, err)
	}
}
