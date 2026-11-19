package service

// Java oracle comparison for ThreadList (testdata/management/threadlist).
// SERVER_THREAD_LIST/DETAIL describe the running JVM, so the oracle kept only
// the item count, MapPack key order and value types (facts.*_shape) plus the
// bytes of the id 0/missing/unknown/Text cases. Go answers with goroutines
// (server_threads.go); the shape and the empty-detail bytes must match.

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func oracleTypeName(v protocol.Value) string {
	switch v.Type {
	case protocol.ValueNull:
		return "null"
	case protocol.ValueBoolean:
		return "boolean"
	case protocol.ValueDecimal:
		return "decimal"
	case protocol.ValueFloat:
		return "float"
	case protocol.ValueDouble:
		return "double"
	case protocol.ValueText:
		return "text"
	case protocol.ValueList:
		if len(v.Values) == 0 {
			return "list"
		}
		set := map[string]bool{}
		for _, x := range v.Values {
			set[oracleTypeName(x)] = true
		}
		var names []string
		for n := range set {
			names = append(names, n)
		}
		sort.Strings(names)
		return "list<" + strings.Join(names, "|") + ">"
	case protocol.ValueMap:
		return "map"
	case protocol.ValueBlob:
		return "blob"
	}
	return "type" + strconv.Itoa(int(v.Type))
}

// shapeOf renders key order and types; the Java JSON object keeps insertion
// order, so the expected order is read from the raw golden text.
func shapeOf(m *protocol.MapPack) []string {
	var out []string
	for _, e := range m.Entries {
		out = append(out, e.Key+":"+oracleTypeName(e.Value))
	}
	return out
}

// javaShapeKeys decodes facts.<name>.items[0].keys preserving JSON order.
func javaShapeKeys(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var shape struct {
		Items []struct {
			Keys json.RawMessage `json:"keys"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil || len(shape.Items) != 1 {
		t.Fatalf("shape %s: %v", raw, err)
	}
	dec := json.NewDecoder(strings.NewReader(string(shape.Items[0].Keys)))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		var v string
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, k.(string)+":"+v)
	}
	return out
}

func TestServerThreadsJavaOracle(t *testing.T) {
	dir, cases := loadOracleCases(t, "threadlist")
	var root struct {
		Facts map[string]json.RawMessage `json:"facts"`
	}
	if err := json.Unmarshal(readGolden(t, dir, "cases.json"), &root); err != nil {
		t.Fatal(err)
	}
	expect := map[string]oracleExpectation{
		"thread-detail-zero":       exactly(),
		"thread-detail-missing-id": exactly(),
		"thread-detail-unknown":    exactly(),
		"thread-detail-text-id":    exactly(),
	}
	requireAllExpected(t, cases, expect)
	captureManageLog(t)
	_, conn, r := startService(t, queryConfig(), &fakeBackend{})
	client := &oracleClient{owner: t, session: loginSession(t, conn, r), conn: conn, r: r}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) { replayOracleCase(t, dir, c, client, nil, expect[c.Name]) })
	}

	// SERVER_THREAD_LIST: one MapPack id/name/stat/cpu, typed like Java.
	sendOracleRaw(t, client.conn, "SERVER_THREAD_LIST", client.session, nil)
	list := readOracleResponse(t, client.r, "pack")
	if !list.complete || len(list.items) != 1 {
		t.Fatalf("SERVER_THREAD_LIST: %d items", len(list.items))
	}
	lm := list.items[0].pack.(*protocol.MapPack)
	if got, want := strings.Join(shapeOf(lm), ","), strings.Join(javaShapeKeys(t, root.Facts["thread_list_shape"]), ","); got != want {
		t.Fatalf("SERVER_THREAD_LIST shape\n go   %s\n java %s", got, want)
	}
	ids := lookup(lm, "id").Values
	for _, col := range []string{"name", "stat", "cpu"} {
		if len(lookup(lm, col).Values) != len(ids) {
			t.Fatalf("column %s length %d, id %d", col, len(lookup(lm, col).Values), len(ids))
		}
	}

	// SERVER_THREAD_DETAIL of a listed id: Java key order and value types.
	detail := protocol.MapPack{Entries: []protocol.Entry{{Key: "id", Value: ids[0]}}}
	body, err := protocol.EncodePack(&detail, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sendOracleRaw(t, client.conn, "SERVER_THREAD_DETAIL", client.session, body)
	got := readOracleResponse(t, client.r, "pack")
	if !got.complete || len(got.items) != 1 {
		t.Fatalf("SERVER_THREAD_DETAIL: %d items", len(got.items))
	}
	if g, want := strings.Join(shapeOf(got.items[0].pack.(*protocol.MapPack)), ","), strings.Join(javaShapeKeys(t, root.Facts["thread_detail_shape"]), ","); g != want {
		t.Fatalf("SERVER_THREAD_DETAIL shape\n go   %s\n java %s", g, want)
	}
}
