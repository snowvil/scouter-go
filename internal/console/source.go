package console

import (
	"context"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Object is one entry of the server's in-memory object map (Java
// AgentManager objMap), dead objects included.
type Object struct {
	Hash       int32
	Type, Name string
	Alive      bool
}

// ObjectType is the CounterEngine view of one object type. HasFamily=false
// is a type whose family was unknown (Java listCounters() returns null).
type ObjectType struct {
	Family    string
	HasFamily bool
	// Counters is getAllCounterList: family counters then type counters
	// whose all attribute is true, in definition order (duplicates kept).
	Counters []string
}

// TagValue is one ValueCount of a tag top-100 result (count-descending).
type TagValue struct {
	Value protocol.Value
	Count float64
}

// Stat is one `debug queue` line.
type Stat struct {
	Key   string
	Value int64
}

// Source is the server state the console reads. Every method returns a
// detached snapshot; a nil error with no data is a valid empty result.
type Source interface {
	// Objects lists the in-memory objects in a stable order (dead included).
	Objects(ctx context.Context) ([]Object, error)
	// ObjectType looks up a counter object type; ok=false if unknown.
	ObjectType(name string) (ObjectType, bool)
	// RealtimeCounter is CounterCache.get(objHash, name, REALTIME).
	RealtimeCounter(ctx context.Context, hash int32, name string) (protocol.Value, bool, error)
	// XLogsSince is the realtime XLog loop cache read after (loop, index)
	// for the given objects (never empty); it returns the new cursor.
	XLogsSince(ctx context.Context, loop int64, index int32, hashes []int32) (int64, int32, []*protocol.XLogPack, error)
	// XLogsByTime reads the date's XLog DB between from and to (ms), in
	// time-index order.
	XLogsByTime(ctx context.Context, date string, from, to int64) ([]*protocol.XLogPack, error)
	// Text is the text dictionary lookup; ok=false if missing.
	Text(ctx context.Context, date, div string, hash int32) (string, bool, error)
	// TagValues is the date's top-100 values of a tag, count-descending.
	TagValues(ctx context.Context, date, objType, group, tag string) ([]TagValue, error)
	// TagValueSeries is the 1440 per-minute counts of one tag value.
	TagValueSeries(ctx context.Context, date, objType, group, tag string, value protocol.Value) ([]float32, error)
	// VisitorObjType / VisitorObject are today's visitor cardinalities.
	VisitorObjType(ctx context.Context, objType string) (int64, error)
	VisitorObject(ctx context.Context, hash int32) (int64, error)
	// Stats is the `debug queue` key set (any order; the console sorts).
	Stats() []Stat
}
