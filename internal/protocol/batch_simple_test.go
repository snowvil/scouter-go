package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestBatchSimpleGoldenAndSQLMetadata(t *testing.T) {
	p := &BatchPack{StartTime: 1, ObjHash: -2, BatchJobID: "job", Args: "-a", PID: 3, ElapsedTime: 4, ThreadCount: 5, CPUTime: 6, GCTime: 7, GCCount: 8, SQLTotalCount: 2, SQLTotalTime: 9, SQLTotalRows: 10, SQLTotalRuns: 11, IsStack: true, ObjName: "/host/app", ObjType: "java", Position: 12}
	b, err := EncodeBatchSimple(p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	d := NewDecoder(b, Limits{})
	internal := d.Blob()
	if d.Err() != nil || d.Remaining() != 0 {
		t.Fatalf("Blob body %v", d.Err())
	}
	if internal[0] != 0 || len(internal) < 90 {
		t.Fatalf("unexpected fixedbody %x", internal)
	}
	got, err := DecodeBatchSimple(b, Limits{})
	if err != nil || !reflect.DeepEqual(p, got) {
		t.Fatalf("simple %+v %v", got, err)
	}
	// No SQL arrays are required by writeSimple although sqlTotalCnt is nonzero.
	p.SQLStats = []BatchSQLStat{{}}
	p.UniqueSQLs = []BatchSQLText{{Hash: 1}}
	p.Extra = []byte{99}
	other, err := EncodeBatchSimple(p, Limits{})
	if err != nil || !bytes.Equal(b, other) {
		t.Fatalf("SQL detail leaked into simple %v", err)
	}
}
func TestBatchSimpleMalformed(t *testing.T) {
	if _, err := EncodeBatchSimple(nil, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("typed nil %v", err)
	}
	p := &BatchPack{SQLTotalCount: -1}
	if _, err := EncodeBatchSimple(p, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative count %v", err)
	}
	b, err := EncodeBatchSimple(&BatchPack{}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{b[:len(b)-1], append(append([]byte{}, b...), 0), {254, 255, 255, 255, 255}} {
		if _, err := DecodeBatchSimple(bad, Limits{}); err == nil {
			t.Fatalf("bad simple accepted %x", bad)
		}
	}
	if _, err = EncodeBatchSimple(&BatchPack{BatchJobID: string([]byte{255})}, Limits{}); err == nil {
		t.Fatal("bad UTF8 accepted")
	}
	if _, err = DecodeBatchSimple(b, Limits{MaxBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func FuzzDecodeBatchSimple(f *testing.F) {
	b, err := EncodeBatchSimple(&BatchPack{StartTime: 1, BatchJobID: "job", SQLTotalCount: 2}, Limits{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 65536 {
			return
		}
		p, err := DecodeBatchSimple(b, Limits{MaxBlobBytes: 65536})
		if err != nil {
			return
		}
		encoded, err := EncodeBatchSimple(p, Limits{MaxBlobBytes: 65536})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeBatchSimple(encoded, Limits{MaxBlobBytes: 65536}); err != nil {
			t.Fatal(err)
		}
	})
}
