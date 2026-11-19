package storage

import "testing"

// SkipCleanMarker (a shutdown that left an ingest writer in a write) closes
// as usual but writes no marker; the next Open opens the pairs from the
// checkpoints Close persisted instead of trusting them sealed.
func TestSkipCleanMarker(t *testing.T) {
	dir := t.TempDir()
	s := cleanStore(t, dir, nil)
	putXLogs(t, s, 1, 50)
	s.SkipCleanMarker()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if markerExists(t, dir) || s.CleanMarkerWritten() {
		t.Fatal("marker written after SkipCleanMarker")
	}
	if s.CloseStage() != "checkpoints" {
		t.Fatalf("close stage %q", s.CloseStage())
	}
	s = cleanStore(t, dir, nil)
	putXLogs(t, s, 51, 60)
	if st := s.IndexOpenStats(); st.CleanMarker || st.Sealed != 0 || st.Checkpointed != 3 || st.Failed != 0 {
		t.Fatalf("restart without marker: %+v", st)
	}
	checkXLogs(t, s, "20261013", 1, 60)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !markerExists(t, dir) || !s.CleanMarkerWritten() {
		t.Fatal("clean Close wrote no marker")
	}
}
