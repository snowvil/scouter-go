package storage

// Test hooks for the external tests of this directory (package storage_test),
// which drive the ingest engine against a Store.

// HoldGate takes the Store gate until release, as a stalled disk or a long
// hold would: the writers and the file reads wait behind it.
func HoldGate(s *Store) (release func()) {
	s.mu.Lock()
	return s.mu.Unlock
}

// WriterQueueLens reports the records queued for the text, XLog and profile
// writers (a writer blocked on the gate holds one more, taken off its queue).
func WriterQueueLens(s *Store) (text, xlog, profile int) {
	return len(s.texts.queue), len(s.xlogQ.queue), len(s.profQ.queue)
}

// PackWritersAtGate reports whether the XLog and profile writers have taken
// their hold's records off the queue and wait for (or hold) the Store gate.
func PackWritersAtGate(s *Store) (xlog, profile bool) {
	return s.xlogQ.atGate.Load(), s.profQ.atGate.Load()
}

const TextQueueMax = textQueueMax

const TextQueueMaxBytes = textQueueMaxBytes

// TextQueueBytes is the bytes charged to the text queue (queued texts and
// the one a blocked writer holds), and TextQueueOverhead what a text of div
// is charged beyond its length.
func TextQueueBytes(s *Store) int {
	s.texts.mu.Lock()
	defer s.texts.mu.Unlock()
	return s.texts.bytes
}

func TextQueueOverhead(s *Store, div string) int {
	return len(s.permTextPath(div)) + 4 + 64
}
