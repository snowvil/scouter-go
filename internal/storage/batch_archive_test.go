package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

func TestBatchArchiveChunksNameAndRestart(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	data := make([]byte, 240001)
	for n := range data {
		data[n] = byte(n % 251)
	}
	input := bytes.NewReader(append(append([]byte{}, data...), 99))
	if err := s.PutBatchArchiveContext(ctx, start+13*3600000, "/host/app", "archive", int64(len(data)), input); err != nil {
		t.Fatal(err)
	}
	if input.Len() != 1 {
		t.Fatal("consumed beyond declared size", input.Len())
	}
	path := filepath.Join(s.config.Directory, "20260101/host/app/13/archive.zip")
	file, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(file, data) {
		t.Fatal("raw archive bytes", err)
	}
	chunks, err := s.BatchArchiveContext(ctx, start+13*3600000, "/host/app", "archive.zip")
	if err != nil || len(chunks) != 3 || len(chunks[0]) != 120000 || len(chunks[1]) != 120000 || len(chunks[2]) != 1 || !bytes.Equal(bytes.Join(chunks, nil), data) {
		t.Fatalf("chunks=%d %v", len(chunks), err)
	}
	chunks[0][0] = 99
	chunks, err = s.BatchArchiveContext(ctx, start+13*3600000, "/host/app", "archive.zip")
	if err != nil || chunks[0][0] != 0 {
		t.Fatal("borrowed chunks", err)
	}
	if _, err = s.BatchArchiveContext(ctx, start+13*3600000, "/host/app", "archive"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read must not add .zip: %v", err)
	}
	if chunks, err = s.BatchArchiveContext(ctx, start+14*3600000, "/host/app", "archive.zip"); err != nil || len(chunks) != 0 {
		t.Fatalf("missing hour %+v %v", chunks, err)
	}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "empty.zip", 0, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(s.config.Directory, "20260101/host/app/0/empty.zip.zip")); err != nil {
		t.Fatal(err)
	}
	if chunks, err = s.BatchArchiveContext(ctx, start, "/host/app", "empty.zip.zip"); err != nil || len(chunks) != 0 {
		t.Fatal(chunks, err)
	}
	cfg := s.config
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	chunks, err = s.BatchArchiveContext(ctx, start+13*3600000, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(bytes.Join(chunks, nil), data) {
		t.Fatal("restart archive", err)
	}
}

type archiveCancelReader struct{ cancel context.CancelFunc }

func (r archiveCancelReader) Read(b []byte) (int, error) { b[0] = 1; r.cancel(); return 1, nil }

type archiveNoProgressReader struct{}

func (archiveNoProgressReader) Read([]byte) (int, error) { return 0, nil }

func TestBatchArchiveCancelDuringUploadSymlinkAndReadLimits(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryRecords: 1})
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	if err := s.PutBatchArchiveContext(cancelled, start, "/host/app", "archive", 1, archiveCancelReader{cancel}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	dir := filepath.Join(s.config.Directory, "20260101/host/app/0")
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("cancel temp leaked", entries, err)
	}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 1, archiveNoProgressReader{}); !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(err)
	}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 120001, bytes.NewReader(make([]byte, 120001))); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "external.zip"), make([]byte, 120001), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BatchArchiveContext(ctx, start, "/host/app", "external.zip"); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err = os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(dir, "escape.zip")); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "escape", 0, bytes.NewReader(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err = s.BatchArchiveContext(ctx, start, "/host/app", "escape.zip"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = os.Symlink(t.TempDir(), filepath.Join(s.config.Directory, "20260101/host/app/1")); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBatchArchiveContext(ctx, start+3600000, "/host/app", "archive", 0, bytes.NewReader(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// The unsuffixed base may be a directory: only the real .zip target matters.
	if err = os.Mkdir(filepath.Join(dir, "name"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "name", 0, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
}

func TestBatchArchiveActualJava(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned source unavailable; actual Java archive oracle not run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	work := t.TempDir()
	classes := filepath.Join(work, "compiled")
	output, err := exec.CommandContext(ctx, "python3", filepath.Join(repo, "scripts/java-storage.py"), "--classes-dir", classes, "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("original classes compile %v\n%s", err, output)
	}
	classpath := strings.TrimSpace(string(output))
	stub := filepath.Join(work, "Configure.java")
	oracle := filepath.Join(work, "ArchiveOracle.java")
	if err = os.WriteFile(stub, []byte(`package scouter.server; public class Configure {private static final Configure I=new Configure();public static Configure getInstance(){return I;}public boolean log_udp_batch=false;}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(oracle, []byte(batchArchiveJavaOracle), 0600); err != nil {
		t.Fatal(err)
	}
	output, err = exec.CommandContext(ctx, "javac", "--release", "8", "-cp", classpath, "-d", filepath.Join(classes, "classes"), stub, filepath.Join(source, "scouter.server/src/main/java/scouter/server/db/BatchZipDB.java"), oracle).CombinedOutput()
	if err != nil {
		t.Fatalf("original BatchZipDB compile %v\n%s", err, output)
	}
	run := func(mode, root string) {
		t.Helper()
		output, err = exec.CommandContext(ctx, "java", "-Duser.timezone=Asia/Seoul", "-cp", classpath, "ArchiveOracle", mode, root).CombinedOutput()
		if err != nil {
			t.Fatalf("original archive %s %v\n%s", mode, err, output)
		}
	}
	javaDB, goDB := filepath.Join(work, "java-db"), filepath.Join(work, "go-db")
	run("generate", javaDB)
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{Directory: javaDB, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := s.BatchArchiveContext(context.Background(), oracleTime, "/oracle/app", "archive.zip")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	wire := protocol.NewEncoder(protocol.Limits{})
	for _, chunk := range chunks {
		wire.Byte(3)
		wire.Blob(chunk)
	}
	b, err := wire.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	j, err := os.ReadFile(filepath.Join(javaDB, "archive-wire.bin"))
	if err != nil || !bytes.Equal(b, j) {
		t.Fatal("actual Java HasNEXT+Blob chunk bytes", err)
	}
	s, err = Open(Config{Directory: goDB, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Join(chunks, nil)
	if err = s.PutBatchArchiveContext(context.Background(), oracleTime, "/oracle/app", "archive", int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	run("verify", goDB)
	j, err = os.ReadFile(filepath.Join(goDB, "archive-wire.bin"))
	if err != nil || !bytes.Equal(b, j) {
		t.Fatal("Go file actual Java reader bytes", err)
	}
}

const batchArchiveJavaOracle = `
import java.io.*;
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.server.db.*;
public class ArchiveOracle{
 public static void main(String[]args)throws Exception{
  System.setProperty("oracle.root",args[1]);
  byte[] bytes=new byte[240001];for(int n=0;n<bytes.length;n++)bytes[n]=(byte)(n%251);
  if(args[0].equals("generate"))BatchZipDB.write(1767193200125L,"/oracle/app","archive",bytes.length,new ByteArrayInputStream(bytes));
  if(!Arrays.equals(bytes,Files.readAllBytes(Paths.get(args[1],"20260101/oracle/app/0/archive.zip"))))throw new AssertionError("archive raw bytes/hour/path");
  DataOutputX out=new DataOutputX();BatchZipDB.read("/oracle/app",1767193200125L,"archive.zip",out);
  Files.write(Paths.get(args[1],"archive-wire.bin"),out.toByteArray());
 }
}`

func TestBatchArchiveAtomicCancelAndBounds(t *testing.T) {
	s, start := extraStore(t, Config{MaxQueryBytes: 120001})
	ctx := context.Background()
	for _, name := range []string{"", ".", "..", "../archive", "/archive", "archive/part", "archive\\part", "archive\x00", "bad\xff"} {
		if err := s.PutBatchArchiveContext(ctx, start, "/host/app", name, 0, bytes.NewReader(nil)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("filename=%q %v", name, err)
		}
	}
	if err := s.PutBatchArchiveContext(ctx, start, "/host/../app", "archive", 0, bytes.NewReader(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, size := range []int64{-1, 120002} {
		if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", size, bytes.NewReader(nil)); err == nil {
			t.Fatal("size accepted", size)
		}
	}
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 1, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 3, bytes.NewReader([]byte{1, 2, 3})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip"); err != nil {
		t.Fatal(err)
	} // cache the old inode
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 3, bytes.NewReader([]byte{9})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short upload=%v", err)
	}
	chunks, err := s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(chunks[0], []byte{1, 2, 3}) {
		t.Fatal("old archive changed", err)
	}
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 1, bytes.NewReader([]byte{7})); err != nil {
		t.Fatal(err)
	}
	chunks, err = s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(chunks[0], []byte{7}) {
		t.Fatal("stale cached inode", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.config.Directory, "20260101/host/app/0"))
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary file leaked", entries, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.BatchArchiveContext(cancelled, start, "/host/app", "archive.zip"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = s.PutBatchArchiveContext(cancelled, start, "/host/app", "archive", 0, bytes.NewReader(nil)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// archiveGateProbeReader is a slow Agent upload: before its first bytes it
// runs probe (which may need the Store gate) in another goroutine and waits
// for it, so the upload fails if it holds the gate while reading the body.
type archiveGateProbeReader struct {
	data  []byte
	probe func() error
	done  bool
	err   error
}

func (r *archiveGateProbeReader) Read(b []byte) (int, error) {
	if !r.done {
		r.done = true
		result := make(chan error, 1)
		go func() { result <- r.probe() }()
		select {
		case r.err = <-result:
		case <-time.After(5 * time.Second):
			r.err = errors.New("store gate held while the upload body is read")
		}
		if r.err != nil {
			return 0, r.err
		}
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(b, r.data)
	r.data = r.data[n:]
	return n, nil
}

// Regression: a slow batch archive upload held the Store gate while reading
// the Agent body (up to 64 MiB, 30 s deadline) and fsyncing, so PutXLog (all
// ingest) waited for it.
func TestBatchArchiveUploadDoesNotHoldStoreGate(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	data := bytes.Repeat([]byte{5}, 250000)
	ingest := func() error {
		return s.PutXLog(&protocol.XLogPack{EndTime: start + 1000, TxID: 1, ObjHash: 9, Elapsed: 1})
	}
	r := &archiveGateProbeReader{data: append([]byte{}, data...), probe: ingest}
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", int64(len(data)), r); err != nil {
		t.Fatal(err)
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	chunks, err := s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(bytes.Join(chunks, nil), data) {
		t.Fatal("archive", err)
	}

	// Concurrent uploads of one key: each completes and the target is one
	// whole archive (the last rename), never a mix.
	first := &archiveGateProbeReader{data: bytes.Repeat([]byte{1}, 130000)}
	second := &archiveGateProbeReader{data: bytes.Repeat([]byte{2}, 130000), probe: func() error { return nil }}
	released := make(chan struct{})
	first.probe = func() error { <-released; return nil }
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 130000, first)
	}()
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 130000, second); err != nil {
		t.Fatal(err)
	}
	close(released)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	chunks, err = s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(bytes.Join(chunks, nil), bytes.Repeat([]byte{1}, 130000)) {
		t.Fatal("last rename wins", err)
	}
	dir := filepath.Join(s.config.Directory, "20260101/host/app/0")
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatal("temporary file leaked", entries, err)
	}

	// A purge (rename to trash under the gate) of the hour directory while the
	// body is read: nothing is published and the DB tree is not recreated.
	trash := t.TempDir()
	purge := &archiveGateProbeReader{data: []byte{9}, probe: func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		return os.Rename(dir, filepath.Join(trash, "hour"))
	}}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 1, purge); !os.IsNotExist(err) {
		t.Fatalf("purged directory: %v", err)
	}
	if purge.err != nil {
		t.Fatal(purge.err)
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("purged directory recreated", err)
	}

	// The target turned into a directory, or the Store closed, while the body
	// was read: the publish is refused and nothing changes.
	target := &archiveGateProbeReader{data: []byte{9}, probe: func() error { return os.MkdirAll(filepath.Join(dir, "other.zip"), 0700) }}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "other", 1, target); !errors.Is(err, ErrInvalid) {
		t.Fatalf("target became a directory: %v", err)
	}
	closing := &archiveGateProbeReader{data: []byte{9}, probe: s.Close}
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "late", 1, closing); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed during upload: %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != "other.zip" {
		t.Fatal("refused uploads left files", entries, err)
	}
}

// The read side opens a private handle under the gate and reads it after
// releasing the gate: a replacement while chunks are read does not tear the
// result (the handle keeps the inode it opened).
func TestBatchArchiveReadKeepsOpenedInode(t *testing.T) {
	s, start := extraStore(t, Config{})
	ctx := context.Background()
	old := bytes.Repeat([]byte{3}, 240001)
	if err := s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", int64(len(old)), bytes.NewReader(old)); err != nil {
		t.Fatal(err)
	}
	f, err := s.openBatchArchive(ctx, start, "/host/app", "archive.zip")
	if err != nil || f == nil {
		t.Fatal(f, err)
	}
	defer f.Close()
	if err = s.PutBatchArchiveContext(ctx, start, "/host/app", "archive", 2, bytes.NewReader([]byte{4, 4})); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal("opened inode", len(got), err)
	}
	chunks, err := s.BatchArchiveContext(ctx, start, "/host/app", "archive.zip")
	if err != nil || !bytes.Equal(bytes.Join(chunks, nil), []byte{4, 4}) {
		t.Fatal("replacement", err)
	}
}

// archiveGateReader blocks its first Read until gate is closed.
type archiveGateReader struct {
	gate <-chan struct{}
	r    io.Reader
}

func (r archiveGateReader) Read(b []byte) (int, error) {
	<-r.gate
	return r.r.Read(b)
}

// Without the Store gate, batchArchiveConcurrent bounds the uploads in
// progress: one more waits for a slot until its ctx ends, and gets one when an
// upload finishes. Reads have their own slots.
func TestBatchArchiveConcurrencyBound(t *testing.T) {
	s, start := extraStore(t, Config{})
	gate := make(chan struct{})
	done := make(chan error, batchArchiveConcurrent)
	for i := 0; i < batchArchiveConcurrent; i++ {
		name := "slow" + string(rune('a'+i))
		go func() {
			done <- s.PutBatchArchiveContext(context.Background(), start, "/host/app", name, 1, archiveGateReader{gate, bytes.NewReader([]byte{1})})
		}()
	}
	for deadline := time.Now().Add(5 * time.Second); len(s.archiveUploads) != batchArchiveConcurrent; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			close(gate)
			t.Fatal("uploads did not start")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := s.PutBatchArchiveContext(ctx, start, "/host/app", "late", 1, bytes.NewReader([]byte{2}))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		close(gate)
		t.Fatal("upload past the bound", err)
	}
	if _, err = s.BatchArchiveContext(context.Background(), start, "/host/app", "missing.zip"); err != nil && !os.IsNotExist(err) {
		close(gate)
		t.Fatal("read waited for upload slots", err)
	}
	close(gate)
	for i := 0; i < batchArchiveConcurrent; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err = s.PutBatchArchiveContext(context.Background(), start, "/host/app", "late", 1, bytes.NewReader([]byte{2})); err != nil {
		t.Fatal(err)
	}
	if len(s.archiveUploads) != 0 || len(s.archiveReads) != 0 {
		t.Fatal("slots not released")
	}
}
