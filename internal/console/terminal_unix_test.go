//go:build darwin || linux

package console

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestTerminalRawModeAndRestore(t *testing.T) {
	master, slave, err := openPTY()
	if err != nil {
		t.Skipf("pseudo terminal unavailable: %v", err)
	}
	defer master.Close()
	defer slave.Close()
	fd := int(slave.Fd())
	before, err := getTermios(fd)
	if err != nil {
		t.Fatal(err)
	}
	if before.Lflag&syscall.ICANON == 0 || before.Lflag&syscall.ECHO == 0 {
		t.Fatalf("new pty is not cooked: %x", before.Lflag)
	}
	out := &syncBuffer{}
	in, err := OpenInput(slave, out)
	if err != nil {
		t.Fatal(err)
	}
	if !in.Terminal() {
		t.Fatal("pty not detected as a terminal")
	}
	raw, _ := getTermios(fd)
	if raw.Lflag&(syscall.ICANON|syscall.ECHO|syscall.IEXTEN) != 0 || raw.Lflag&syscall.ISIG == 0 ||
		raw.Cc[syscall.VMIN] != 0 || raw.Cc[syscall.VTIME] != 1 {
		t.Fatalf("raw mode lflag=%x vmin=%d vtime=%d", raw.Lflag, raw.Cc[syscall.VMIN], raw.Cc[syscall.VTIME])
	}
	if _, err = master.Write([]byte("obj\t\r\x1b[A\r")); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, want := range []string{"obj", "obj"} {
		line, err := in.ReadLine(ctx, Prompt)
		if err != nil || line != want {
			t.Fatalf("line %q %v want %q", line, err, want)
		}
	}
	// Cancellation is observed within the 100ms VTIME tick.
	cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	start := time.Now()
	if _, err = in.ReadLine(cctx, Prompt); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("cancel took %v", d)
	}
	// Ctrl-D on an empty line is EOF.
	if _, err = master.Write([]byte{4}); err != nil {
		t.Fatal(err)
	}
	if _, err = in.ReadLine(ctx, Prompt); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err = in.Close(); err != nil {
		t.Fatal(err)
	}
	if err = in.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := getTermios(fd)
	if err != nil {
		t.Fatal(err)
	}
	if after.Iflag != before.Iflag || after.Oflag != before.Oflag || after.Cflag != before.Cflag || after.Lflag != before.Lflag || after.Cc != before.Cc {
		t.Fatalf("terminal not restored:\nbefore %+v\nafter  %+v", before, after)
	}
}

func fdNonblocking(t *testing.T, fd uintptr) bool {
	t.Helper()
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	return flags&syscall.O_NONBLOCK != 0
}

// TestPipeInputInterruptibleAndRestored: a non-terminal stdin is read
// through a pollable duplicate; Close interrupts a pending read and puts the
// shared descriptor back into blocking mode.
func TestPipeInputInterruptibleAndRestored(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	in, err := OpenInput(r, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if in.Terminal() {
		t.Fatal("pipe detected as terminal")
	}
	if _, err = w.Write([]byte("help\n")); err != nil {
		t.Fatal(err)
	}
	if line, err := in.ReadLine(context.Background(), ""); err != nil || line != "help" {
		t.Fatal(line, err)
	}
	// The reader goroutine is now blocked on the empty pipe.
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if _, err = in.ReadLine(cctx, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	start := time.Now()
	if err = in.Close(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Close waited %v for a blocked read", d)
	}
	if fdNonblocking(t, r.Fd()) {
		t.Fatal("stdin left non-blocking")
	}
}
