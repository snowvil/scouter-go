//go:build darwin || linux

package console

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

func getTermios(fd int) (syscall.Termios, error) {
	var t syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(ioctlGetTermios), uintptr(unsafe.Pointer(&t)))
	if errno != 0 {
		return t, errno
	}
	return t, nil
}

func setTermios(fd int, t *syscall.Termios) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(ioctlSetTermios), uintptr(unsafe.Pointer(t)))
	if errno != 0 {
		return errno
	}
	return nil
}

// OpenInput opens the console input on f. A terminal gets the raw-mode line
// editor (Tab completion over Words, in-memory history); anything else is
// read line by line through a non-blocking duplicate of the descriptor so
// Close can interrupt a pending read. Prompts are drawn on out.
func OpenInput(f *os.File, out io.Writer) (Input, error) {
	fd := int(f.Fd())
	if _, err := getTermios(fd); err == nil {
		return openTerminal(fd, out)
	}
	dup, err := syscall.Dup(fd)
	if err != nil {
		return nil, err
	}
	if err = syscall.SetNonblock(dup, true); err != nil {
		syscall.Close(dup)
		return nil, err
	}
	file := os.NewFile(uintptr(dup), f.Name())
	in := &plainInput{file: file, fd: dup}
	unblock := func() error { return nil }
	if file.SetReadDeadline(time.Time{}) == nil {
		unblock = func() error { return file.SetReadDeadline(time.Now()) }
	}
	in.PlainReader = NewPlainReader(file, out, unblock)
	return in, nil
}

// plainInput owns the duplicated descriptor of a non-terminal stdin.
type plainInput struct {
	*PlainReader
	file *os.File
	fd   int
	once sync.Once
}

func (p *plainInput) Terminal() bool { return false }

func (p *plainInput) Close() error {
	var err error
	p.once.Do(func() {
		err = p.PlainReader.Close()
		// O_NONBLOCK lives on the shared open file description: restore it
		// before the duplicate goes away so the parent's stdin is unchanged.
		err = errors.Join(err, syscall.SetNonblock(p.fd, false), p.file.Close())
	})
	return err
}

// terminal is the raw-mode input. ICANON, ECHO and IEXTEN are cleared;
// ISIG stays on, so Ctrl-C/Ctrl-\ still raise SIGINT/SIGQUIT and the normal
// signal shutdown restores the terminal through Close. VMIN=0/VTIME=1 make
// every read return within 100ms so ReadLine observes ctx.
type terminal struct {
	fd    int
	saved syscall.Termios
	ed    *editor
	once  sync.Once
}

func openTerminal(fd int, out io.Writer) (*terminal, error) {
	saved, err := getTermios(fd)
	if err != nil {
		return nil, err
	}
	raw := saved
	raw.Lflag &^= syscall.ICANON | syscall.ECHO | syscall.IEXTEN
	raw.Cc[syscall.VMIN] = 0
	raw.Cc[syscall.VTIME] = 1
	if err = setTermios(fd, &raw); err != nil {
		return nil, err
	}
	return &terminal{fd: fd, saved: saved, ed: newEditor(out, Words)}, nil
}

func (t *terminal) Terminal() bool { return true }

// ReadLine implements LineReader with line editing.
func (t *terminal) ReadLine(ctx context.Context, prompt string) (string, error) {
	t.ed.begin(prompt)
	if r, line := t.ed.feed(nil); r != editMore {
		return t.result(r, line)
	}
	var buf [256]byte
	for {
		if err := ctx.Err(); err != nil {
			t.ed.write("\n")
			return "", err
		}
		n, err := syscall.Read(t.fd, buf[:])
		switch {
		case errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN):
			continue
		case errors.Is(err, syscall.EIO):
			return "", io.EOF // the terminal hung up
		case err != nil:
			return "", err
		}
		var r editResult
		var line string
		if n <= 0 {
			r, line = t.ed.idle()
		} else {
			r, line = t.ed.feed(buf[:n])
		}
		if r != editMore {
			return t.result(r, line)
		}
	}
}

func (t *terminal) result(r editResult, line string) (string, error) {
	if r == editEOF {
		return "", io.EOF
	}
	return line, nil
}

// Close restores the saved terminal attributes.
func (t *terminal) Close() error {
	var err error
	t.once.Do(func() { err = setTermios(t.fd, &t.saved) })
	return err
}
