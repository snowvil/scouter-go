package console

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
)

// MaxLineBytes bounds one input line; a longer line is discarded whole.
const MaxLineBytes = 4096

// ErrLineTooLong reports a discarded input line; reading may continue.
var ErrLineTooLong = errors.New("console: input line too long")

// LineReader supplies input lines without the line terminator. It returns
// io.EOF when the input ends and ctx.Err() when ctx ends first.
type LineReader interface {
	ReadLine(ctx context.Context, prompt string) (string, error)
}

// Input is the console input opened on the process stdin.
type Input interface {
	LineReader
	// Close restores the terminal (or the descriptor flags) and stops the
	// reader goroutine. It is idempotent.
	Close() error
	// Terminal reports whether the raw-mode line editor is in use.
	Terminal() bool
}

// PlainReader is the non-terminal input: it prints the prompt and reads
// '\n'-terminated lines. One goroutine, started by the first ReadLine and
// owned by the reader, blocks on the source; it ends at EOF, on a read
// error, or when Close unblocks the source.
type PlainReader struct {
	src     io.Reader
	out     io.Writer
	unblock func() error

	start   sync.Once
	stop    chan struct{}
	stopped sync.Once
	results chan readResult
	done    chan struct{}
	mu      sync.Mutex
	eof     error
}

type readResult struct {
	line string
	err  error
}

// NewPlainReader reads lines from src and draws the prompt on out.
// unblock, when not nil, is called by Close to interrupt a blocked read
// (for example by setting a read deadline or closing a pipe).
func NewPlainReader(src io.Reader, out io.Writer, unblock func() error) *PlainReader {
	return &PlainReader{src: src, out: out, unblock: unblock, stop: make(chan struct{}), results: make(chan readResult), done: make(chan struct{})}
}

// ReadLine implements LineReader.
func (p *PlainReader) ReadLine(ctx context.Context, prompt string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	if p.eof != nil {
		err := p.eof
		p.mu.Unlock()
		return "", err
	}
	p.mu.Unlock()
	if prompt != "" {
		_, _ = io.WriteString(p.out, prompt)
	}
	p.start.Do(func() { go p.loop() })
	select {
	case r := <-p.results:
		if r.err != nil && !errors.Is(r.err, ErrLineTooLong) {
			p.mu.Lock()
			p.eof = r.err
			p.mu.Unlock()
		}
		return r.line, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (p *PlainReader) loop() {
	defer close(p.done)
	br := bufio.NewReaderSize(p.src, MaxLineBytes)
	for {
		line, err := readLine(br)
		select {
		case p.results <- readResult{line, err}:
		case <-p.stop:
			return
		}
		if err != nil && !errors.Is(err, ErrLineTooLong) {
			return
		}
	}
}

// readLine returns the next line without "\n" or "\r\n". A final line
// without a terminator is returned before io.EOF.
func readLine(br *bufio.Reader) (string, error) {
	b, err := br.ReadSlice('\n')
	switch {
	case err == nil:
		return strings.TrimSuffix(string(bytes.TrimSuffix(b, []byte{'\n'})), "\r"), nil
	case errors.Is(err, bufio.ErrBufferFull):
		for errors.Is(err, bufio.ErrBufferFull) {
			_, err = br.ReadSlice('\n')
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
		return "", ErrLineTooLong
	case len(b) > 0 && errors.Is(err, io.EOF):
		br.Reset(eofReader{})
		return strings.TrimSuffix(string(b), "\r"), nil
	default:
		return "", err
	}
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

// Close stops the reader goroutine. It unblocks a pending read through
// unblock and waits up to one second; a source that cannot be interrupted
// (a blocking descriptor without unblock) is reported instead of waited on.
func (p *PlainReader) Close() error {
	var err error
	p.stopped.Do(func() {
		close(p.stop)
		// Never started: mark done so nothing starts the goroutine later.
		p.start.Do(func() { close(p.done) })
		if p.unblock != nil {
			err = p.unblock()
		}
		select {
		case <-p.done:
		case <-time.After(time.Second):
			err = errors.Join(err, errors.New("console: input reader still blocked on read"))
		}
	})
	return err
}
