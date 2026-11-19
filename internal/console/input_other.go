//go:build !darwin && !linux

package console

import (
	"io"
	"os"
)

// OpenInput reads f line by line; the raw-mode editor needs termios.
func OpenInput(f *os.File, out io.Writer) (Input, error) {
	return plainOnly{NewPlainReader(f, out, nil)}, nil
}

type plainOnly struct{ *PlainReader }

func (plainOnly) Terminal() bool { return false }
