package main

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// createExitFile is Java Main's exit file: "<pid>.scouter" in dir (the
// working directory), else "<pid>.scouter.run" in home (user.home). Like
// File.createNewFile an existing file is reused. Both failing is an error.
func createExitFile(dir, home string, pid int) (string, error) {
	first := filepath.Join(dir, strconv.Itoa(pid)+".scouter")
	err := touch(first)
	if err == nil {
		return first, nil
	}
	if home == "" {
		return "", err
	}
	second := filepath.Join(home, strconv.Itoa(pid)+".scouter.run")
	err2 := touch(second)
	if err2 == nil {
		return second, nil
	}
	return "", errors.Join(err, err2)
}

func touch(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

// userHome is System.getProperty("user.home", "/tmp").
func userHome() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "/tmp"
}

// watchExitFile checks the file every interval until ctx ends; when it is
// gone (Java exit.exists()==false, which any stat failure also yields) it
// calls gone once and returns.
func watchExitFile(ctx context.Context, path string, interval time.Duration, gone func(reason string)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(path); err != nil {
				reason := "was deleted"
				if !errors.Is(err, fs.ErrNotExist) {
					reason = "is not readable (" + err.Error() + ")"
				}
				gone(reason)
				return
			}
		}
	}
}

// removeExitFile is File.deleteOnExit at a normal exit.
func removeExitFile(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("exit file: %v", err)
	}
}
