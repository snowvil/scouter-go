//go:build !darwin && !linux

package main

import "errors"

func filesystemInfo(string) (string, uint64, uint64, error) {
	return "", 0, 0, errors.New("filesystem statistics not implemented on this OS")
}
