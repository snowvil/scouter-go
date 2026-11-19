//go:build race

package storage

// raceEnabled slows every memory access; timing bounds in tests widen.
const raceEnabled = true
