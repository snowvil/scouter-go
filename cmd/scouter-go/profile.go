package main

import (
	"errors"
	"os"
	"path/filepath"
	goruntime "runtime"
	"runtime/pprof"
	"strconv"
)

// startProfiles enables the measurement-only --profile-dir option: a CPU
// profile for the whole run plus block, mutex and heap profiles written at
// shutdown into <dir>/<pid>.
// The Store gate is channel based, so its waits appear in the block profile.
func startProfiles(dir string) (func() error, error) {
	// One subdirectory per process: a restart with the same flags keeps the
	// earlier run's profiles.
	dir = filepath.Join(dir, strconv.Itoa(os.Getpid()))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	cpu, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		return nil, err
	}
	if err = pprof.StartCPUProfile(cpu); err != nil {
		cpu.Close()
		return nil, err
	}
	goruntime.SetBlockProfileRate(int(1e6)) // sample blocking events of ~1ms
	goruntime.SetMutexProfileFraction(10)
	return func() error {
		pprof.StopCPUProfile()
		errs := []error{cpu.Close()}
		for _, name := range []string{"block", "mutex", "heap"} {
			f, err := os.Create(filepath.Join(dir, name+".pprof"))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			errs = append(errs, pprof.Lookup(name).WriteTo(f, 0), f.Close())
		}
		return errors.Join(errs...)
	}, nil
}
