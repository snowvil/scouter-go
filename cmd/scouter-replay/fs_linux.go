package main

import (
	"fmt"
	"syscall"
)

func filesystemInfo(path string) (fsType string, avail, total uint64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(path, &st); err != nil {
		return "", 0, 0, err
	}
	return fmt.Sprintf("magic:0x%x", st.Type), st.Bavail * uint64(st.Bsize), st.Blocks * uint64(st.Bsize), nil
}
