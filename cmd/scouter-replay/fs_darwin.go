package main

import "syscall"

func filesystemInfo(path string) (fsType string, avail, total uint64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(path, &st); err != nil {
		return "", 0, 0, err
	}
	b := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		b = append(b, byte(c))
	}
	return string(b), st.Bavail * uint64(st.Bsize), st.Blocks * uint64(st.Bsize), nil
}
