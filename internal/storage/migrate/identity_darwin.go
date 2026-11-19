// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0
package migrate

import (
	"io/fs"
	"syscall"
)

// fileIdentity returns the status-change time and inode of an lstat result.
// Every write, chmod, utimes and rename updates ctime and userland cannot set
// it, so an mtime-preserving rewrite still shows up.
// identitySupported: precopy/finish change detection is available.
const identitySupported = true

func fileIdentity(info fs.FileInfo) (ctime int64, ino uint64) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return st.Ctimespec.Nano(), st.Ino
}
