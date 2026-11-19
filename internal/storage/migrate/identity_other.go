// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin && !linux

package migrate

import "io/fs"

// identitySupported is false: without ctime and inode, precopy/finish would
// trust size, mtime and mode only, so they refuse to run here.
const identitySupported = false

// fileIdentity is unavailable on this platform.
func fileIdentity(fs.FileInfo) (ctime int64, ino uint64) { return 0, 0 }
