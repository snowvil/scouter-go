// Copyright 2026 Scouter Go contributors.
// SPDX-License-Identifier: Apache-2.0

// Package migrate inspects, copies, verifies and rolls back Scouter file DBs.
// Original-compatible files are copied byte for byte; no format conversion is
// performed. The source DB is only opened with O_RDONLY descriptors.
package migrate

import (
	"fmt"
	"sort"
)

// ToolVersion is recorded in reports and manifests.
const ToolVersion = "scouter-db/1"

const (
	SeverityError   = "error"
	SeverityWarning = "warning"
	SeverityInfo    = "info"
)

// File status values. Only "checked" means every Go-supported rule ran.
const (
	StatusChecked       = "checked"
	StatusStructureOnly = "structure-only" // index structure checked, payload semantics not
	StatusUnchecked     = "unchecked"      // recognised Scouter data that Go does not verify
	StatusUnknown       = "unknown"        // not recognised; still copied and checksummed
	StatusExcluded      = "excluded"       // writer lock files; never copied
)

// maxDetail bounds repeated problems per (code, path); the rest are counted.
const maxDetail = 10

type Problem struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Path     string `json:"path,omitempty"`
	Offset   *int64 `json:"offset,omitempty"`
	Detail   string `json:"detail"`
	Count    int64  `json:"count,omitempty"` // suppressed repetitions summary
}

type FileEntry struct {
	Path     string `json:"path"` // slash separated, relative to the DB root
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256,omitempty"`
	Mode     uint32 `json:"mode"`
	MTime    int64  `json:"mtime_unix_nano"`
	Category string `json:"category"`
	Status   string `json:"status"`
	Records  *int64 `json:"records,omitempty"`
}

type Summary struct {
	Files     int                         `json:"files"`
	Dirs      int                         `json:"dirs"`
	Bytes     int64                       `json:"bytes"`
	Errors    int64                       `json:"errors"`
	Warnings  int64                       `json:"warnings"`
	Infos     int64                       `json:"infos"`
	Statuses  map[string]int              `json:"statuses"`
	Codes     map[string]int64            `json:"codes"`
	Dates     map[string]map[string]int64 `json:"dates"`
	Permanent map[string]int64            `json:"permanent"`
}

type Report struct {
	Tool     string      `json:"tool"`
	Root     string      `json:"root"`
	Timezone string      `json:"timezone"`
	Now      int64       `json:"kv_now_unix_seconds"`
	Started  string      `json:"started"`
	Finished string      `json:"finished"`
	Locks    LockState   `json:"locks"`
	Dirs     []string    `json:"dirs"`
	Files    []FileEntry `json:"files"`
	Problems []Problem   `json:"problems"`
	Summary  Summary     `json:"summary"`
}

// collector aggregates problems with bounded detail.
type collector struct {
	problems   []Problem
	seen       map[[2]string]int64
	suppressed map[[2]string]*Problem
	counts     map[string]int64
	bySev      map[string]int64
}

func newCollector() *collector {
	return &collector{seen: map[[2]string]int64{}, suppressed: map[[2]string]*Problem{}, counts: map[string]int64{}, bySev: map[string]int64{}}
}
func (c *collector) add(severity, code, path string, offset int64, format string, args ...any) {
	c.counts[code]++
	c.bySev[severity]++
	key := [2]string{code, path}
	c.seen[key]++
	if c.seen[key] > maxDetail {
		p := c.suppressed[key]
		if p == nil {
			p = &Problem{Severity: severity, Code: code, Path: path}
			c.suppressed[key] = p
		}
		p.Count++
		return
	}
	p := Problem{Severity: severity, Code: code, Path: path, Detail: fmt.Sprintf(format, args...)}
	if offset >= 0 {
		o := offset
		p.Offset = &o
	}
	c.problems = append(c.problems, p)
}
func (c *collector) finish() []Problem {
	keys := make([][2]string, 0, len(c.suppressed))
	for k := range c.suppressed {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	out := append(make([]Problem, 0, len(c.problems)+len(keys)), c.problems...)
	for _, k := range keys {
		p := *c.suppressed[k]
		p.Detail = fmt.Sprintf("%d additional occurrences not listed", p.Count)
		out = append(out, p)
	}
	return out
}

// HasErrors reports whether any error-severity problem was found.
func (r *Report) HasErrors() bool { return r.Summary.Errors > 0 }
