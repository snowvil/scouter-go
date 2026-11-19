package logger

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// clearOldLogs is Logger.clearOldLog: with log_rotation_enabled and
// log_keep_days > 0, every non-directory entry of log_dir named
// "server-<d>.<ext>" or "request-<d>.<ext>" (d = text between the prefix and
// the last '.', exactly 8 characters) whose date unit is more than keep_days
// before today is removed and "[scouter] delete log file : <absolute path>"
// is logged. Unparseable dates are kept; years before 2000 count as day 0.
// Unlike Java (D-L3), a listing failure does not stop later cleanups.
func (l *Logger) clearOldLogs(nowMs int64) {
	cfg := l.cfg.Load()
	if !cfg.Rotation || cfg.KeepDays <= 0 {
		return
	}
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			l.deleteErrors.Add(1)
			l.warn(&l.cleanupSink, err)
		}
		return
	}
	l.cleanupSink.warned = false
	nowUnit := l.cal.dateUnit(nowMs)
	for _, e := range entries {
		name := e.Name()
		if isDir(cfg.Dir, e) {
			continue
		}
		var prefix string
		switch {
		case strings.HasPrefix(name, "server-"):
			prefix = "server-"
		case strings.HasPrefix(name, "request-"):
			prefix = "request-"
		default:
			continue
		}
		x := strings.LastIndexByte(name, '.')
		if x < 0 {
			continue
		}
		unit, ok := parseDateUnit(name[len(prefix):x])
		if !ok || nowUnit-unit <= int64(cfg.KeepDays) {
			continue
		}
		path := filepath.Join(cfg.Dir, name)
		if err := os.Remove(path); err != nil {
			l.deleteErrors.Add(1)
			l.warn(&l.cleanupSink, err)
			continue
		}
		l.deleted.Add(1)
		ms := l.now().UnixMilli()
		l.enqueued.Add(1)
		l.handle(record{kind: kindServer, ms: ms, text: l.cal.datetime(ms) + " [scouter] delete log file : " + javaAbsPath(cfg.Dir, name), stdout: cfg.Trace})
	}
}

// isDir is File.isDirectory, which follows symbolic links.
func isDir(dir string, e os.DirEntry) bool {
	if e.Type()&fs.ModeSymlink == 0 {
		return e.IsDir()
	}
	info, err := os.Stat(filepath.Join(dir, e.Name()))
	return err == nil && info.IsDir()
}

// javaAbsPath is new File(dir).listFiles()[i].getAbsolutePath() on Unix: the
// dir path with repeated and trailing '/' removed (no "." resolution),
// resolved against the working directory when relative, then "/" + name.
func javaAbsPath(dir, name string) string {
	var sb strings.Builder
	prevSlash := false
	for i := 0; i < len(dir); i++ {
		c := dir[i]
		if c == '/' {
			if prevSlash {
				continue
			}
			prevSlash = true
		} else {
			prevSlash = false
		}
		sb.WriteByte(c)
	}
	p := sb.String()
	if len(p) > 1 {
		p = strings.TrimSuffix(p, "/")
	}
	if !strings.HasPrefix(p, "/") {
		wd, err := os.Getwd()
		if err == nil {
			if p == "" {
				p = wd
			} else {
				p = strings.TrimSuffix(wd, "/") + "/" + p
			}
		}
	}
	if p == "/" {
		return "/" + name
	}
	return p + "/" + name
}
