package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/snowvil/scouter-go/internal/storage/legacyindex"
)

// relinkCheckFor returns the data check for an index whose Long5 values point
// into a known data file. A crash may leave key records whose data never
// reached disk (power loss, no fsync); relinking them would turn a silently
// lost tail into failing queries, so such a relink is refused instead.
// Indexes without a mapping here relink on key-file structure alone.
func (s *Store) relinkCheckFor(path string) (legacyindex.RelinkCheck, func()) {
	return NewRelinkCheck(path, s.live.compressXLog.Load(), s.live.compressProfile.Load())
}

// NewRelinkCheck builds the data check for an index base path (no suffix).
// compressXLog/compressProfile are the process defaults used only when the
// date has no .conf file yet. The returned func releases the data handle.
func NewRelinkCheck(path string, compressXLog, compressProfile bool) (legacyindex.RelinkCheck, func()) {
	dir, base := filepath.Split(path)
	dir = filepath.Clean(dir)
	var data, conf, key string
	var prefix int
	var fallback bool
	switch {
	case base == "xlog_tim" || base == "xlog_tid" || base == "xlog_gid":
		data, prefix = filepath.Join(dir, "xlog.service"), 2
		conf, key, fallback = filepath.Join(dir, "xlog.service.conf"), "compress_xlog_enabled", compressXLog
	case base == "xlog_profile":
		data, prefix = filepath.Join(dir, "xlog.profile"), 4
		conf, key, fallback = filepath.Join(dir, "xlog.profile.conf"), "compress_profile_enabled", compressProfile
	case base == "obj" && filepath.Base(dir) == "object":
		data, prefix = filepath.Join(dir, "obj.data"), 4
	case strings.HasPrefix(base, "text_") && filepath.Base(dir) == "text":
		data, prefix = path+".data", 4
	default:
		return nil, func() {}
	}
	c := &dataCheck{path: data, prefix: prefix}
	if conf != "" {
		if zip, present, err := loadCompressionMode(conf, key, fallback, false); err == nil && (zip || !present && fallback) {
			c.path, c.zipCount = filepath.Join(dir, "data", "count.dat"), true
		}
	}
	return c.check, c.close
}

type dataCheck struct {
	path     string
	prefix   int
	zipCount bool
	f        *os.File
	end      int64
	err      error
}

func (c *dataCheck) load() error {
	if c.f != nil || c.err != nil {
		return c.err
	}
	if c.zipCount {
		b, err := limitedFile(c.path, 8)
		if err == nil && len(b) != 8 {
			err = ErrCorrupt
		}
		if err == nil {
			c.end = int64(binary.BigEndian.Uint64(b))
		}
		c.err = err
		return err
	}
	f, err := os.Open(c.path)
	if err == nil {
		var st os.FileInfo
		if st, err = f.Stat(); err == nil {
			c.f, c.end = f, st.Size()
		} else {
			f.Close()
		}
	}
	c.err = err
	return err
}

func (c *dataCheck) check(_, value []byte) error {
	pos, err := dataOffset(value)
	if err != nil {
		return err
	}
	if err = c.load(); err != nil {
		return fmt.Errorf("data %s: %w", c.path, err)
	}
	if c.zipCount { // logical GZipStore offset; record bytes live in blocks
		if pos >= c.end {
			return fmt.Errorf("record offset %d beyond compressed count %d", pos, c.end)
		}
		return nil
	}
	if pos+int64(c.prefix) > c.end {
		return fmt.Errorf("record offset %d beyond data end %d", pos, c.end)
	}
	var head [4]byte
	if _, err = c.f.ReadAt(head[:c.prefix], pos); err != nil {
		return err
	}
	n := int64(int32(binary.BigEndian.Uint32(head[:])))
	if c.prefix == 2 {
		n = int64(int16(binary.BigEndian.Uint16(head[:2])))
	}
	if n < 0 || pos+int64(c.prefix)+n > c.end {
		return fmt.Errorf("record at %d of %d bytes beyond data end %d", pos, n, c.end)
	}
	return nil
}

func (c *dataCheck) close() {
	if c.f != nil {
		c.f.Close()
	}
}
