package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ReloadInterval is the Configure thread period (Configure.java:569-574) and
// the reload(false) gate (Configure.java:593-596).
const ReloadInterval = 3 * time.Second

// Options configures a Store. Zero values select the production behavior.
type Options struct {
	// Lookup resolves ${name} and is the fallback for keys missing from the
	// file (Java merges System.getenv into the Properties). Default
	// os.LookupEnv.
	Lookup func(string) (string, bool)
	// HostName is the server_id default (SysJMX.getHostName). Default
	// os.Hostname.
	HostName string
	// ExtraKeys are known keys outside Configure fields (e.g. the Pulse plugin
	// keys); they are not reported as unknown.
	ExtraKeys []string
	// RestartKeys are extra keys the Go server reads only at startup.
	RestartKeys []string
	// Pinned maps keys set by an explicit command-line flag to the flag
	// value (flag > scouter.conf). Snapshot.Live uses the pinned value; a
	// file change of a pinned key is logged once and not applied.
	Pinned map[string]string
	// OnChange is called by every Reload after the initial load that changes
	// the file entries, while the reload lock is held: it must not call
	// Reload or SaveText.
	OnChange func(Change)
	// Logf receives reload diagnostics. Default log.Printf.
	Logf func(string, ...any)
	// TelegrafPath is scouter-telegraf.xml (Configure.getTgFile; Java always
	// uses ./conf/scouter-telegraf.xml). Its text and mtime are re-read with
	// scouter.conf so each Snapshot holds one consistent pair. Empty disables.
	TelegrafPath string
	// Interval overrides ReloadInterval (tests).
	Interval time.Duration
	// Now overrides time.Now (tests).
	Now func() time.Time
}

// Change reports one reload that saw a modified file.
type Change struct {
	// Applied keys now have their new value in Current().
	Applied []string
	// Ignored keys need a restart; Current() keeps the startup value.
	Ignored []string
	// Unknown keys are not Configure settings; they are kept and logged.
	Unknown []string
	// Telegraf is set when scouter-telegraf.xml appeared, disappeared or
	// changed; consumers recompose it with the conf deprecated keys.
	Telegraf bool
}

func (c Change) Empty() bool { return len(c.Applied) == 0 && len(c.Ignored) == 0 && !c.Telegraf }

// Store owns one scouter.conf path: it loads it, polls its stat every
// ReloadInterval while Watch runs, and replaces it on SaveText. Readers take
// immutable Snapshots from Current without locking.
type Store struct {
	path    string
	opts    Options
	known   map[string]bool
	restart map[string]bool

	mu        sync.Mutex // Java: synchronized reload
	loaded    bool
	lastCheck time.Time
	stamp     fileStamp
	tgStamp   fileStamp
	lastFile  map[string]string // entries of the last read, before restart pinning
	pinWarned map[string]bool   // pinned keys whose file change was logged
	current   atomic.Pointer[Snapshot]
}

type fileStamp struct {
	exists bool
	info   os.FileInfo
}

// same is Configure.reloadConfig's lastModified comparison, extended with
// size and file identity so a rename-replaced file within the same mtime
// granularity is still seen (CONTRACT.md).
func (a fileStamp) same(b fileStamp) bool {
	if a.exists != b.exists {
		return false
	}
	if !a.exists {
		return true
	}
	return a.info.ModTime().Equal(b.info.ModTime()) && a.info.Size() == b.info.Size() && os.SameFile(a.info, b.info)
}

// Open loads path once. A missing file is not an error: every key takes its
// default, as Configure does; Snapshot.Exists reports it. A file that exists
// but cannot be read (permission, size limit, not a regular file) is an error
// so a broken startup configuration is not silently replaced by defaults.
func Open(path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("config: empty path")
	}
	if opts.Lookup == nil {
		opts.Lookup = os.LookupEnv
	}
	if opts.HostName == "" {
		opts.HostName, _ = os.Hostname()
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.Interval <= 0 {
		opts.Interval = ReloadInterval
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Store{path: path, opts: opts, known: map[string]bool{}, restart: map[string]bool{}, pinWarned: map[string]bool{}}
	pinned := make(map[string]string, len(opts.Pinned))
	for k, v := range opts.Pinned {
		pinned[k] = v
	}
	for _, k := range Keys {
		s.known[k.Name] = true
		s.restart[k.Name] = k.Restart
	}
	for _, k := range accountKeys {
		s.known[k] = true
	}
	for _, k := range goKeys {
		s.known[k] = true
	}
	for _, k := range goLimitKeys {
		s.known[k.name] = true
		s.restart[k.name] = k.restart
	}
	for _, k := range opts.ExtraKeys {
		s.known[k] = true
	}
	for _, k := range opts.RestartKeys {
		s.known[k] = true
		s.restart[k] = true
	}
	s.current.Store(&Snapshot{values: map[string]string{}, lookup: opts.Lookup, hostName: opts.HostName, telegraf: TelegrafFile{Path: opts.TelegrafPath}, pinned: pinned})
	// A malformed \uXXXX was logged and the entries before it applied, as
	// Java does; any other failure stops the startup.
	if _, err := s.Reload(true); err != nil && !errors.Is(err, ErrMalformedUnicode) {
		return nil, err
	}
	return s, nil
}

// Path is the configuration file path.
func (s *Store) Path() string { return s.path }

// Current returns the active snapshot.
func (s *Store) Current() *Snapshot { return s.current.Load() }

// Known reports whether key is a Configure, account or extra key, or a
// input_telegraf_$<measurement>$_<option> key.
func (s *Store) Known(key string) bool {
	if s.known[key] {
		return true
	}
	if rest, ok := strings.CutPrefix(key, "input_telegraf_$"); ok {
		return strings.Contains(rest, "$_")
	}
	return false
}

// Reload is Configure.reload(force): without force a call within Interval of
// the previous check does nothing; otherwise scouter.conf and
// scouter-telegraf.xml are re-read when their stat changed, and one new
// Snapshot holds both. A read failure keeps the previous content (Java keeps
// it when canRead fails); a malformed \uXXXX applies the entries before it
// like Java and is returned once per file version.
func (s *Store) Reload(force bool) (Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.opts.Now()
	if !force && s.loaded && now.Before(s.lastCheck.Add(s.opts.Interval)) {
		return Change{}, nil
	}
	s.lastCheck = now
	first := !s.loaded
	confStamp, err := s.stat(s.path)
	if err != nil {
		return Change{}, err
	}
	confChanged := first || !confStamp.same(s.stamp)
	var tgStamp fileStamp
	tgChanged := false
	if s.opts.TelegrafPath != "" {
		var tgErr error
		if tgStamp, tgErr = s.stat(s.opts.TelegrafPath); tgErr != nil {
			s.opts.Logf("config: %v; keeping the loaded telegraf file", tgErr)
		} else {
			tgChanged = first || !tgStamp.same(s.tgStamp)
		}
	}
	if !confChanged && !tgChanged {
		return Change{}, nil
	}
	old := s.Current()
	next := *old
	var change Change
	var result error
	if confChanged {
		s.stamp = confStamp
		if !confStamp.exists {
			if !first {
				s.opts.Logf("config: %s is missing; keeping the loaded values", s.path)
			} else {
				// No file: the go_* limits still fall back to the
				// environment (Snapshot.Get), so check them once.
				logInvalidGoLimits(s.envValue, false, s.opts.Logf)
			}
		} else if data, err := readBounded(s.path); err != nil {
			if first {
				return Change{}, err
			}
			s.opts.Logf("config: read %s: %v; keeping the loaded values", s.path, err)
			result = err
		} else {
			props, parseErr := parseProperties(data)
			if parseErr != nil {
				s.opts.Logf("config: %s: %v; applying the %d entries before it", s.path, parseErr, props.Len())
			}
			values := ReplaceSysProp(props.Map(), s.opts.Lookup)
			var effective map[string]string
			change, effective = s.apply(first, old, values, props.Keys())
			next.Exists, next.values, next.unknown = true, effective, change.Unknown
			result = parseErr
		}
	}
	if tgChanged {
		s.tgStamp = tgStamp
		tf := TelegrafFile{Path: s.opts.TelegrafPath, Exists: tgStamp.exists}
		if tgStamp.exists {
			tf.ModTime = tgStamp.info.ModTime()
			if data, err := readBounded(s.opts.TelegrafPath); err != nil {
				s.opts.Logf("config: read %s: %v; keeping the loaded telegraf file", s.opts.TelegrafPath, err)
				tf = old.telegraf
			} else {
				tf.Text = string(data)
			}
		}
		next.telegraf = tf
		change.Telegraf = !first
	}
	s.loaded = true
	s.current.Store(&next)
	if !first && !change.Empty() && s.opts.OnChange != nil {
		s.opts.OnChange(change)
	}
	return change, result
}

func (s *Store) apply(first bool, old *Snapshot, values map[string]string, order []string) (Change, map[string]string) {
	var change Change
	for _, k := range order {
		if !s.Known(k) {
			change.Unknown = append(change.Unknown, k)
		}
	}
	// Changes are found against the previous file entries, so a pending
	// restart-only edit is reported once, not on every later touch.
	keys := make(map[string]bool, len(values)+len(s.lastFile))
	for k := range values {
		keys[k] = true
	}
	for k := range s.lastFile {
		keys[k] = true
	}
	changed := make([]string, 0)
	for k := range keys {
		nv, nok := values[k]
		ov, ook := s.lastFile[k]
		if nok != ook || nv != ov {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	s.lastFile = values
	effective := make(map[string]string, len(values))
	for k, v := range values {
		effective[k] = v
	}
	if !first {
		// Restart-only keys keep the running (startup) value.
		for k := range keys {
			if !s.restart[k] {
				continue
			}
			if ov, ok := old.values[k]; ok {
				effective[k] = ov
			} else {
				delete(effective, k)
			}
		}
	}
	for _, k := range changed {
		if first || !s.restart[k] {
			change.Applied = append(change.Applied, k)
			continue
		}
		nv, nok := values[k]
		ov, ook := old.values[k]
		if nok == ook && nv == ov {
			continue // edited back to the running value
		}
		change.Ignored = append(change.Ignored, k)
	}
	if !first {
		for _, k := range changed {
			if _, ok := old.pinned[k]; ok && !s.pinWarned[k] {
				s.pinWarned[k] = true
				s.opts.Logf("config: %s changed in %s; it is fixed by a command-line flag, the running server keeps the flag value", k, s.path)
			}
		}
		for _, k := range change.Ignored {
			s.opts.Logf("config: %s changed in %s; restart required, the running server keeps the previous value", k, s.path)
		}
		// The log names only keys whose new value is in force: a pinned
		// key keeps its flag value (reported once above).
		var reloaded []string
		for _, k := range change.Applied {
			if _, pinned := old.pinned[k]; !pinned {
				reloaded = append(reloaded, k)
			}
		}
		if len(reloaded) > 0 {
			s.opts.Logf("config: reloaded %s: %s", s.path, strings.Join(reloaded, ","))
		}
	}
	for _, k := range change.Unknown {
		if first || containsString(changed, k) {
			s.opts.Logf("config: unknown key %s in %s is kept but not used", k, s.path)
		}
	}
	// A go_* limit with an invalid value is logged when it appears or
	// changes (the environment fallback once, at startup).
	logInvalidGoLimits(func(key string) (string, bool) {
		if v, ok := values[key]; ok {
			return v, first || containsString(changed, key)
		}
		if first {
			return s.envValue(key)
		}
		return "", false
	}, !first, s.opts.Logf)
	return change, effective
}

// envValue is the environment fallback of Snapshot.Get.
func (s *Store) envValue(key string) (string, bool) {
	v, ok := s.opts.Lookup(key)
	return JavaTrim(v), ok
}

func (s *Store) stat(path string) (fileStamp, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return fileStamp{}, nil
	}
	if err != nil {
		return fileStamp{}, err
	}
	if !info.Mode().IsRegular() {
		return fileStamp{}, fmt.Errorf("config: %s is not a regular file", path)
	}
	return fileStamp{exists: true, info: info}, nil
}

// Watch runs the Configure thread loop: wait Interval, Reload(false), until
// ctx is done. It returns ctx.Err(); errors are logged and retried. The
// caller owns the goroutine and waits for Watch to return.
func (s *Store) Watch(ctx context.Context) error {
	timer := time.NewTimer(s.opts.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		// Errors were logged by Reload; like Java (last_load_time is set
		// before the read), a failed read is retried only when the file
		// changes again.
		_, _ = s.Reload(false)
		// Java sleeps 3s after each reload, so the gate never skips a tick.
		timer.Reset(s.opts.Interval)
	}
}

// LoadText is Configure.loadText: the whole file as stored. exists is false
// for a missing file (Java returns null; GET_CONFIGURE_SERVER sends "").
func (s *Store) LoadText() (text string, exists bool, err error) {
	data, err := readBounded(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(data), true, nil
}

// SaveText is Configure.saveText made atomic: the text is written to a
// temporary file in the same directory, synced and renamed over the target
// (through a symlink to its final path), so a reader or a crash sees either
// the old or the new file. The parent directory is created like Java mkdirs.
// Callers apply it with Reload(true).
func (s *Store) SaveText(text string) error { return saveAtomic(s.path, text) }

// SaveTelegrafText replaces scouter-telegraf.xml the same way (Java
// saveTgConfigContents, without content validation); callers apply it with
// Reload(true).
func (s *Store) SaveTelegrafText(text string) error {
	if s.opts.TelegrafPath == "" {
		return errors.New("config: no telegraf path")
	}
	return saveAtomic(s.opts.TelegrafPath, text)
}

func saveAtomic(path, text string) error {
	if len(text) > MaxFileBytes {
		return ErrTooLarge
	}
	target := path
	if resolved, err := filepath.EvalSymlinks(target); err == nil {
		target = resolved
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	mode := fs.FileMode(0o644)
	if info, err := os.Stat(target); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("config: %s is not a regular file", target)
		}
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(target)+".*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			os.Remove(name)
		}
	}()
	_, err = io.Copy(tmp, strings.NewReader(text))
	if err == nil {
		err = tmp.Chmod(mode)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, target); err != nil {
		return err
	}
	committed = true
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync() // best effort: the rename is already visible
		d.Close()
	}
	return nil
}

func readBounded(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, io.LimitReader(f, MaxFileBytes+1)); err != nil {
		return nil, err
	}
	if buf.Len() > MaxFileBytes {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
