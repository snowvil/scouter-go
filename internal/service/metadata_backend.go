package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// MetadataProperty is an actual Go server configuration property. Callers
// supply only configured values; Java-only settings must not be fabricated.
// Type uses Scouter ValueType: VALUE=1, NUM=2, BOOL=3, CSV=4, CSV-pairs=5.
type MetadataProperty struct {
	Key, Value, Default, Description string
	Type                             int64
	TypeDescription                  []protocol.Entry
	// ValueKind selects the LIST_CONFIGURE_SERVER value type like
	// ConfigValueUtil.toValue: "int"/"long" → DecimalValue, "bool" →
	// BooleanValue, anything else → TextValue. NullDefault sends NullValue in
	// the default column (null field initializer). NoDescription omits the
	// key from CONFIGURE_DESC (no @ConfigDesc annotation).
	ValueKind     string
	NullDefault   bool
	NoDescription bool
}
type MetadataLoginUser struct {
	Session               int64
	ID, IP, Version, Host string
	LoginTime             int64
}
type MetadataFile struct {
	Name               string
	Size, LastModified int64
}
type MetadataDatabase struct {
	Directories []MetadataFile
	Free, Total int64
}

// MetadataBackend owns read-only server configuration and file snapshots.
// Logs returns exists=false for the Java no-response missing-directory case.
// Text selectors are server, telegraf, counters-site; nil capability must fail.
type MetadataBackend interface {
	Configuration(context.Context) ([]MetadataProperty, error)
	ConfigurationText(context.Context, string) (string, error)
	Logs(context.Context) ([]MetadataFile, bool, error)
	Log(context.Context, string) (string, bool, error)
	Database(context.Context) (MetadataDatabase, error)
}

// MetadataAlertScriptBackend reads existing rule files; it does not compile or run Java.
type MetadataAlertScriptBackend interface {
	AlertScript(context.Context, string, bool) (string, error)
}

type FileMetadataConfig struct {
	DatabaseDir, LogDir, PluginDir string
	TelegrafFile                   string
	// ServerConfigFile is scouter.conf (Configure.getPropertyFile). When set,
	// GET_CONFIGURE_SERVER's serverConfig is the file text (Configure.loadText)
	// and SET_CONFIGURE_SERVER replaces it.
	ServerConfigFile string
	// CountersSite owns counters.site.xml; nil leaves counters-site text to
	// CountersSiteText and refuses its mutations.
	CountersSite *counters.Store
	// ValidateTelegraf, when set, checks a SET_CONFIGURE_TELEGRAF text
	// before it is saved (internal/telegraf.ParseFile). A failure is
	// ErrConfigurationRejected: nothing is saved and the answer is "false".
	// Java saves any text and answers "true" (intended difference).
	ValidateTelegraf func(text string) error
	// Reload is called after a successful write ("server", "telegraf",
	// "counters-site"), like Configure.reload(true). It may be nil; its
	// error is logged, not returned (Java answers true once saved).
	Reload     func(ctx context.Context, kind string) error
	Properties []MetadataProperty
	// Optional persisted file texts, supplied by their configuration owner.
	// A nil pointer means unavailable, rather than an invented empty success.
	TelegrafText, CountersSiteText     *string
	MaxEntries, MaxDepth, MaxTextBytes int
}

// FileMetadata never writes database/log files. os.Root anchors every access;
// symlinks are excluded and log detail additionally uses O_NOFOLLOW. Iteration
// is deterministic: Java File.listFiles has no specified sibling order.
type FileMetadata struct {
	config FileMetadataConfig
	// writeMu serializes configuration/rule file replacement.
	writeMu sync.Mutex
	alerts  *alertScriptMessages
	// logDir is the live log_dir after SetLogDir; nil keeps config.LogDir.
	logDir atomic.Pointer[string]
}

// SetLogDir replaces the log_dir that SERVER_LOG_LIST/SERVER_LOG_DETAIL read
// (ServerInfo.scala:86,110 read Configure.log_dir per request). The next
// request uses it. An empty dir answers like a missing directory: Java
// new File("").exists() is false, so SERVER_LOG_LIST answers nothing. (Java
// SERVER_LOG_DETAIL would resolve new File("", name) against "/"; Go reads
// nothing — intended difference.)
func (b *FileMetadata) SetLogDir(dir string) { b.logDir.Store(&dir) }

// currentLogDir returns the live log_dir and whether log queries are
// configured at all (config.LogDir "" without SetLogDir is unsupported).
func (b *FileMetadata) currentLogDir() (string, bool) {
	if p := b.logDir.Load(); p != nil {
		return *p, true
	}
	return b.config.LogDir, b.config.LogDir != ""
}

func NewFileMetadata(config FileMetadataConfig) (*FileMetadata, error) {
	if config.MaxEntries == 0 {
		config.MaxEntries = 4096
	}
	if config.MaxDepth == 0 {
		config.MaxDepth = 64
	}
	if config.MaxTextBytes == 0 {
		config.MaxTextBytes = 4 << 20
	}
	if config.MaxEntries < 1 || config.MaxEntries > 65536 || config.MaxDepth < 1 || config.MaxDepth > 256 || config.MaxTextBytes < 1 || config.MaxTextBytes > 4<<20 {
		return nil, fmt.Errorf("metadata: invalid limits")
	}
	var err error
	config.Properties, err = metadataPropertySnapshot(config.Properties)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, p := range config.Properties {
		if p.Key == "" || strings.ContainsAny(p.Key, "\r\n=") || seen[p.Key] || strings.ContainsAny(p.Value, "\r\n") || p.Type < 1 || p.Type > 5 {
			return nil, fmt.Errorf("metadata: invalid property")
		}
		seen[p.Key] = true
	}
	for _, text := range []**string{&config.TelegrafText, &config.CountersSiteText} {
		if *text != nil {
			owned := **text
			if len(owned) > config.MaxTextBytes {
				return nil, transport.ErrLimit
			}
			*text = &owned
		}
	}
	return &FileMetadata{config: config, alerts: newAlertScriptMessages()}, nil
}
func (b *FileMetadata) Configuration(ctx context.Context) ([]MetadataProperty, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.config.Properties == nil {
		return nil, transport.ErrUnsupported
	}
	return metadataPropertySnapshot(b.config.Properties)
}
func (b *FileMetadata) ConfigurationText(ctx context.Context, kind string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var text *string
	switch kind {
	case "server":
		if b.config.ServerConfigFile != "" {
			// Configure.loadText: the whole file; a missing file is null -> "".
			data, _, err := counters.ReadFile(filepath.Dir(b.config.ServerConfigFile), filepath.Base(b.config.ServerConfigFile), b.config.MaxTextBytes)
			if err != nil {
				return "", err
			}
			return strings.ToValidUTF8(string(data), "\uFFFD"), nil
		}
		if b.config.Properties == nil {
			return "", transport.ErrUnsupported
		}
		var out strings.Builder
		for _, p := range b.config.Properties {
			if out.Len()+len(p.Key)+len(p.Value)+2 > b.config.MaxTextBytes {
				return "", transport.ErrLimit
			}
			out.WriteString(p.Key)
			out.WriteByte('=')
			out.WriteString(p.Value)
			out.WriteByte('\n')
		}
		return out.String(), nil
	case "telegraf":
		if b.config.TelegrafFile != "" {
			loaded, found, err := b.readFile(ctx, filepath.Dir(b.config.TelegrafFile), filepath.Base(b.config.TelegrafFile))
			if err != nil {
				return "", err
			}
			if found {
				return loaded, nil
			}
		}
		text = b.config.TelegrafText
	case "counters-site":
		if b.config.CountersSite != nil {
			return b.config.CountersSite.Text(ctx)
		}
		text = b.config.CountersSiteText
	default:
		return "", transport.ErrUnsupported
	}
	if text == nil {
		return "", transport.ErrUnsupported
	}
	return *text, nil
}
func metadataNames(root *os.Root, name string, max int) ([]os.DirEntry, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(max + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > max {
		return nil, transport.ErrLimit
	}
	// Preserve depth-first completion order, with portable deterministic siblings.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}
func (b *FileMetadata) Logs(ctx context.Context) ([]MetadataFile, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	dir, ok := b.currentLogDir()
	if !ok {
		return nil, false, transport.ErrUnsupported
	}
	if dir == "" {
		return nil, false, nil
	}
	root, err := os.OpenRoot(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer root.Close()
	entries, err := metadataNames(root, ".", b.config.MaxEntries)
	if err != nil {
		return nil, true, err
	}
	files := []MetadataFile{}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		if !strings.HasSuffix(e.Name(), ".log") || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := root.Lstat(e.Name())
		if err != nil {
			return nil, true, err
		}
		if info.Mode().IsRegular() {
			files = append(files, MetadataFile{e.Name(), info.Size(), info.ModTime().UnixMilli()})
		}
	}
	return files, true, nil
}
func (b *FileMetadata) Log(ctx context.Context, name string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if name == "" {
		return "", false, nil
	}
	if name != filepath.Base(name) || strings.ContainsAny(name, "/\\") || name == "." || name == ".." || !strings.HasSuffix(name, ".log") {
		return "", false, badParam("name")
	}
	dir, ok := b.currentLogDir()
	if !ok {
		return "", false, transport.ErrUnsupported
	}
	if dir == "" {
		return "", false, nil
	}
	return b.readFile(ctx, dir, name)
}
func (b *FileMetadata) AlertScript(ctx context.Context, counterName string, configuration bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if counterName != "" && (counterName != filepath.Base(counterName) || strings.ContainsAny(counterName, "/\\") || counterName == "." || counterName == "..") {
		return "", badParam("counterName")
	}
	if b.config.PluginDir == "" {
		return "", transport.ErrUnsupported
	}
	suffix := ".alert"
	if configuration {
		suffix = ".conf"
	}
	text, _, err := b.readFile(ctx, b.config.PluginDir, counterName+suffix)
	return text, err
}
func (b *FileMetadata) readFile(ctx context.Context, dir, name string) (string, bool, error) {
	root, err := os.OpenRoot(dir)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() {
		return "", false, badParam("name")
	}
	if info.Size() > int64(b.config.MaxTextBytes) {
		return "", false, transport.ErrLimit
	}
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return "", false, err
	}
	if !opened.Mode().IsRegular() {
		return "", false, badParam("name")
	}
	if opened.Size() > int64(b.config.MaxTextBytes) {
		return "", false, transport.ErrLimit
	}
	content, err := io.ReadAll(io.LimitReader(file, int64(b.config.MaxTextBytes)+1))
	if err != nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if len(content) > b.config.MaxTextBytes {
		return "", false, transport.ErrLimit
	}
	return string(content), true, nil
}
func (b *FileMetadata) Database(ctx context.Context) (MetadataDatabase, error) {
	var out MetadataDatabase
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if b.config.DatabaseDir == "" {
		return out, transport.ErrUnsupported
	}
	root, err := os.OpenRoot(b.config.DatabaseDir)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer root.Close()
	count := 0
	var walk func(string, int) (int64, error)
	walk = func(dir string, depth int) (int64, error) {
		if depth > b.config.MaxDepth {
			return 0, transport.ErrLimit
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		entries, err := metadataNames(root, dir, b.config.MaxEntries-count)
		if err != nil {
			return 0, err
		}
		count += len(entries)
		var length int64
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			name := filepath.Join(dir, e.Name())
			info, err := root.Lstat(name)
			if err != nil {
				return 0, err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			var size int64
			if info.IsDir() {
				size, err = walk(name, depth+1)
				if err != nil {
					return 0, err
				}
				out.Directories = append(out.Directories, MetadataFile{"/" + filepath.ToSlash(name), size, info.ModTime().UnixMilli()})
			} else if info.Mode().IsRegular() {
				size = info.Size()
			}
			if size > math.MaxInt64-length {
				return 0, transport.ErrLimit
			}
			length += size
		}
		return length, nil
	}
	out.Total, err = walk(".", 0)
	if err != nil {
		return MetadataDatabase{}, err
	}
	dir, err := root.Open(".")
	if err != nil {
		return MetadataDatabase{}, err
	}
	defer dir.Close()
	var stat syscall.Statfs_t
	if err = syscall.Fstatfs(int(dir.Fd()), &stat); err != nil {
		return MetadataDatabase{}, err
	}
	if stat.Bsize > 0 && uint64(stat.Bavail) <= uint64(math.MaxInt64)/uint64(stat.Bsize) {
		out.Free = int64(stat.Bavail) * int64(stat.Bsize)
	} else {
		return MetadataDatabase{}, transport.ErrLimit
	}
	return out, nil
}

func metadataPropertySnapshot(properties []MetadataProperty) ([]MetadataProperty, error) {
	if properties == nil {
		return nil, nil
	}
	if len(properties) > 4096 {
		return nil, transport.ErrLimit
	}
	out := make([]MetadataProperty, len(properties))
	copy(out, properties)
	for i, p := range properties {
		if p.TypeDescription != nil {
			raw, err := protocol.EncodeValue(protocol.Value{Type: protocol.ValueMap, Entries: p.TypeDescription}, protocol.Limits{})
			if err != nil {
				return nil, err
			}
			v, err := protocol.DecodeValue(raw, protocol.Limits{})
			if err != nil {
				return nil, err
			}
			out[i].TypeDescription = v.Entries
		}
	}
	return out, nil
}

// SaveConfigurationText implements ConfigurationWriter. Kinds match
// ConfigurationText. server/telegraf replace the configured file
// (Configure.saveText/saveTgConfigContents, here a temporary file + rename
// that refuses symlink targets) and then call Reload; counters-site is
// CounterManager's validate+normalize+save+reload, where a parse rejection
// is ErrConfigurationRejected. An unconfigured target is ErrUnsupported.
// A Reload failure is logged and does not change the result: Java answers
// "true" once the text is saved (internal/server.ServerConfigFile does the
// same for "server").
func (b *FileMetadata) SaveConfigurationText(ctx context.Context, kind, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var path string
	var perm os.FileMode = 0o644
	switch kind {
	case "server":
		path, perm = b.config.ServerConfigFile, 0o600
	case "telegraf":
		path = b.config.TelegrafFile
	case "counters-site":
		if b.config.CountersSite == nil {
			return transport.ErrUnsupported
		}
	default:
		return transport.ErrUnsupported
	}
	if kind != "counters-site" && path == "" {
		return transport.ErrUnsupported
	}
	if len(text) > b.config.MaxTextBytes {
		return transport.ErrLimit
	}
	if kind == "telegraf" && b.config.ValidateTelegraf != nil {
		if err := b.config.ValidateTelegraf(text); err != nil {
			return fmt.Errorf("%w: telegraf: %v", ErrConfigurationRejected, err)
		}
	}
	b.writeMu.Lock()
	var err error
	if kind == "counters-site" {
		var ok bool
		if ok, err = b.config.CountersSite.Save(ctx, text); err == nil && !ok {
			err = ErrConfigurationRejected
		}
	} else {
		err = counters.WriteFile(filepath.Dir(path), filepath.Base(path), []byte(text), perm)
	}
	b.writeMu.Unlock()
	if err != nil {
		return err
	}
	b.reload(ctx, kind)
	return nil
}

func (b *FileMetadata) reload(ctx context.Context, kind string) {
	if b.config.Reload == nil {
		return
	}
	if err := b.config.Reload(ctx, kind); err != nil {
		log.Printf("metadata: reload after saving %s: %v", kind, err)
	}
}

// DefineObjectType implements CountersSiteStore.
func (b *FileMetadata) DefineObjectType(ctx context.Context, spec counters.ObjectTypeSpec) (bool, error) {
	return b.objectType(ctx, spec, false)
}

// EditObjectType implements CountersSiteStore.
func (b *FileMetadata) EditObjectType(ctx context.Context, spec counters.ObjectTypeSpec) (bool, error) {
	return b.objectType(ctx, spec, true)
}

func (b *FileMetadata) objectType(ctx context.Context, spec counters.ObjectTypeSpec, edit bool) (bool, error) {
	if b.config.CountersSite == nil {
		return false, transport.ErrUnsupported
	}
	b.writeMu.Lock()
	var ok bool
	var err error
	if edit {
		ok, err = b.config.CountersSite.EditObjectType(ctx, spec)
	} else {
		ok, err = b.config.CountersSite.DefineObjectType(ctx, spec)
	}
	b.writeMu.Unlock()
	if err != nil || !ok {
		return ok, err
	}
	b.reload(ctx, "counters-site")
	return true, nil
}

func validAlertCounterName(counterName string) bool {
	return counterName == "" || counterName == filepath.Base(counterName) && !strings.ContainsAny(counterName, "/\\\x00") && counterName != "." && counterName != ".."
}

// SaveAlertScript implements AlertScriptWriter: AlertRuleLoader.saveRuleContents
// / saveConfigContents into plugin_dir/<counterName>.alert|.conf. The file is
// saved only; Java compilation/execution is not implemented and the
// load-message cache says so.
func (b *FileMetadata) SaveAlertScript(ctx context.Context, counterName string, configuration bool, contents string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !validAlertCounterName(counterName) {
		return false, badParam("counterName")
	}
	if b.config.PluginDir == "" {
		return false, transport.ErrUnsupported
	}
	if len(contents) > b.config.MaxTextBytes {
		return false, transport.ErrLimit
	}
	name := counterName + alertRuleSuffix
	if configuration {
		name = counterName + alertConfSuffix
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if err := counters.WriteFile(b.config.PluginDir, name, []byte(contents), 0o644); err != nil {
		return false, err
	}
	b.alerts.saved(b.config.PluginDir, name)
	return true, nil
}

// AlertScriptLoadMessages implements AlertScriptMessageSource. Rule-file
// changes under PluginDir are detected when the cache is read, in place of
// AlertRuleLoader's 5 second loop.
func (b *FileMetadata) AlertScriptLoadMessages(ctx context.Context, loop int64, index int32) (AlertScriptLoadMessages, error) {
	if err := ctx.Err(); err != nil {
		return AlertScriptLoadMessages{}, err
	}
	if b.config.PluginDir != "" {
		if err := b.alerts.scan(b.config.PluginDir, b.config.MaxEntries); err != nil {
			return AlertScriptLoadMessages{}, err
		}
	}
	return b.alerts.get(loop, index)
}

// CustomCounterXML is CounterManager.getXmlCustomContent for GET_XML_COUNTER:
// the current counters.site.xml bytes, nil without a store or file.
func (b *FileMetadata) CustomCounterXML() []byte {
	if b == nil || b.config.CountersSite == nil {
		return nil
	}
	return b.config.CountersSite.CustomXML()
}
