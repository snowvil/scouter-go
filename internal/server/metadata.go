package server

import (
	"context"
	"net"
	"strconv"

	"github.com/snowvil/scouter-go/internal/config"
	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/service"
	"github.com/snowvil/scouter-go/internal/storage"
	"github.com/snowvil/scouter-go/internal/telegraf"
)

// runtimeMetadataWith builds the Client metadata backend. Without a store the
// Go flag values are listed; with one the LIST/GET_CONFIGURE_SERVER family
// follows the live scouter.conf (Configure.getKeyValueInfo/loadText). site is
// the counters.site.xml store the runtime shares with the Telegraf/register
// registrar (nil: none, counters-site mutations are unsupported). SET_CONFIGURE_TELEGRAF texts are parsed before they are
// saved (intended difference: Java saves invalid XML and answers true).
func runtimeMetadataWith(cfg Config, conf *config.Store, reload func(context.Context, string) error, site *counters.Store) (service.MetadataBackend, error) {
	base := service.FileMetadataConfig{DatabaseDir: cfg.Directory, LogDir: cfg.LogDirectory, PluginDir: cfg.PluginDirectory, TelegrafFile: cfg.TelegrafConfigurationPath, TelegrafText: &telegrafSample, CountersSiteText: new(string), Reload: reload, CountersSite: site}
	base.ValidateTelegraf = func(text string) error {
		_, _, err := telegraf.ParseFile([]byte(text))
		return err
	}
	if conf != nil {
		base.ServerConfigFile = conf.Path()
	}
	if conf == nil {
		base.Properties = flagProperties(cfg)
		return service.NewFileMetadata(base)
	}
	// confMetadata answers Configuration and the "server" text itself, so
	// FileMetadata gets no property rows (a file value may hold a newline,
	// which its key=value rendering rejects).
	m := &confMetadata{ServerConfigFile: ServerConfigFile{Store: conf}, overlay: effectiveOverlay(cfg)}
	var err error
	if m.FileMetadata, err = service.NewFileMetadata(base); err != nil {
		return nil, err
	}
	return m, nil
}

func flagProperties(cfg Config) []service.MetadataProperty {
	return []service.MetadataProperty{
		{Key: "log_dir", Value: cfg.LogDirectory, Default: "./logs", Description: "Directory for server log read queries", Type: 1},
		{Key: "plugin_dir", Value: cfg.PluginDirectory, Default: "./plugin", Description: "Directory for alert script file read queries", Type: 1},
		{Key: "db_dir", Value: cfg.Directory, Default: "./database", Description: "Go server database directory", Type: 1},
		{Key: "udp", Value: cfg.UDP, Default: "127.0.0.1:6100", Description: "Go server UDP bind address", Type: 1},
		{Key: "tcp", Value: cfg.TCP, Default: "127.0.0.1:6100", Description: "Go server TCP bind address", Type: 1},
		{Key: "timezone", Value: cfg.Location.String(), Default: "Asia/Seoul", Description: "Storage and query calendar timezone", Type: 1},
		{Key: "max_connections", Value: strconv.Itoa(cfg.MaxConnections), Default: "1024", Description: "Maximum concurrent Client TCP connections (Agents have --max-agent-connections)", Type: 2},
		{Key: "compress_xlog", Value: strconv.FormatBool(cfg.CompressXLog), Default: "false", Description: "XLog record compression", Type: 3},
		{Key: "compress_profile", Value: strconv.FormatBool(cfg.CompressProfile), Default: "false", Description: "Profile record compression", Type: 3},
	}
}

// confMetadata serves the server configuration commands from a config.Store
// and every other metadata command from FileMetadata (AlertScript included).
type confMetadata struct {
	*service.FileMetadata
	ServerConfigFile
	overlay map[string]string
	// live returns the applied value of every live key (config.Live
	// Effective); it wins over the file value and the startup overlay.
	live func() map[string]string
}

func (m *confMetadata) Configuration(ctx context.Context) ([]service.MetadataProperty, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m.properties()
}

// ConfigurationText returns the scouter.conf text as stored for "server"
// (Configure.loadText; a missing file is ""), else the FileMetadata text.
func (m *confMetadata) ConfigurationText(ctx context.Context, kind string) (string, error) {
	if kind != "server" {
		return m.FileMetadata.ConfigurationText(ctx, kind)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	text, _, err := m.Store.LoadText()
	return text, err
}

// properties lists the Configure keys in declaration order. Keys the Go
// server consumed at startup show the running value, which can differ from
// the file when a flag overrides it or a restart is pending.
func (m *confMetadata) properties() ([]service.MetadataProperty, error) {
	// Bounded by the fixed Keys table (well under the service 4096 rows).
	rows := m.Store.Current().Properties()
	out := make([]service.MetadataProperty, 0, len(rows))
	var live map[string]string
	if m.live != nil {
		live = m.live()
	}
	for _, p := range rows {
		value := p.Value
		if v, ok := m.overlay[p.Key]; ok {
			value = v
		}
		if v, ok := live[p.Key]; ok {
			value = v
		}
		row := service.MetadataProperty{Key: p.Key, Value: value, Default: p.Default, Description: p.Desc, Type: int64(p.Type), ValueKind: kindName(p.Kind), NullDefault: p.NullDefault, NoDescription: !p.HasDesc}
		if p.TypeDesc != nil {
			row.TypeDescription = valueTypeDesc(p.TypeDesc)
		}
		out = append(out, row)
	}
	return out, nil
}

// effectiveOverlay maps the startup-only Configure keys to the values the
// runtime actually uses. Live keys are overlaid per request from
// config.Live.Effective (confMetadata.live).
func effectiveOverlay(cfg Config) map[string]string {
	o := map[string]string{
		"db_dir":                   cfg.Directory,
		"log_dir":                  cfg.LogDirectory,
		"plugin_dir":               cfg.PluginDirectory,
		"compress_xlog_enabled":    strconv.FormatBool(cfg.CompressXLog),
		"compress_profile_enabled": strconv.FormatBool(cfg.CompressProfile),
		"mgr_purge_enabled":        strconv.FormatBool(cfg.Retention != nil),
		"net_http_server_enabled":  strconv.FormatBool(cfg.ScouterHTTP != nil),
	}
	servletListen := ""
	if cfg.ScouterHTTP != nil {
		servletListen = cfg.ScouterHTTP.Listen
		if servletListen == "" {
			servletListen = cfg.HTTP // servlets mounted on the --http listener
		}
	}
	if cfg.ServerID != "" {
		o["server_id"] = cfg.ServerID
	}
	for prefix, addr := range map[string]string{"net_udp_listen_": cfg.UDP, "net_tcp_listen_": cfg.TCP, "net_http_": servletListen} {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		if prefix != "net_http_" {
			o[prefix+"ip"] = host
		}
		o[prefix+"port"] = port
	}
	if cfg.UDPReadBuffer > 0 {
		o["net_udp_so_rcvbuf_size"] = strconv.Itoa(cfg.UDPReadBuffer)
	}
	if r := cfg.Retention; r != nil {
		for k, v := range retentionValues(*r) {
			o[k] = strconv.Itoa(v)
		}
	}
	return o
}

func retentionValues(r storage.RetentionConfig) map[string]int {
	return map[string]int{
		"mgr_purge_disk_usage_pct":             r.DiskUsagePct,
		"mgr_purge_profile_keep_days":          r.ProfileDays,
		"mgr_purge_xlog_keep_days":             r.XLogDays,
		"mgr_purge_counter_keep_days":          r.CounterDays,
		"mgr_purge_realtime_counter_keep_days": r.RealtimeCounterDays,
		"mgr_purge_tag_counter_keep_days":      r.TagDays,
		"mgr_purge_visitor_counter_keep_days":  r.VisitorDays,
		"mgr_purge_daily_text_days":            r.TextDays,
		"mgr_purge_sum_data_days":              r.SumDays,
	}
}

// valueTypeDescKeys is the wire order of ValueTypeDesc.toMapValue(): the
// MapPack keys put into a default Hashtable(11) and enumerated by entrySet
// (Java oracle, JDK 8 and 17: strings ints1 strings3 booleans strings2
// strings1 ints booleans1).
var valueTypeDescKeys = []string{"strings", "ints1", "strings3", "booleans", "strings2", "strings1", "ints", "booleans1"}

func valueTypeDesc(d *config.TypeDesc) []protocol.Entry {
	out := make([]protocol.Entry, 0, len(valueTypeDescKeys))
	for _, k := range valueTypeDescKeys {
		list := protocol.Value{Type: protocol.ValueList, Values: []protocol.Value{}}
		switch k {
		case "strings":
			for _, s := range d.Strings {
				list.Values = append(list.Values, protocol.Value{Type: protocol.ValueText, Text: s})
			}
		case "booleans":
			for _, b := range d.Booleans {
				list.Values = append(list.Values, protocol.Value{Type: protocol.ValueBoolean, Bool: b})
			}
		}
		out = append(out, protocol.Entry{Key: k, Value: list})
	}
	return out
}

var _ service.MetadataAlertScriptBackend = (*confMetadata)(nil)

// SaveConfigurationText resolves the ambiguous promotion between the embedded
// FileMetadata (telegraf and counters-site writers) and ServerConfigFile
// (scouter.conf + Configure.reload(true)); without it neither method is
// promoted and every SET_CONFIGURE_* would be unsupported.
func (m *confMetadata) SaveConfigurationText(ctx context.Context, kind, text string) error {
	if kind == "server" {
		return m.ServerConfigFile.SaveConfigurationText(ctx, kind, text)
	}
	return m.FileMetadata.SaveConfigurationText(ctx, kind, text)
}

// kindName maps the config field kind to the service's value kind vocabulary.
func kindName(k config.Kind) string {
	switch k {
	case config.KindInt:
		return "int"
	case config.KindLong:
		return "long"
	case config.KindBool:
		return "bool"
	}
	return "string"
}
