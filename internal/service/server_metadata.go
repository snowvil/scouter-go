package service

import (
	"context"
	"crypto/subtle"
	"runtime"
	"strconv"
	"strings"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/scoutertime"
	"github.com/snowvil/scouter-go/internal/transport"
)

// MetadataCommands enumerates the read handlers in the pinned inventory.
func MetadataCommands() []string {
	return []string{
		"CHECK_ACCOUNT_ID", "CHECK_JOB", "CHECK_LOGIN", "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE", "CONFIGURE_VALUE_TYPE_DESC", "DEBUG_SERVER", "GET_ALERT_REAL_COUNTER_DESC", "GET_ALERT_SCRIPTING_CONFIG_CONTETNS", "GET_ALERT_SCRIPTING_CONTETNS", "GET_ALERT_SCRIPT_LOAD_MESSAGE", "GET_CONFIGURE_COUNTERS_SITE", "GET_CONFIGURE_SERVER", "GET_CONFIGURE_TELEGRAF", "GET_GROUP_POLICY_ALL", "GET_LOGIN_LIST", "GET_PLUGIN_HELPER_DESC", "LIST_ACCOUNT", "LIST_ACCOUNT_GROUP", "LIST_CONFIGURE_SERVER", "SERVER_DB_LIST", "SERVER_ENV", "SERVER_LOG_DETAIL", "SERVER_LOG_LIST", "SERVER_THREAD_DETAIL", "SERVER_THREAD_LIST",
	}
}

func (s *Service) HandleMetadataCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	recognized := false
	for _, cmd := range MetadataCommands() {
		if cmd == r.Command {
			recognized = true
			break
		}
	}
	if !recognized {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	switch r.Command {
	case "CHECK_ACCOUNT_ID", "CHECK_JOB", "CHECK_LOGIN", "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE", "CONFIGURE_VALUE_TYPE_DESC", "DEBUG_SERVER", "SERVER_LOG_DETAIL", "GET_ALERT_SCRIPTING_CONTETNS", "GET_ALERT_SCRIPTING_CONFIG_CONTETNS":
		p, err := readMap(stream)
		if err != nil {
			return true, err
		}
		return true, s.metadataQuery(ctx, r.Command, p, stream)
	default:
		return true, s.metadataQuery(ctx, r.Command, nil, stream)
	}
}

// singleAdminPolicy is the policy of the "admin" group in the Java default
// account_group.xml (scouter.server account resources), in the file's element
// order. Without account files the single --user is that admin, so LOGIN and
// GET_GROUP_POLICY_ALL send this map. Java allows every action; here
// (intended difference) the three account-administration policies
// (AllowAddAccount, AllowEditAccount, AllowEditGroupPolicy) are false: their
// commands ADD_ACCOUNT, EDIT_ACCOUNT, ADD_ACCOUNT_GROUP and
// EDIT_GROUP_POLICY need an account store (--account-dir) and are refused
// here, so the Client must not offer them. The Client hides SQL bind
// parameters as "[******]" unless AllowSqlParameter is true
// (ProfileText.java:1057-1069) and disables the menus of the other actions.
func singleAdminPolicy() protocol.Value {
	var entries []protocol.Entry
	for _, key := range []string{"AllowEditGroupPolicy", "AllowHeapDump", "AllowFileDump", "AllowHeapHistogram", "AllowThreadDump", "AllowSystemGC", "AllowConfigure", "AllowExportCounter", "AllowExportAppSum", "AllowLoginList", "AllowDBManager", "AllowAddAccount", "AllowEditAccount", "AllowSqlParameter", "AllowKillTransaction", "AllowExportClass", "AllowRedefineClass", "AllowDefineObjectType"} {
		allow := key != "AllowEditGroupPolicy" && key != "AllowAddAccount" && key != "AllowEditAccount"
		entries = append(entries, entry(key, boolean(allow)))
	}
	return protocol.Value{Type: protocol.ValueMap, Entries: entries}
}
func (s *Service) metadataQuery(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	switch command {
	case "CHECK_JOB":
		// ManageService.java:113-126. Service.Handle dispatches CHECK_JOB to
		// HandleManageCommand first, which dequeues the RemoteControl job of
		// the session (remote_control.go). This branch is reached only by
		// direct HandleMetadataCommand callers without that queue and answers
		// the Java no-job response.
		if _, err := getNumber(p, "session"); err != nil {
			return err
		}
		return nil
	case "CHECK_ACCOUNT_ID":
		id, err := getText(p, "id")
		if err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("result", boolean(id != s.config.Username))))
	case "CHECK_LOGIN":
		id, err := getText(p, "id")
		if err != nil {
			return err
		}
		pass, err := getText(p, "pass")
		if err != nil {
			return err
		}
		valid := subtle.ConstantTimeCompare([]byte(id), []byte(s.config.Username)) & subtle.ConstantTimeCompare([]byte(pass), []byte(s.passwordDigest))
		return stream.ResponseValue(boolean(valid == 1))
	case "LIST_ACCOUNT":
		// Account.java toBytes writes four one-byte UTF-8 lengths, not Scouter Blob
		// lengths. Reject source's length-wrap defect instead of emitting corruption.
		var blob []byte
		for _, text := range []string{s.config.Username, s.passwordDigest, "", "admin"} {
			if len(text) > 255 {
				return transport.ErrLimit
			}
			blob = append(blob, byte(len(text)))
			blob = append(blob, []byte(text)...)
		}
		return stream.ResponseValue(protocol.Value{Type: protocol.ValueBlob, Bytes: blob})
	case "LIST_ACCOUNT_GROUP":
		return stream.ResponsePack(newMap(entry("group_list", list([]protocol.Value{stringValue("admin")}))))
	case "GET_GROUP_POLICY_ALL":
		return stream.ResponsePack(newMap(entry("admin", singleAdminPolicy())))
	case "GET_LOGIN_LIST":
		users := s.metadataLoginUsers()
		if len(users) == 0 {
			return nil
		}
		if len(users) > s.config.MaxSessions {
			return transport.ErrLimit
		}
		result := newMap()
		now := s.config.Clock().UnixMilli()
		for _, key := range []string{"session", "user", "ip", "logintime", "ver", "host"} {
			values := make([]protocol.Value, 0, len(users))
			for _, u := range users {
				switch key {
				case "session":
					values = append(values, decimal(u.Session))
				case "user":
					values = append(values, stringValue(u.ID))
				case "ip":
					values = append(values, stringValue(u.IP))
				case "logintime":
					values = append(values, decimal((now-u.LoginTime)/1000))
				case "ver":
					values = append(values, stringValue(u.Version))
				case "host":
					values = append(values, stringValue(u.Host))
				}
			}
			result.Entries = append(result.Entries, entry(key, list(values)))
		}
		return stream.ResponsePack(result)
	case "SERVER_ENV":
		// The wire dictionary remains a MapPack. Runtime-specific fields honestly
		// describe this Go process and deliberately omit unavailable java.* fields.
		osName := runtime.GOOS
		switch osName {
		case "darwin":
			osName = "Mac OS X"
		case "linux":
			osName = "Linux"
		case "windows":
			osName = "Windows"
		}
		return stream.ResponsePack(newMap(entry("os.name", stringValue(osName)), entry("os.arch", stringValue(runtime.GOARCH)), entry("go.version", stringValue(runtime.Version()))))
	case "DEBUG_SERVER":
		b, ok := s.backend.(AlertBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		now := s.config.Clock()
		date := scoutertime.Date(now.UnixMilli(), s.config.Location)
		start, err := scoutertime.ParseDate(date, s.config.Location)
		if err != nil {
			return err
		}
		end := start + scoutertime.DayMillis - 1
		alerts := 0
		err = b.ScanAlertHistory(ctx, date, start, end, func(*protocol.AlertPack) (bool, error) { alerts++; return true, nil })
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("msg", stringValue("processed "+strconv.Itoa(alerts)))))
	case "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE", "CONFIGURE_VALUE_TYPE_DESC":
		hash, err := getInt(p, "objHash")
		if err != nil {
			return err
		}
		if hash != 0 {
			if command == "CONFIGURE_VALUE_TYPE_DESC" {
				return stream.ResponsePack(newMap())
			}
			b, ok := s.backend.(AgentBackend)
			if !ok {
				return transport.ErrUnsupported
			}
			packs, err := awaitAgent(ctx, stream, b, hash, command, p)
			if err != nil {
				if !skippableAgentError(ctx, err) {
					return err
				}
				// ConfigureService.scala:176-183/199-206: AgentCall returns
				// null on an agent failure and nothing is written.
				noteAgentRelaySkip(command, hash, false, err)
				return nil
			}
			if len(packs) > 4096 {
				return transport.ErrLimit
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(packs) > 0 {
				if result, ok := packs[len(packs)-1].(*protocol.MapPack); ok {
					return stream.ResponsePack(result)
				}
			}
			return nil
		}
	}
	if s.config.Metadata == nil {
		return transport.ErrUnsupported
	}
	b := s.config.Metadata
	switch command {
	case "GET_CONFIGURE_SERVER", "LIST_CONFIGURE_SERVER", "CONFIGURE_DESC", "CONFIGURE_VALUE_TYPE", "CONFIGURE_VALUE_TYPE_DESC":
		properties, err := b.Configuration(ctx)
		if err != nil {
			return err
		}
		if len(properties) > 4096 {
			return transport.ErrLimit
		}
		out := newMap()
		switch command {
		case "GET_CONFIGURE_SERVER":
			keys := []protocol.Value{}
			for _, prop := range properties {
				keys = append(keys, stringValue(prop.Key))
			}
			text, err := b.ConfigurationText(ctx, "server")
			if err != nil {
				return err
			}
			out.Entries = []protocol.Entry{entry("configKey", list(keys)), entry("serverConfig", stringValue(text))}
		case "LIST_CONFIGURE_SERVER":
			for _, key := range []string{"key", "value", "default"} {
				values := []protocol.Value{}
				for _, prop := range properties {
					switch key {
					case "value":
						values = append(values, configuredValue(prop.Value, prop.ValueKind, false))
					case "default":
						values = append(values, configuredValue(prop.Default, prop.ValueKind, prop.NullDefault))
					default:
						values = append(values, stringValue(prop.Key))
					}
				}
				out.Entries = append(out.Entries, entry(key, list(values)))
			}
		case "CONFIGURE_DESC":
			for _, prop := range properties {
				if prop.NoDescription {
					continue // getConfigDescMap: fields without @ConfigDesc are absent
				}
				out.Entries = append(out.Entries, entry(prop.Key, stringValue(prop.Description)))
			}
		case "CONFIGURE_VALUE_TYPE":
			for _, prop := range properties {
				out.Entries = append(out.Entries, entry(prop.Key, decimal(prop.Type)))
			}
		case "CONFIGURE_VALUE_TYPE_DESC":
			for _, prop := range properties {
				if prop.TypeDescription != nil {
					out.Entries = append(out.Entries, entry(prop.Key, protocol.Value{Type: protocol.ValueMap, Entries: prop.TypeDescription}))
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ResponsePack(out)
	case "GET_ALERT_SCRIPTING_CONTETNS", "GET_ALERT_SCRIPTING_CONFIG_CONTETNS":
		owner, ok := b.(MetadataAlertScriptBackend)
		if !ok {
			return transport.ErrUnsupported
		}
		name, err := getText(p, "counterName")
		if err != nil {
			return err
		}
		if lookup(p, "counterName").Type == protocol.ValueNull {
			name = "null"
		}
		text, err := owner.AlertScript(ctx, name, command == "GET_ALERT_SCRIPTING_CONFIG_CONTETNS")
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("contents", stringValue(text))))
	case "GET_CONFIGURE_TELEGRAF", "GET_CONFIGURE_COUNTERS_SITE":
		kind, key := "telegraf", "tgConfigContents"
		if command == "GET_CONFIGURE_COUNTERS_SITE" {
			kind, key = "counters-site", "contents"
		}
		text, err := b.ConfigurationText(ctx, kind)
		if err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry(key, stringValue(text))))
	case "SERVER_DB_LIST":
		usage, err := b.Database(ctx)
		if err != nil {
			return err
		}
		out, err := metadataFilePack(usage.Directories)
		if err != nil {
			return err
		}
		out.Entries = append(out.Entries, entry("free", decimal(usage.Free)), entry("total", decimal(usage.Total)))
		return stream.ResponsePack(out)
	case "SERVER_LOG_LIST":
		files, exists, err := b.Logs(ctx)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		out, err := metadataFilePack(files)
		if err != nil {
			return err
		}
		return stream.ResponsePack(out)
	case "SERVER_LOG_DETAIL":
		name, err := getText(p, "name")
		if err != nil {
			return err
		}
		text, exists, err := b.Log(ctx, name)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		return stream.ResponseValue(stringValue(text))
	}
	return transport.ErrUnsupported
}
func metadataFilePack(files []MetadataFile) (*protocol.MapPack, error) {
	if len(files) > 4096 {
		return nil, transport.ErrLimit
	}
	names, sizes, times := []protocol.Value{}, []protocol.Value{}, []protocol.Value{}
	for _, f := range files {
		names = append(names, stringValue(f.Name))
		sizes = append(sizes, decimal(f.Size))
		times = append(times, decimal(f.LastModified))
	}
	return newMap(entry("name", list(names)), entry("size", list(sizes)), entry("lastModified", list(times))), nil
}

// configuredValue is ConfigValueUtil.toValue on a Configure field: numbers
// become DecimalValue, booleans BooleanValue, a null initializer NullValue and
// everything else TextValue. A number or boolean that does not parse (a
// value the file could not apply) falls back to its text.
func configuredValue(text, kind string, null bool) protocol.Value {
	if null {
		return protocol.Value{Type: protocol.ValueNull}
	}
	switch kind {
	case "int", "long":
		if n, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64); err == nil {
			return decimal(n)
		}
	case "bool":
		if b, err := strconv.ParseBool(strings.TrimSpace(text)); err == nil {
			return boolean(b)
		}
	}
	return stringValue(text)
}
