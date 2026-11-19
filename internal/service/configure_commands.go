// ConfigureService, AlertScriptingService and ThreadList handlers:
// Copyright 2015 Scouter authors, Apache-2.0. Independent implementation of
// pinned source dee47fb5.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"

	"github.com/snowvil/scouter-go/internal/counters"
	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/transport"
)

// ConfigurationWriter replaces a configuration text. kind uses the
// MetadataBackend.ConfigurationText vocabulary: server, telegraf,
// counters-site. nil is the Java result "true". Any error answers "false"
// (Java saveText/parse failure), except configureFatal errors
// (cancellation, unsupported owner), which fail the command.
// internal/server.ServerConfigFile implements the "server" kind.
type ConfigurationWriter interface {
	SaveConfigurationText(ctx context.Context, kind, text string) error
}

// ErrConfigurationRejected is the Java false of a validating writer
// (SET_CONFIGURE_COUNTERS_SITE: CounterEngine.parse failed).
var ErrConfigurationRejected = errors.New("service: configuration rejected")

// configureFatal reports the writer errors that must not be answered as
// the Java "false": the command was not attempted.
func configureFatal(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, transport.ErrUnsupported) || errors.Is(err, transport.ErrUnauthorized)
}

// saveConfiguration answers MapPack{result: "true"/"false"}.
func (s *Service) saveConfiguration(ctx context.Context, stream *transport.Stream, kind, text string) error {
	err := s.config.Metadata.(ConfigurationWriter).SaveConfigurationText(ctx, kind, text)
	if err != nil && configureFatal(err) {
		return err
	}
	if err != nil && !errors.Is(err, ErrConfigurationRejected) {
		log.Printf("service: SET %s configuration answered false: %v", kind, err)
	}
	return configureResult(ctx, stream, err == nil)
}

// CountersSiteStore defines or edits object types in counters.site.xml
// (CounterManager.addObjectType/editObjectType).
type CountersSiteStore interface {
	DefineObjectType(ctx context.Context, spec counters.ObjectTypeSpec) (bool, error)
	EditObjectType(ctx context.Context, spec counters.ObjectTypeSpec) (bool, error)
}

// AlertScriptWriter saves plugin_dir/<counterName>.alert (configuration=false)
// or .conf. It never compiles or runs the rule.
type AlertScriptWriter interface {
	SaveAlertScript(ctx context.Context, counterName string, configuration bool, contents string) (bool, error)
}

// AlertScriptMessageSource is AlertScriptLoadMessageCache.get(loop, index).
type AlertScriptMessageSource interface {
	AlertScriptLoadMessages(ctx context.Context, loop int64, index int32) (AlertScriptLoadMessages, error)
}

// ConfigureCommands lists the server configuration, counters-site, object
// type, alert script and server thread commands handled by
// HandleConfigureCommand before the metadata/read handlers.
// GET_CONFIGURE_SERVER/TELEGRAF/COUNTERS_SITE stay in the metadata handler,
// which reads through MetadataBackend.ConfigurationText.
func ConfigureCommands() []string {
	return []string{
		"SET_CONFIGURE_SERVER", "SET_CONFIGURE_TELEGRAF", "SET_CONFIGURE_COUNTERS_SITE",
		"DEFINE_OBJECT_TYPE", "EDIT_OBJECT_TYPE",
		"SAVE_ALERT_SCRIPTING_CONTETNS", "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS",
		"GET_ALERT_SCRIPT_LOAD_MESSAGE", "GET_ALERT_REAL_COUNTER_DESC", "GET_PLUGIN_HELPER_DESC",
		"SERVER_THREAD_LIST", "SERVER_THREAD_DETAIL",
	}
}

func configureCommand(command string) bool {
	for _, c := range ConfigureCommands() {
		if c == command {
			return true
		}
	}
	return false
}

// HandleConfigureCommand recognizes its commands before reading the body and
// leaves the stream intact for unknown commands. A missing capability fails
// before the body is read.
func (s *Service) HandleConfigureCommand(ctx context.Context, r transport.ClientRequest, stream *transport.Stream) (bool, error) {
	if !configureCommand(r.Command) {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if !r.Authenticated {
		return true, transport.ErrUnauthorized
	}
	// Body-less commands (ThreadList.serverThreadList, descriptor streams).
	switch r.Command {
	case "SERVER_THREAD_LIST":
		return true, s.serverThreadList(ctx, stream)
	case "GET_ALERT_REAL_COUNTER_DESC", "GET_PLUGIN_HELPER_DESC":
		if err := consumeClientEmptyMapPack(stream); err != nil {
			return true, err
		}
		if r.Command == "GET_ALERT_REAL_COUNTER_DESC" {
			return true, writeMethodDescriptors(ctx, stream, realCounterDescriptors)
		}
		return true, writeMethodDescriptors(ctx, stream, pluginHelperDescriptors)
	}
	var capable bool
	switch r.Command {
	case "SET_CONFIGURE_SERVER", "SET_CONFIGURE_TELEGRAF", "SET_CONFIGURE_COUNTERS_SITE":
		_, capable = s.config.Metadata.(ConfigurationWriter)
	case "DEFINE_OBJECT_TYPE", "EDIT_OBJECT_TYPE":
		_, capable = s.config.Metadata.(CountersSiteStore)
	case "SAVE_ALERT_SCRIPTING_CONTETNS", "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS":
		_, capable = s.config.Metadata.(AlertScriptWriter)
	case "GET_ALERT_SCRIPT_LOAD_MESSAGE":
		_, capable = s.config.Metadata.(AlertScriptMessageSource)
	case "SERVER_THREAD_DETAIL":
		capable = true
	}
	if !capable {
		return true, fmt.Errorf("%w: %s requires a configured owner", transport.ErrUnsupported, r.Command)
	}
	p, err := readMap(stream)
	if err != nil {
		return true, err
	}
	return true, s.configureMutation(ctx, r.Command, p, stream)
}

func (s *Service) configureMutation(ctx context.Context, command string, p *protocol.MapPack, stream *transport.Stream) error {
	switch command {
	case "SERVER_THREAD_DETAIL":
		return s.serverThreadDetail(ctx, javaLongDefault(p, "id", 0), stream)
	case "SET_CONFIGURE_SERVER", "SET_CONFIGURE_TELEGRAF":
		kind, key := "server", "setConfig"
		if command == "SET_CONFIGURE_TELEGRAF" {
			kind, key = "telegraf", "tgConfigContents"
		}
		// Java saveText(null) throws inside its try block and returns false.
		text, ok := javaText(p, key)
		if !ok {
			return configureResult(ctx, stream, false)
		}
		return s.saveConfiguration(ctx, stream, kind, text)
	case "SET_CONFIGURE_COUNTERS_SITE":
		text, ok := javaText(p, "contents")
		if !ok {
			// contents.getBytes NullPointerException: Java answers nothing.
			return badParam("contents")
		}
		// Java then RegisterHandler.notifyAllClients(); REFETCH_COUNTER_XML
		// fan-out is the Reload owner's responsibility (CONTRACT).
		return s.saveConfiguration(ctx, stream, "counters-site", text)
	case "DEFINE_OBJECT_TYPE", "EDIT_OBJECT_TYPE":
		name, hasName := javaText(p, "name")
		spec := counters.ObjectTypeSpec{Name: name, SubObject: javaBoolean(p, "sub-object")}
		spec.Display, _ = javaText(p, "disp")
		spec.Family, _ = javaText(p, "family")
		spec.Icon, _ = javaText(p, "icon")
		success := false
		if hasName {
			store := s.config.Metadata.(CountersSiteStore)
			var err error
			if command == "DEFINE_OBJECT_TYPE" {
				success, err = store.DefineObjectType(ctx, spec)
			} else {
				success, err = store.EditObjectType(ctx, spec)
			}
			if err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ResponseValue(boolean(success))
	case "SAVE_ALERT_SCRIPTING_CONTETNS", "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS":
		name, ok := javaText(p, "counterName")
		if !ok {
			name = "null" // plugin_dir + "/" + null + suffix
		}
		success := false
		if contents, ok := javaText(p, "contents"); ok {
			var err error
			success, err = s.config.Metadata.(AlertScriptWriter).SaveAlertScript(ctx, name, command == "SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS", contents)
			if err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ResponsePack(newMap(entry("success", boolean(success))))
	case "GET_ALERT_SCRIPT_LOAD_MESSAGE":
		out, err := s.config.Metadata.(AlertScriptMessageSource).AlertScriptLoadMessages(ctx, javaLongDefault(p, "loop", 0), javaIntDefault(p, "index"))
		if err != nil {
			return err
		}
		messages := make([]protocol.Value, 0, len(out.Messages))
		for _, m := range out.Messages {
			messages = append(messages, stringValue(m))
		}
		return stream.ResponsePack(newMap(entry("loop", decimal(out.Loop)), entry("index", decimal(int64(out.Index))), entry("messages", list(messages))))
	}
	return transport.ErrUnsupported
}

// clientEmptyMapPack is a serialized empty MapPack (type 10, count 0). The
// Java Client sends it after GET_ALERT_REAL_COUNTER_DESC and
// GET_PLUGIN_HELPER_DESC although the server reads no body
// (AlertScriptingView.java:597; management-commands.md §9-11).
var clientEmptyMapPack = []byte{0x0A, 0x00}

// bufferedConsumer is the transport.Stream hook proposed for §9-11 (b):
// consume prefix only when it is already buffered and matches exactly.
type bufferedConsumer interface {
	ConsumeBuffered(prefix []byte) (bool, error)
}

// consumeClientEmptyMapPack applies decision §9-11 (b) when the stream
// provides ConsumeBuffered; otherwise nothing is read, as in the original.
func consumeClientEmptyMapPack(stream *transport.Stream) error {
	c, ok := any(stream).(bufferedConsumer)
	if !ok {
		return nil
	}
	_, err := c.ConsumeBuffered(clientEmptyMapPack)
	return err
}

// configureResult is MapPack{result: String.valueOf(success)} (Text, not Boolean).
func configureResult(ctx context.Context, stream *transport.Stream, success bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.ResponsePack(newMap(entry("result", stringValue(strconv.FormatBool(success)))))
}

// writeMethodDescriptors streams one MapPack per descriptor:
// desc, methodName, returnTypeName, parameterTypeNames (AlertScriptingService).
func writeMethodDescriptors(ctx context.Context, stream *transport.Stream, descriptors []MethodDescriptor) error {
	for _, d := range descriptors {
		if err := ctx.Err(); err != nil {
			return err
		}
		params := make([]protocol.Value, 0, len(d.ParameterTypeNames))
		for _, name := range d.ParameterTypeNames {
			params = append(params, stringValue(name))
		}
		if err := stream.ResponsePack(newMap(entry("desc", stringValue(d.Desc)), entry("methodName", stringValue(d.MethodName)), entry("returnTypeName", stringValue(d.ReturnTypeName)), entry("parameterTypeNames", list(params)))); err != nil {
			return err
		}
	}
	return nil
}

// javaBoolean mirrors MapPack.getBoolean: only a BooleanValue is true.
func javaBoolean(p *protocol.MapPack, key string) bool {
	v := lookup(p, key)
	return v.Type == protocol.ValueBoolean && v.Bool
}

// javaIntDefault mirrors MapPack.getInt: NumberValue.intValue() (long
// truncation for Decimal, saturating float conversion), 0 otherwise.
func javaIntDefault(p *protocol.MapPack, key string) int32 {
	v := lookup(p, key)
	switch v.Type {
	case protocol.ValueDecimal:
		return int32(v.Int)
	case protocol.ValueFloat:
		return javaIntFromFloat(float64(v.Float))
	case protocol.ValueDouble:
		return javaIntFromFloat(v.Double)
	}
	return 0
}

func javaIntFromFloat(f float64) int32 {
	switch {
	case math.IsNaN(f):
		return 0
	case f >= math.MaxInt32:
		return math.MaxInt32
	case f <= math.MinInt32:
		return math.MinInt32
	}
	return int32(f)
}
