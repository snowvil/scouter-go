package logger

import (
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// MaskSQL replaces SQL text with its UTF-8 byte length and its Scouter text
// hash (HashUtil.hash: CRC-32 of the UTF-8 bytes, printed with Hexa32 like the
// TextPack hash). Original SQL is never written to a log or stdout (D-S1).
func MaskSQL(text string) string {
	return "<sql masked len=" + strconv.Itoa(len(text)) + " hash=" + javaHexa32(int64(int32(crc32.ChecksumIEEE([]byte(text))))) + ">"
}

// maskLogin replaces a login (account) text with its length and hash.
func maskLogin(text string) string {
	return "<login masked len=" + strconv.Itoa(len(text)) + " hash=" + javaHexa32(int64(int32(crc32.ChecksumIEEE([]byte(text))))) + ">"
}

// FormatValue is the Java Value.toString() text. A NullValue (and an unset
// zero Value) prints as "null", the string concatenation of Java's null.
// MapValue entries follow JDK 8 Hashtable iteration order over the wire order.
func FormatValue(v protocol.Value) string {
	var sb strings.Builder
	writeValue(&sb, v)
	return sb.String()
}

func writeValue(sb *strings.Builder, v protocol.Value) {
	switch v.Type {
	case protocol.ValueBoolean:
		sb.WriteString(strconv.FormatBool(v.Bool))
	case protocol.ValueDecimal:
		sb.WriteString(strconv.FormatInt(v.Int, 10))
	case protocol.ValueFloat:
		sb.WriteString(javaFloatString(v.Float))
	case protocol.ValueDouble:
		sb.WriteString(javaDecimalFormat(v.Double))
	case protocol.ValueDoubleSummary:
		sb.WriteString("[sum=" + javaDecimalFormat(v.Double) + ",count=" + strconv.Itoa(int(v.Count)) +
			",min=" + javaDecimalFormat(v.MinDouble) + ",max=" + javaDecimalFormat(v.MaxDouble) + "]")
	case protocol.ValueLongSummary:
		sb.WriteString("[sum=" + strconv.FormatInt(v.Int, 10) + ",count=" + strconv.Itoa(int(v.Count)) +
			",min=" + strconv.FormatInt(v.MinInt, 10) + ",max=" + strconv.FormatInt(v.MaxInt, 10) + "]")
	case protocol.ValueText:
		sb.WriteString(v.Text)
	case protocol.ValueTextHash:
		sb.WriteString(strconv.FormatInt(int64(int32(v.Int)), 16))
	case protocol.ValueBlob:
		sb.WriteString("byte[" + strconv.Itoa(len(v.Bytes)) + "]")
	case protocol.ValueIP4:
		sb.WriteString(javaIP(v.Bytes))
	case protocol.ValueList:
		sb.WriteByte('[')
		for i, x := range v.Values {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeValue(sb, x)
		}
		sb.WriteByte(']')
	case protocol.ValueIntArray:
		sb.WriteByte('[')
		for i, x := range v.Ints {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(strconv.FormatInt(int64(x), 10))
		}
		sb.WriteByte(']')
	case protocol.ValueLongArray:
		sb.WriteByte('[')
		for i, x := range v.Longs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(strconv.FormatInt(x, 10))
		}
		sb.WriteByte(']')
	case protocol.ValueFloatArray:
		sb.WriteByte('[')
		for i, x := range v.Floats {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(javaFloatString(x))
		}
		sb.WriteByte(']')
	case protocol.ValueTextArray:
		sb.WriteByte('[')
		for i, x := range v.Texts {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(x)
		}
		sb.WriteByte(']')
	case protocol.ValueMap:
		writeMapValue(sb, v.Entries)
	default:
		sb.WriteString("null")
	}
}

// writeMapValue is MapValue.toString: "{k=v,k=v}" in Hashtable order.
func writeMapValue(sb *strings.Builder, entries []protocol.Entry) {
	sb.WriteByte('{')
	if len(entries) > 0 {
		keys := make([]string, len(entries))
		for i := range entries {
			keys[i] = entries[i].Key
		}
		for n, i := range hashtableOrder(keys) {
			if n > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(entries[i].Key)
			sb.WriteByte('=')
			writeValue(sb, entries[i].Value)
		}
	}
	sb.WriteByte('}')
}

// writeMapField prints a Java field initialised to new MapValue(): an unset
// (zero) Go Value prints as the empty map "{}".
func writeMapField(sb *strings.Builder, v protocol.Value) {
	if v.Type == protocol.ValueNull {
		sb.WriteString("{}")
		return
	}
	writeValue(sb, v)
}

func timeTypeName(t byte) string {
	switch t {
	case 1:
		return "REALTIME"
	case 2:
		return "ONE_MIN"
	case 3:
		return "FIVE_MIN"
	case 4:
		return "TEN_MIN"
	case 5:
		return "HOUR"
	case 6:
		return "DAY"
	}
	return ""
}

func itoa32(n int32) string { return strconv.FormatInt(int64(n), 10) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
func sbyte(b byte) string   { return strconv.Itoa(int(int8(b))) }

// FormatPack is the Java Pack.toString() text that NetDataProcessor prints for
// log_udp_packet and the other log_udp_* keys, with two intended differences:
// TextPack text of type "sql" is replaced by MaskSQL and of type "login" by a
// length/hash mask. loc is the server time zone used by DateUtil.timestamp
// (fixed-offset Scouter calendar) and by ObjectPack's java.sql.Timestamp wakeup.
// A nil pack prints as "null".
func FormatPack(p protocol.Pack, loc *time.Location) string {
	c := newCalendar(loc)
	var sb strings.Builder
	switch p := p.(type) {
	case *protocol.MapPack:
		// LinkedHashMap (AbstractMap.toString): insertion order, ", " separator.
		sb.WriteString("MapPack {")
		for i, e := range p.Entries {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(e.Key)
			sb.WriteByte('=')
			writeValue(&sb, e.Value)
		}
		sb.WriteByte('}')
	case *protocol.XLogPack:
		sb.WriteString("XLOG " + c.timestamp(p.EndTime))
		sb.WriteString(" objHash=" + javaHexa32(int64(p.ObjHash)))
		sb.WriteString(" service=" + javaHexa32(int64(p.Service)))
		sb.WriteString(" txid=" + javaHexa32(p.TxID))
		sb.WriteString(" caller=" + javaHexa32(p.Caller))
		sb.WriteString(" gxid=" + javaHexa32(p.GxID))
		sb.WriteString(" elapsed=" + itoa32(p.Elapsed))
		sb.WriteString(" error=" + itoa32(p.Error))
	case *protocol.DroppedXLogPack:
		sb.WriteString("DroppedXLogPack: g:" + itoa64(p.GxID) + " t:" + itoa64(p.TxID))
	case *protocol.ProfilePack:
		if p.Version2 {
			sb.WriteString("Profile2 " + c.timestamp(p.Time))
			sb.WriteString(" objHash=" + javaHexa32(int64(p.ObjHash)))
			sb.WriteString(" txid=" + javaHexa32(p.TxID))
			sb.WriteString(" gxid=" + javaHexa32(p.GxID))
		} else {
			sb.WriteString("Profile " + c.timestamp(p.Time))
			sb.WriteString(" objHash=" + javaHexa32(int64(p.ObjHash)))
		}
		sb.WriteString(" profile=" + strconv.Itoa(len(p.Profile)))
	case *protocol.SpanPack:
		sb.WriteString("SpanPack{gxid=" + itoa64(p.GxID) + ", txid=" + itoa64(p.TxID) + ", caller=" + itoa64(p.Caller) +
			", timestamp=" + itoa64(p.Timestamp) + ", elapsed=" + itoa32(p.Elapsed) + ", spanType=" + sbyte(p.SpanType) +
			", name=" + itoa32(p.Name) + ", objHash=" + itoa32(p.ObjHash) + ", error=" + itoa32(p.Error) +
			", localEndpointServiceName=" + itoa32(p.LocalEndpointServiceName) + ", localEndpointIp=" + javaIP(p.LocalEndpointIP) +
			", localEndpointPort=" + strconv.Itoa(int(p.LocalEndpointPort)) +
			", remoteEndpointServiceName=" + itoa32(p.RemoteEndpointServiceName) + ", remoteEndpointIp=" + javaIP(p.RemoteEndpointIP) +
			", remoteEndpointPort=" + strconv.Itoa(int(p.RemoteEndpointPort)) +
			", debug=" + strconv.FormatBool(p.Debug) + ", shared=" + strconv.FormatBool(p.Shared) + ", annotationTimestamps=")
		writeValue(&sb, p.AnnotationTimestamps)
		sb.WriteString(", annotationValues=")
		writeValue(&sb, p.AnnotationValues)
		sb.WriteString(", tags=")
		writeValue(&sb, p.Tags)
		sb.WriteByte('}')
	case *protocol.SpanContainerPack:
		sb.WriteString("SpanContainerPack  gxid=" + javaHexa32(p.GxID) + " spanCount=" + javaHexa32(int64(p.SpanCount)))
	case *protocol.TextPack:
		text := p.Text
		switch p.XType {
		case "sql":
			text = MaskSQL(text)
		case "login":
			text = maskLogin(text)
		}
		sb.WriteString(p.XType + " " + javaHexa32(int64(p.Hash)) + " " + text)
	case *protocol.PerfCounterPack:
		sb.WriteString("PerfCounter " + c.timestamp(p.Time) + " " + p.ObjName + " " + timeTypeName(p.TimeType) + " ")
		writeMapField(&sb, p.Data)
	case *protocol.InteractionPerfCounterPack:
		sb.WriteString("PerfInteractionCounterPack{time=" + itoa64(p.Time) + ", objName='" + p.ObjName +
			"', interactionType='" + p.InteractionType + "', fromHash='" + itoa32(p.FromHash) + "', toHash='" + itoa32(p.ToHash) +
			"', period=" + itoa32(p.Period) + ", count=" + itoa32(p.Count) + ", errorCount=" + itoa32(p.ErrorCount) +
			", totalElapsed=" + itoa64(p.TotalElapsed) + ", customData=")
		writeMapField(&sb, p.CustomData)
		sb.WriteByte('}')
	case *protocol.AlertPack:
		sb.WriteString("ALERT " + c.timestamp(p.Time) + " objType=" + p.ObjType + " objHash=" + javaHexa32(int64(p.ObjHash)) +
			" level=" + sbyte(p.Level) + " title=" + p.Title + " message=" + p.Message + " tags=")
		writeMapField(&sb, p.Tags)
	case *protocol.ObjectPack:
		sb.WriteString("OBJECT  objType=" + p.ObjType + " objHash=" + javaHexa32(int64(p.ObjHash)) + " objName=" + p.ObjName)
		if p.Address != "" {
			sb.WriteString(" addr=" + p.Address)
		}
		if p.Version != "" {
			sb.WriteString(" " + p.Version)
		}
		if p.Alive {
			sb.WriteString(" alive")
		}
		if p.Wakeup > 0 {
			sb.WriteString(" " + sqlTimestamp(p.Wakeup, loc))
		}
		if p.Tags.Type == protocol.ValueMap && len(p.Tags.Entries) > 0 {
			sb.WriteByte(' ')
			writeValue(&sb, p.Tags)
		}
	case *protocol.StatusPack:
		sb.WriteString("Status " + c.timestamp(p.Time) + " objType=" + p.ObjType + " objHash=" + javaHexa32(int64(p.ObjHash)) +
			" key=" + p.Key + " data=")
		writeMapField(&sb, p.Data)
	case *protocol.StackPack:
		sb.WriteString("Stack " + c.timestamp(p.Time) + " objHash=" + javaHexa32(int64(p.ObjHash)) + " stack=" + strconv.Itoa(len(p.Data)) + "bytes")
	case *protocol.SummaryPack:
		sb.WriteString("Summary " + c.timestamp(p.Time) + " objHash=" + javaHexa32(int64(p.ObjHash)) + " objType=" + p.ObjType +
			" stype=" + sbyte(p.SType) + " ")
		writeMapField(&sb, p.Table)
	case *protocol.BatchPack:
		sb.WriteString("Batch " + c.timestamp(p.StartTime) + " objHash=" + javaHexa32(int64(p.ObjHash)) + " JobId=" + p.BatchJobID +
			" PID=" + itoa32(p.PID) + " elapsed=" + itoa64(p.ElapsedTime))
	default:
		return "null"
	}
	return sb.String()
}

// UDPLog returns the scouter.conf key that gates the NetDataProcessor stdout
// line for p and the prefix printed before FormatPack (NetDataProcessor.scala
// :133-228). ok is false for packs without a per-type key (MapPack and
// standalone SpanPack go to PackExtProcessChain). log_udp_packet is separate:
// it prints FormatPack(p) without a prefix for every pack, before the type
// dispatch, while the per-type line is printed after the pack is processed.
func UDPLog(p protocol.Pack) (key, prefix string, ok bool) {
	switch p.(type) {
	case *protocol.PerfCounterPack:
		return "log_udp_counter", "DEBUG UDP COUNTER: ", true
	case *protocol.InteractionPerfCounterPack:
		return "log_udp_interaction_counter", "DEBUG UDP INTERACTION COUNTER: ", true
	case *protocol.XLogPack:
		return "log_udp_xlog", "DEBUG UDP XLOG: ", true
	case *protocol.DroppedXLogPack:
		return "log_udp_xlog", "DEBUG UDP DROPPED XLOG: ", true
	case *protocol.ProfilePack:
		return "log_udp_profile", "DEBUG UDP PROFILE: ", true
	case *protocol.TextPack:
		return "log_udp_text", "DEBUG UDP TEXT: ", true
	case *protocol.AlertPack:
		return "log_udp_alert", "DEBUG UDP ALERT: ", true
	case *protocol.ObjectPack:
		return "log_udp_object", "DEBUG UDP OBJECT: ", true
	case *protocol.StatusPack:
		return "log_udp_status", "DEBUG UDP STATUS: ", true
	case *protocol.StackPack:
		return "log_udp_stack", "DEBUG UDP STACK: ", true
	case *protocol.SummaryPack:
		return "log_udp_summary", "DEBUG UDP SUMMARY: ", true
	case *protocol.BatchPack:
		return "log_udp_batch", "DEBUG UDP Batch: ", true
	case *protocol.SpanContainerPack:
		return "log_udp_span", "DEBUG UDP SPAN CONTAINER: ", true
	}
	return "", "", false
}

// JavaNull is how Java string concatenation prints a null reference. Pass it
// as a service name that TextRD.getString did not find.
const JavaNull = "null"

// ProfileDebugString is XLogProfilePack.toDebugString / XLogProfilePack2
// .toDebugString(svcName) used by ProfilePreCore when log_test_rate > 0.
func ProfileDebugString(p *protocol.ProfilePack, svcName string) string {
	var sb strings.Builder
	if p.Version2 {
		sb.WriteString("Profile2  svcName=" + svcName + " discardType=" + sbyte(p.DiscardType) +
			" objHash=" + javaHexa32(int64(p.ObjHash)) + " svc=" + itoa32(p.Service) + " gxid=" + javaHexa32(p.GxID) +
			" txid=" + javaHexa32(p.TxID) + " packType=" + strconv.Itoa(int(protocol.PackProfile2)) +
			" isDriving=" + strconv.FormatBool(p.GxID == p.TxID || p.GxID == 0) +
			" isService=" + strconv.FormatBool(p.XType == 0 || p.XType == 1 || p.XType == 3))
	} else {
		sb.WriteString("Profile  svcName=" + svcName + " objHash=" + javaHexa32(int64(p.ObjHash)) + " svc=" + itoa32(p.Service) +
			" txid=" + javaHexa32(p.TxID))
	}
	sb.WriteString(" profile=" + strconv.Itoa(len(p.Profile)))
	return sb.String()
}

// log_test_rate case tags printed by ProfilePreCore (ProfilePreCore.scala:107-159).
const (
	ProfileCaseIgnoreCons = "[ProfileCoreCase1(IgnoreCons)]"
	ProfileCaseNormal     = "[ProfileCoreCase2]"
	ProfileCaseOld        = "[ProfileCoreCase3(Old)]"
)

// ProfileCaseDelaying is the processDelayingProfiles tag "[processDelaying][cnt/total]".
func ProfileCaseDelaying(cnt, total int) string {
	return "[processDelaying][" + strconv.Itoa(cnt) + "/" + strconv.Itoa(total) + "]"
}

// ProfileTestLine is the Logger message "<profile bytes>\t<tag> <debug string>"
// that ProfilePreCore writes with Logger.println(msg) (no ID).
func ProfileTestLine(tag string, p *protocol.ProfilePack, svcName string) string {
	return strconv.Itoa(len(p.Profile)) + "\t" + tag + " " + ProfileDebugString(p, svcName)
}

// log_udp_some_object stdout lines (CounterManager.java:283-316). detected
// and xml are printed verbatim; pass JavaNull for a missing detected type.
func SomeObjectLine(objType string) string { return "DEBUG objType: " + objType }
func SomeObjectDetectedLine(objType, detected string) string {
	return "DEBUG objType: " + objType + ", detected: " + detected
}
func SomeObjectWillBeAddedLine(name string) string {
	return "DEBUG objType: " + name + ", will be added "
}
func SomeObjectNewXMLLine(name, xml string) string {
	return "DEBUG objType: " + name + ", new XML = " + xml
}
