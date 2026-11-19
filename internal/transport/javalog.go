package transport

import (
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// LogFunc receives one Java Logger line without the timestamp prefix. id is the
// Java message ID ("S150", "S160") or "" for Java's ID-less Logger.println(msg).
// It is called synchronously on connection, worker and sweeper goroutines, so it
// must be concurrency-safe, bounded and nonblocking. Transport already applies
// the per-ID rate limits of the original call sites (S150: 10 seconds); the
// caller applies only formatting and mgr_log_ignore_ids filtering.
type LogFunc func(id, message string)

// ObjectNameFunc mirrors Java TextCache.get(TextTypes.OBJECT, objHash): a cache
// lookup only. It must not block on storage. ok=false prints Java's "null".
type ObjectNameFunc func(objHash int32) (name string, ok bool)

type logSink struct{ fn atomic.Pointer[LogFunc] }

func (l *logSink) set(fn LogFunc) {
	if fn == nil {
		l.fn.Store(nil)
		return
	}
	l.fn.Store(&fn)
}
func (l *logSink) enabled() bool { return l.fn.Load() != nil }
func (l *logSink) emit(id, message string) {
	if fn := l.fn.Load(); fn != nil {
		(*fn)(id, message)
	}
}

// rateLimit mirrors Logger.checkOk(id, sec): a line passes when now is at least
// sec seconds after the previous passing line with the same ID.
type rateLimit struct{ last atomic.Int64 }

func (r *rateLimit) allow(now time.Time, every time.Duration) bool {
	for {
		last := r.last.Load()
		if last != 0 && now.UnixNano() < last+int64(every) {
			return false
		}
		if r.last.CompareAndSwap(last, now.UnixNano()) {
			return true
		}
	}
}

// hexa32 is scouter.util.Hexa32.toString32 (verified against JDK 8 output).
func hexa32(n int64) string {
	switch {
	case n == math.MinInt64:
		return "z8000000000000"
	case n < 0:
		return "z" + strconv.FormatInt(-n, 32)
	case n < 10:
		return strconv.FormatInt(n, 10)
	default:
		return "x" + strconv.FormatInt(n, 32)
	}
}

// javaInetAddress is InetAddress.toString() for an address without a resolved
// host name: "/" + getHostAddress(). JDK 8 prints IPv6 as eight uncompressed
// lowercase groups and turns IPv4-mapped IPv6 into the IPv4 form.
func javaInetAddress(addr netip.Addr) string {
	if !addr.IsValid() {
		return "null"
	}
	addr = addr.Unmap()
	if addr.Is4() {
		return "/" + addr.String()
	}
	raw := addr.As16()
	var b strings.Builder
	b.WriteByte('/')
	for i := 0; i < 16; i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(strconv.FormatUint(uint64(raw[i])<<8|uint64(raw[i+1]), 16))
	}
	if zone := addr.Zone(); zone != "" {
		b.WriteString("%" + zone)
	}
	return b.String()
}

// javaSocketAddress is JDK 8 InetSocketAddress.toString() for an accepted
// socket: "/ip:port" (JDK 14+ would bracket IPv6; the oracle is JDK 8).
func javaSocketAddress(addr net.Addr) string {
	if addr == nil {
		return "null"
	}
	var ap netip.AddrPort
	if tcp, ok := addr.(*net.TCPAddr); ok {
		ap = tcp.AddrPort()
	} else if parsed, err := netip.ParseAddrPort(addr.String()); err == nil {
		ap = parsed
	} else {
		return sanitizeLogText(addr.String())
	}
	return javaInetAddress(ap.Addr()) + ":" + strconv.Itoa(int(ap.Port()))
}

// packClassName is Pack.getClass().getName() for the S160 line.
func packClassName(p protocol.Pack) string {
	if p == nil {
		return "null"
	}
	switch p.Type() {
	case protocol.PackMap:
		return "scouter.lang.pack.MapPack"
	case protocol.PackXLog:
		return "scouter.lang.pack.XLogPack"
	case protocol.PackDroppedXLog:
		return "scouter.lang.pack.DroppedXLogPack"
	case protocol.PackProfile:
		return "scouter.lang.pack.XLogProfilePack"
	case protocol.PackProfile2:
		return "scouter.lang.pack.XLogProfilePack2"
	case protocol.PackSpan:
		return "scouter.lang.pack.SpanPack"
	case protocol.PackSpanContainer:
		return "scouter.lang.pack.SpanContainerPack"
	case protocol.PackText:
		return "scouter.lang.pack.TextPack"
	case protocol.PackPerfCounter:
		return "scouter.lang.pack.PerfCounterPack"
	case protocol.PackStatus:
		return "scouter.lang.pack.StatusPack"
	case protocol.PackStack:
		return "scouter.lang.pack.StackPack"
	case protocol.PackSummary:
		return "scouter.lang.pack.SummaryPack"
	case protocol.PackBatch:
		return "scouter.lang.pack.BatchPack"
	case protocol.PackInteractionCounter:
		return "scouter.lang.pack.InteractionPerfCounterPack"
	case protocol.PackAlert:
		return "scouter.lang.pack.AlertPack"
	case protocol.PackObject:
		return "scouter.lang.pack.ObjectPack"
	default:
		return "scouter.lang.pack.Pack" + strconv.Itoa(int(p.Type()))
	}
}

const maxLogText = 256

// sanitizeLogText bounds peer-controlled text (Client command names, error
// strings) before it reaches a log line: control characters become '?' and the
// result is cut at maxLogText bytes on a rune boundary. Java logs it raw.
func sanitizeLogText(text string) string {
	clean := strings.Map(func(r rune) rune {
		if r == utf8.RuneError || unicode.IsControl(r) {
			return '?'
		}
		return r
	}, text)
	if len(clean) <= maxLogText {
		return clean
	}
	cut := maxLogText
	for cut > 0 && !utf8.RuneStart(clean[cut]) {
		cut--
	}
	return clean[:cut] + "..."
}
