package ingest

// Settings are the Configure values the engine reads while it runs (the
// runtime converts config.Live and calls SetSettings). Each field is read
// at the point where the Java code reads the Configure field, so a change
// applies like Java: dead times and the alert level on the next 1s check,
// TagCount on every XLog/alert, Auto5MSampling at the 5-minute tick,
// XLogKeepMillis/ProfileKeepSecs on the next delayed pack, the bucket
// counts when the next bucket is created, GeoIP on every XLog.
type Settings struct {
	// ObjectDeadTimeMs/ZipkinDeadTimeMs are object_deadtime_ms and
	// object_zipkin_deadtime_ms (zipkin and z$ object types).
	ObjectDeadTimeMs, ZipkinDeadTimeMs int64
	// InactiveAlertLevel is object_inactive_alert_level, narrowed to a byte
	// like Java asInstanceOf[Byte].
	InactiveAlertLevel int32
	TagCount           bool // tagcnt_enabled
	Auto5MSampling     bool // _auto_5m_sampling
	// XLog consequent sampling bounds; a value <= 0 selects the default.
	XLogKeepCount, XLogKeepMillis, ProfileKeepCount, ProfileKeepSecs int
	GeoIP                                                            bool // geoip_enabled
}

var defaultSettings = DefaultSettings()

// DefaultSettings are the applyConfig defaults.
func DefaultSettings() Settings {
	return Settings{ObjectDeadTimeMs: 8000, ZipkinDeadTimeMs: 300000, TagCount: true, Auto5MSampling: true,
		XLogKeepCount: defaultXLogKeepCount, XLogKeepMillis: defaultXLogKeepMillis, ProfileKeepCount: defaultProfileKeepCount,
		ProfileKeepSecs: defaultProfileKeepSecs, GeoIP: true}
}

const (
	defaultGxidKeepCount    = 500000
	defaultXLogKeepCount    = 100000
	defaultXLogKeepMillis   = 5000
	defaultProfileKeepCount = 5000
	defaultProfileKeepSecs  = 5
	profileBucketMillis     = 500
)

// SetSettings publishes new settings. It only stores a pointer, so it is
// safe to call from a configuration reload callback.
func (e *Engine) SetSettings(s Settings) {
	if s.XLogKeepCount <= 0 {
		s.XLogKeepCount = defaultXLogKeepCount
	}
	if s.XLogKeepMillis <= 0 {
		s.XLogKeepMillis = defaultXLogKeepMillis
	}
	if s.ProfileKeepCount <= 0 {
		s.ProfileKeepCount = defaultProfileKeepCount
	}
	if s.ProfileKeepSecs <= 0 {
		s.ProfileKeepSecs = defaultProfileKeepSecs
	}
	e.live.Store(&s)
}

// Settings returns the settings in use.
func (e *Engine) Settings() Settings { return *e.live.Load() }

func (e *Engine) settings() *Settings { return e.live.Load() }

// profileBuckets is ProfileDelayingCache's maxBucketCount:
// secs * 1000 / 500 in Java int arithmetic; a non-positive result (overflow)
// selects the default instead of the Java NoSuchElementException path.
func (s *Settings) profileBuckets() int {
	n := int(int32(s.ProfileKeepSecs)*1000) / profileBucketMillis
	if n <= 0 {
		return defaultProfileKeepSecs * 1000 / profileBucketMillis
	}
	return n
}
