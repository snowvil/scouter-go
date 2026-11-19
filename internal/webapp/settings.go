package webapp

import "time"

// Settings are the scouter.conf values the Java webapp reads on every
// request through ConfigureAdaptor (CorsFilter.java:43-44, AuthFilter.java:56-83,
// ZZ.java:41-42, ConnectionPool.java:49,56). The configuration owner stores
// a new value with Server.SetSettings after each reload; the next request
// uses it. Values are the applied Java values; this package does not parse
// scouter.conf.
type Settings struct {
	CORSAllowOrigin      string // net_http_api_cors_allow_origin; blank disables CORS
	CORSAllowCredentials string // net_http_api_cors_allow_credentials

	AuthIPEnabled      bool   // net_http_api_auth_ip_enabled
	AllowIPs           string // net_http_api_allow_ips (Configure.allowIpExact/allowIpMatch)
	AuthIPHeaderKey    string // net_http_api_auth_ip_header_key
	AuthSessionEnabled bool   // net_http_api_auth_session_enabled
	AuthBearerEnabled  bool   // net_http_api_auth_bearer_token_enabled

	// PoolIdleTimeout is net_webapp_tcp_client_pool_timeout. Without the key
	// Java applies the current net_tcp_client_so_timeout_ms (default 8000ms);
	// the owner passes that derived value. Zero or negative closes every
	// idle connection instead of reusing it, as in Java.
	PoolIdleTimeout time.Duration
}

// live is one immutable Settings value with its parsed allow list.
type live struct {
	Settings
	allow ipAllow
}

// SetSettings replaces the live settings; a request sees either the old or
// the new value as a whole. Like New, it warns when a change turns every
// authentication option off on a non-loopback listener.
func (s *Server) SetSettings(v Settings) {
	old := s.live.Swap(&live{Settings: v, allow: newIPAllow(v.AllowIPs)})
	if old != nil && (old.AuthIPEnabled || old.AuthSessionEnabled || old.AuthBearerEnabled) {
		s.warnNoAuth(v)
	}
}

// Settings returns the live settings.
func (s *Server) Settings() Settings { return s.live.Load().Settings }

func (s *Server) warnNoAuth(v Settings) {
	if !v.AuthIPEnabled && !v.AuthSessionEnabled && !v.AuthBearerEnabled && s.cfg.ListenAddr != "" && !loopbackAddr(s.cfg.ListenAddr) {
		s.cfg.Logf("WARNING: webapp REST API on %s has no authentication (net_http_api_auth_ip/session/bearer_token_enabled are all false)", s.cfg.ListenAddr)
	}
}
