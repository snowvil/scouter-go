package httpapi

import (
	"compress/gzip"
	"net/http"
	"strings"
)

// gzipMinSize is GzipHandler.setMinGzipSize(1024).
const gzipMinSize = 1024

// gzipHandler is the Jetty GzipHandler subset the server configures:
// GET/POST/PUT/DELETE responses of at least gzipMinSize bytes are compressed
// when the request accepts gzip and the handler did not set Content-Encoding.
// Smaller responses pass through unchanged. The /counter, /register and
// /telegraf answers have empty bodies, so in practice nothing is compressed.
func gzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Accept-Encoding")
		switch r.Method {
		case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
		default:
			next.ServeHTTP(w, r)
			return
		}
		if !acceptsGzip(r.Header.Values("Accept-Encoding")) {
			next.ServeHTTP(w, r)
			return
		}
		g := &gzipWriter{ResponseWriter: w}
		next.ServeHTTP(g, r)
		g.finish()
	})
}

func acceptsGzip(values []string) bool {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			coding, params, _ := strings.Cut(strings.TrimSpace(part), ";")
			if !strings.EqualFold(strings.TrimSpace(coding), "gzip") {
				continue
			}
			if q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q="); ok && (q == "0" || q == "0.0" || q == "0.00" || q == "0.000") {
				return false
			}
			return true
		}
	}
	return false
}

// gzipWriter buffers up to gzipMinSize bytes before deciding.
type gzipWriter struct {
	http.ResponseWriter
	status  int
	buf     []byte
	decided bool
	gz      *gzip.Writer
}

func (g *gzipWriter) WriteHeader(status int) {
	if g.status == 0 {
		g.status = status
	}
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	if g.status == 0 {
		g.status = http.StatusOK
	}
	if g.decided {
		if g.gz != nil {
			return g.gz.Write(p)
		}
		return g.ResponseWriter.Write(p)
	}
	g.buf = append(g.buf, p...)
	if len(g.buf) >= gzipMinSize {
		if err := g.decide(true); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (g *gzipWriter) decide(large bool) error {
	g.decided = true
	h := g.ResponseWriter.Header()
	if large && h.Get("Content-Encoding") == "" {
		h.Set("Content-Encoding", "gzip")
		h.Del("Content-Length")
		g.ResponseWriter.WriteHeader(g.status)
		g.gz = gzip.NewWriter(g.ResponseWriter)
		_, err := g.gz.Write(g.buf)
		g.buf = nil
		return err
	}
	if g.status != 0 {
		g.ResponseWriter.WriteHeader(g.status)
	}
	_, err := g.ResponseWriter.Write(g.buf)
	g.buf = nil
	return err
}

func (g *gzipWriter) finish() {
	if !g.decided {
		g.decide(false)
		return
	}
	if g.gz != nil {
		g.gz.Close()
	}
}
