package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthSamplePreservesMissingQueue(t *testing.T) {
	for _, body := range []string{`{"udp":{"QueueLen":3,"QueueCap":1024},"ingest":{"Completed":10}}`, `{"udp":{},"ingest":{}}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		got := sampleHealth(context.Background(), server.Client(), strings.TrimPrefix(server.URL, "http://"))
		server.Close()
		if got.Error != "" || got.At.IsZero() {
			t.Fatalf("%+v", got)
		}
		_, present := got.UDP["QueueLen"]
		if present != strings.Contains(body, "QueueLen") {
			t.Fatalf("missing counter converted to zero: %+v", got)
		}
	}
}
