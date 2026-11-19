package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type healthSample struct {
	At     time.Time        `json:"at"`
	UDP    map[string]int64 `json:"udp"`
	Ingest map[string]int64 `json:"ingest"`
	// Locks are the server's --lock-stats sums (absent without the flag):
	// the difference of two samples over their interval is the occupancy.
	Locks map[string]lockSample `json:"locks,omitempty"`
	Error string                `json:"error,omitempty"`
}

type lockSample struct {
	Acquired uint64  `json:"acquired"`
	HeldMs   float64 `json:"heldMs"`
	WaitedMs float64 `json:"waitedMs"`
}

func sampleHealth(ctx context.Context, c *http.Client, addr string) healthSample {
	s := healthSample{At: time.Now()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		s.Error = err.Error()
		return s
	}
	res, err := c.Do(req)
	if err != nil {
		s.Error = err.Error()
		return s
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		s.Error = fmt.Sprintf("health HTTP %d", res.StatusCode)
		return s
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&s); err != nil {
		s.Error = err.Error()
	}
	return s
}
func startHealthSamples(parent context.Context, addr string, rep *report) func() {
	if addr == "" {
		return func() {}
	}
	if err := requireLoopback(addr); err != nil {
		rep.HealthSamples = append(rep.HealthSamples, healthSample{At: time.Now(), Error: err.Error()})
		return func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		c := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		defer c.CloseIdleConnections()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			if len(rep.HealthSamples) < maxRSSSamples {
				rep.HealthSamples = append(rep.HealthSamples, sampleHealth(ctx, c, addr))
			} else {
				rep.HealthSamplesDropped++
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return func() { once.Do(func() { cancel(); <-done }) }
}
