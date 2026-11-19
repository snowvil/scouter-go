package webapp_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
	"github.com/snowvil/scouter-go/internal/server"
	"github.com/snowvil/scouter-go/internal/transport"
	"github.com/snowvil/scouter-go/internal/webapp"
)

// TestEndToEndAgainstGoServer runs the webapp against a real Go collector
// (Client TCP + UDP ingest), assembled only inside this test.
func TestEndToEndAgainstGoServer(t *testing.T) {
	xml, err := os.ReadFile(filepath.Join("..", "..", "cmd", "scouter-go", "counters.xml"))
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(t.TempDir(), "ready.json")
	r, err := server.Open(server.Config{Directory: t.TempDir(), Location: time.UTC, UDP: "127.0.0.1:0", TCP: "127.0.0.1:0", ReadyFile: ready,
		Username: "admin", Password: "admin", CounterXML: xml})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	b, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var endpoints server.Endpoints
	if err := json.Unmarshal(b, &endpoints); err != nil {
		t.Fatal(err)
	}

	cfg := webapp.DefaultConfig()
	cfg.CollectorAddr, cfg.Username, cfg.Password = endpoints.TCP, "admin", "admin"
	cfg.Location = time.UTC
	cfg.Logf = t.Logf
	w, err := webapp.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	hs := httptest.NewServer(w.Handler())
	defer hs.Close()

	body := get(t, hs.URL+"/scouter/v1/info/server")
	host, port, _ := net.SplitHostPort(endpoints.TCP)
	id := strconv.FormatInt(int64(int32(crc32.ChecksumIEEE([]byte(host+port)))), 10)
	if !strings.HasPrefix(body, `{"status":"200","requestId":"#`) || !strings.Contains(body, `"result":[{"id":"`+id+`","name":"1","connected":true,"serverTime":"`) {
		t.Fatal(body)
	}

	// Feed one object and one realtime counter over UDP.
	name := "/e2e/tc1"
	hash := int32(crc32.ChecksumIEEE([]byte(name)))
	send(t, endpoints.UDP, &protocol.ObjectPack{ObjType: "tomcat", ObjHash: hash, ObjName: name, Address: "127.0.0.1", Version: "e2e", Alive: true,
		Tags: protocol.Value{Type: protocol.ValueMap}})
	send(t, endpoints.UDP, &protocol.PerfCounterPack{Time: time.Now().UnixMilli(), ObjName: name, TimeType: 1,
		Data: protocol.Value{Type: protocol.ValueMap, Entries: []protocol.Entry{{Key: "TPS", Value: protocol.Value{Type: protocol.ValueFloat, Float: 12.5}}}}})
	want := `"result":[{"objHash":"` + strconv.Itoa(int(hash)) + `","objName":"` + name + `","name":"TPS","displayName":"TPS","unit":"tps","value":"12.5"}]}`
	deadline := time.Now().Add(5 * time.Second)
	for {
		body = get(t, hs.URL+"/scouter/v1/counter/realTime/TPS/ofType/tomcat")
		if strings.HasSuffix(body, want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("realtime counter not served:\n got %s\nwant suffix %s", body, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %s", url, resp.StatusCode, b)
	}
	return string(b)
}

func send(t *testing.T, addr string, p protocol.Pack) {
	t.Helper()
	raw, err := protocol.EncodePack(p, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	buf := binary.BigEndian.AppendUint32(nil, transport.UDPCafe)
	if _, err := c.Write(append(buf, raw...)); err != nil {
		t.Fatal(err)
	}
}
