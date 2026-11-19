package webapp

import (
	"strings"
	"testing"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// Review follow-ups (M1 reserved key spaces, M3 profile limits, M6 body cap).

func TestBCustomKvReservedKeySpaces(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	for _, ks := range []string{"__WEB_SESSION__", "___WEB_SESSION_PRIVATE___"} {
		msg := "fail to validate input parameters. : keySpace is reserved: " + ks
		bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/space/"+ks+"/alice", ""), 400, msg)
		bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/"+ks, `{"key":"alice","value":"V1.1.x.alice"}`), 400, msg)
		bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/"+ks+"/alice/:ttl", `{"ttl":1}`), 400, msg)
		bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/kv/space/"+ks+"/alice,bob/:bulk", ""), 400, msg)
		bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/"+ks+"/:bulk", `{"kvList":[{"key":"alice","value":"x"}]}`), 400, msg)
	}
	for _, cmd := range []string{"GET_CUSTOM_KV", "SET_CUSTOM_KV", "SET_CUSTOM_TTL", "GET_CUSTOM_KV_BULK", "SET_CUSTOM_KV_BULK"} {
		if n := len(f.requests(cmd)); n != 0 {
			t.Fatalf("%s reached the collector %d times", cmd, n)
		}
	}
	// Other key spaces, including near-misses, are still served.
	// (SET_CUSTOM_TTL is not registered in the fake: reaching the collector is a 500.)
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/__WEB_SESSION_/k/:ttl", `{"ttl":1}`), 500, "")
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/web", `{"key":"k","value":"v"}`), 200, "true")
}

func TestBKvBodyFollowsMaxBodyBytes(t *testing.T) {
	_, _, hs := bSetup(t, func(c *Config) { c.MaxBodyBytes = 64 })
	small := `{"key":"k","value":"v"}`
	big := `{"key":"k","value":"` + strings.Repeat("x", 64) + `"}`
	bExpect(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/ns", small), 200, "true")
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/ns", big), 400, "fail to validate input parameters. : request body is too large")
	bExpectError(t, bHTTP(t, hs, "PUT", "/scouter/v1/kv/space/ns/:bulk", `{"kvList":[`+strings.Repeat(`{"key":"k","value":"v"},`, 4)+`{"key":"k","value":"v"}]}`), 400,
		"fail to validate input parameters. : request body is too large")
}

func bProfileOf(t *testing.T, n int) []byte {
	t.Helper()
	var out []byte
	for i := 0; i < n; i++ {
		b, err := protocol.EncodeStep(&protocol.MethodStep{SingleStep: protocol.SingleStep{Parent: int32(i - 1), Index: int32(i)}, Hash: 11, Elapsed: 1}, protocol.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b...)
	}
	return out
}

func TestBProfileStepLimits(t *testing.T) {
	f, _, hs := bSetup(t, nil)
	f.static("TRANX_PROFILE", &protocol.ProfilePack{Profile: bProfileOf(t, 5000)})
	res := bHTTP(t, hs, "GET", "/scouter/v1/profile/20240101/1", "")
	bExpect(t, res, 200, "")
	if n := strings.Count(resultOf(t, res.body), `"stepTypeName":"METHOD"`); n != 5000 {
		t.Fatalf("profile returned %d steps, want 5000", n)
	}
	res = bHTTP(t, hs, "GET", "/scouter/v1/profile-data/20240101/1", "")
	bExpect(t, res, 200, "")
	if n := strings.Count(resultOf(t, res.body), `"mainValue":`); n != 5000 {
		t.Fatalf("profile-data returned %d steps, want 5000", n)
	}
	// Above the 65,536-step bound the decode fails explicitly (500), never truncates.
	f.static("TRANX_PROFILE", &protocol.ProfilePack{Profile: bProfileOf(t, 1<<16+1)})
	bExpectError(t, bHTTP(t, hs, "GET", "/scouter/v1/profile/20240101/1", ""), 500, "")
}
