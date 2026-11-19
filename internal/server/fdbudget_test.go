package server

import "testing"

func TestFitFDBudget(t *testing.T) {
	defaults := fdBudget{agents: 16384, handshakes: 2048, clients: 1024}
	for _, c := range []struct {
		limit   uint64
		want    fdBudget
		changed bool
	}{
		{0, defaults, false},
		{524288, defaults, false}, // a usual server hard limit: defaults fit
		{1 << 16, fdBudget{16384, 2048, 1024}, false},
		{20000, fdBudget{12188, 1875, 937}, true}, // reserve 5000, 15000 left
		{2048, fdBudget{832, 128, 64}, true},      // limit <= 2x reserve: half
		{1024, fdBudget{416, 64, 32}, true},
		{64, fdBudget{1, 16, 16}, true},
	} {
		got, changed := fitFDBudget(c.limit, defaults)
		if got != c.want || changed != c.changed {
			t.Errorf("limit %d: %+v %v, want %+v %v", c.limit, got, changed, c.want, c.changed)
		}
		if changed && c.limit > 64 && uint64(got.agents+got.handshakes+got.clients) > c.limit-min(c.limit/2, fdReserve(c.limit)) {
			t.Errorf("limit %d: budgets %+v leave no reserve", c.limit, got)
		}
	}
}
