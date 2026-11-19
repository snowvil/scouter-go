package server

import (
	"fmt"
	"syscall"

	"github.com/snowvil/scouter-go/internal/transport"
)

// fdReserve is the part of the open file limit kept for everything but TCP
// connections: database index and data files, logs, Kafka, HTTP and pprof.
// Java has no connection budgets, so on a host whose limit is below them its
// sockets and files run out together; connection-scale tests showed the Go
// server then failing every storage open (EMFILE) while the Agent sockets
// held the descriptors (docs/validation connection scale, scenario D).
func fdReserve(limit uint64) uint64 {
	return max(1024, limit/4)
}

// fdBudget is the TCP connection budgets that fit an open file limit.
type fdBudget struct{ agents, handshakes, clients int }

// fitFDBudget lowers the Agent, handshake and Client budgets so that their
// sum leaves fdReserve(limit) descriptors free. It returns the budgets and
// whether they changed; limit 0 (unknown) changes nothing.
func fitFDBudget(limit uint64, want fdBudget) (fdBudget, bool) {
	if limit == 0 {
		return want, false
	}
	reserve := fdReserve(limit)
	avail := limit / 2
	if limit > reserve*2 {
		avail = limit - reserve
	}
	if uint64(want.agents+want.handshakes+want.clients) <= avail {
		return want, false
	}
	got := want
	got.clients = min(want.clients, max(16, int(avail/16)))
	got.handshakes = min(want.handshakes, max(16, int(avail/8)))
	got.agents = max(1, min(want.agents, int(avail)-got.clients-got.handshakes))
	return got, true
}

// applyFDLimit fits cfg's budgets to the process open file limit (the Go
// runtime has already raised the soft limit to the hard one) and returns a
// warning when it had to lower them.
func applyFDLimit(cfg *Config) string {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return ""
	}
	want := fdBudget{agents: cfg.MaxAgentConnections, handshakes: cfg.MaxHandshakes, clients: cfg.MaxConnections}
	if want.agents == 0 {
		want.agents = transport.DefaultMaxAgentConnections
	}
	if want.handshakes == 0 {
		want.handshakes = transport.DefaultMaxHandshakes
	}
	got, changed := fitFDBudget(uint64(rl.Cur), want)
	if !changed {
		return ""
	}
	cfg.MaxAgentConnections, cfg.MaxHandshakes, cfg.MaxConnections = got.agents, got.handshakes, got.clients
	return fmt.Sprintf("WARNING: the open file limit %d leaves too few descriptors for the TCP budgets after a %d-file reserve for the database; Agent/handshake/Client connections lowered from %d/%d/%d to %d/%d/%d (connections past them are refused and logged). Raise the limit (ulimit -n, systemd LimitNOFILE) to keep the defaults",
		rl.Cur, fdReserve(uint64(rl.Cur)), want.agents, want.handshakes, want.clients, got.agents, got.handshakes, got.clients)
}
