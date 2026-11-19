#!/usr/bin/env bash
# connection-scale.sh: large Agent TCP connection scenarios against scouter-go
# on loopback (docs/validation/2026-10-22-connection-scale.md). The server runs
# on a fresh DB under --out; `scouter-replay agents` emulates Java Agent TCP
# sessions (handshake, KEEP_ALIVE answers, 1s/4s reconnect) plus UDP and
# Client traffic, and samples /health, pprof and /proc of the server.
#
# Usage
#   scripts/connection-scale.sh --out DIR SCENARIO [SCENARIO...]
#     A<N>      steady scale: N agents (20% V1), UDP heartbeat/counter/XLog,
#               2 Client query workers and a Client connect probe
#     idle<N>   N agents only, no UDP/Client: the KEEP_ALIVE cost
#     B<N>      restart storm: N agents, SIGTERM + immediate restart at 40s
#     Bkill<N>  restart storm after SIGKILL (lock.dat refusals retried)
#     C<N>:<L>  N agents with --max-agent-connections=L (+UDP, Clients, probe)
#     H<N>:<L>  restart storm of N agents with --max-handshakes=L
#     D<N>:<F>  N agents, server ulimit -n F (accept EMFILE backoff)
#     E         orphan relays: 200 agents (UDP heartbeat so the server knows
#               them) answer 45s late, 60 relay Clients
#   Env: SERVER, REPLAY (binaries; default next to this script or dist/),
#        DURATION (default 120s), XLOG_RATE (default 200), PORT (6300),
#        HTTP_PORT (6380), NOFILE (default 65536, tool and server),
#        KEEP_DB=1 keeps each scenario DB (default: size recorded in
#        db-size.txt, then removed; per-object visitor files are ~0.7MB per
#        object per day and per hour, so thousands of agents fill a disk fast).
# Loopback only. Nothing system-wide is changed; the script records the
# kernel limits it ran under (DIR/conditions.txt).
set -euo pipefail

out=""
while [ $# -gt 0 ]; do
	case "$1" in
	--out) out=$2; shift 2 ;;
	-h | --help) sed -n '2,26p' "$0"; exit 0 ;;
	--) shift; break ;;
	-*) echo "unknown option: $1" >&2; exit 2 ;;
	*) break ;;
	esac
done
[ -n "$out" ] && [ $# -gt 0 ] || { echo "usage: $0 --out DIR SCENARIO..." >&2; exit 2; }
mkdir -p "$out"
out=$(cd "$out" && pwd)

here=$(cd "$(dirname "$0")" && pwd)
find_bin() {
	for d in "$here" "$here/../dist"; do
		[ -x "$d/$1" ] && { echo "$d/$1"; return; }
	done
	echo "missing $1 (set SERVER/REPLAY or put it next to this script)" >&2
	exit 2
}
SERVER=${SERVER:-$(find_bin scouter-go)}
REPLAY=${REPLAY:-$(find_bin scouter-replay)}
DURATION=${DURATION:-120s}
XLOG_RATE=${XLOG_RATE:-200}
PORT=${PORT:-6300}
HTTP_PORT=${HTTP_PORT:-6380}
NOFILE=${NOFILE:-65536}
ulimit -n "$NOFILE"

{
	date -Is
	uname -a
	nproc
	free -m 2>/dev/null || true
	for k in net.core.somaxconn net.ipv4.tcp_max_syn_backlog net.ipv4.tcp_syncookies net.ipv4.ip_local_port_range net.core.rmem_max; do
		sysctl "$k" 2>/dev/null || true
	done
	echo "ulimit -n $(ulimit -n) (hard $(ulimit -Hn))"
	echo "server $SERVER $(sha256sum "$SERVER" 2>/dev/null | cut -c1-16)"
	echo "replay $REPLAY $(sha256sum "$REPLAY" 2>/dev/null | cut -c1-16)"
} >"$out/conditions.txt"

work=""
server_pid=""
# start_server NOFILE ARGS...: fresh or existing $work/db, waits for the ready file.
start_server() {
	local nofile=$1
	shift
	rm -f "$work/ready.json"
	mkdir -p "$work/logs" "$work/plugin" "$work/conf"
	(cd "$work" && ulimit -n "$nofile" && exec "$SERVER" --conf= --db "$work/db" --udp "127.0.0.1:$PORT" --tcp "127.0.0.1:$PORT" \
		--http "127.0.0.1:$HTTP_PORT" --ready-file "$work/ready.json" --user admin --password admin \
		--log-dir "$work/logs" --plugin-dir "$work/plugin" --telegraf-config "$work/conf/scouter-telegraf.xml" \
		--exit-file=false --stats-interval 5s "$@") >>"$work/server.log" 2>&1 &
	server_pid=$!
	echo "$server_pid" >"$work/server.pid"
	for _ in $(seq 1 600); do
		[ -s "$work/ready.json" ] && return 0
		kill -0 "$server_pid" 2>/dev/null || { echo "server exited during start" >&2; return 1; }
		sleep 0.1
	done
	echo "server not ready" >&2
	return 1
}
stop_server() {
	local sig=${1:-TERM}
	[ -n "$server_pid" ] || return 0
	kill -"$sig" "$server_pid" 2>/dev/null || true
	wait "$server_pid" 2>/dev/null || true
	echo "server $server_pid stopped by SIG$sig at $(date -Is)" >>"$work/events.txt"
	server_pid=""
}
agents() {
	"$REPLAY" agents --tcp "127.0.0.1:$PORT" --http "127.0.0.1:$HTTP_PORT" --pid-file "$work/server.pid" \
		--report "$work/agents.json" "$@" >"$work/agents.log" 2>&1
}
finish() {
	echo "$(du -s --apparent-size -B1M "$work/db" | cut -f1)MiB apparent, $(find "$work/db" -path '*visit*' -type f | wc -l) visitor files" >"$work/db-size.txt"
	[ "${KEEP_DB:-0}" = 1 ] || rm -rf "$work/db"
	grep -c . "$work/server.log" >"$work/server-log-lines.txt" || true
	grep -E "rejected|accept|orphan|relay|EMFILE|too many open files" "$work/server.log" >"$work/server-log-notable.txt" || true
	tail -n 3 "$work/agents.log"
	echo "== $(basename "$work") server log lines: $(cat "$work/server-log-lines.txt"), notable: $(grep -c . "$work/server-log-notable.txt" || true)"
}
trap 'stop_server KILL' EXIT

full() { echo --udp "127.0.0.1:$PORT" --xlog-rate "$XLOG_RATE" --clients 2 --probe-interval 5s; }

for sc in "$@"; do
	work="$out/$sc"
	[ ! -e "$work" ] || { echo "$work exists" >&2; exit 2; }
	mkdir -p "$work"
	echo "== $sc start $(date -Is)"
	case "$sc" in
	A*)
		n=${sc#A}
		start_server "$NOFILE"
		agents --agents "$n" --spread 20s --duration "$DURATION" $(full)
		stop_server
		;;
	idle*)
		n=${sc#idle}
		start_server "$NOFILE"
		agents --agents "$n" --spread 20s --duration "$DURATION"
		stop_server
		;;
	Bkill*|B*)
		sig=TERM n=${sc#B}
		case "$sc" in Bkill*) sig=KILL n=${sc#Bkill} ;; esac
		start_server "$NOFILE"
		agents --agents "$n" --spread 10s --duration "$DURATION" $(full) &
		apid=$!
		sleep 40
		stop_server "$sig"
		# A crash leaves lock.dat fresh; retry like the Java 5s stale rule needs.
		for _ in $(seq 1 30); do
			start_server "$NOFILE" && break
			sleep 1
		done
		echo "server restarted pid $server_pid at $(date -Is)" >>"$work/events.txt"
		wait "$apid" || true
		stop_server
		;;
	C*)
		n=${sc#C} n=${n%%:*} limit=${sc#*:}
		start_server "$NOFILE" --max-agent-connections "$limit"
		agents --agents "$n" --spread 10s --duration "$DURATION" $(full)
		stop_server
		;;
	H*)
		n=${sc#H} n=${n%%:*} limit=${sc#*:}
		start_server "$NOFILE" --max-handshakes "$limit"
		agents --agents "$n" --duration "$DURATION" $(full)
		stop_server
		;;
	D*)
		n=${sc#D} n=${n%%:*} limit=${sc#*:}
		start_server "$limit"
		agents --agents "$n" --spread 10s --duration "$DURATION" $(full)
		stop_server
		;;
	E)
		start_server "$NOFILE"
		agents --agents 200 --udp "127.0.0.1:$PORT" --duration "${DURATION}" --agent-delay 45s --relay-clients 60 --relay-at 10s --relay-timeout 8s --clients 1 --probe-interval 5s
		stop_server
		;;
	*)
		echo "unknown scenario $sc" >&2
		exit 2
		;;
	esac
	finish
done
