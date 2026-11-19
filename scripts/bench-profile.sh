#!/usr/bin/env bash
# bench-profile.sh: collect Go profiles from scouter-go while scouter-replay
# drives a loopback load, then print `go tool pprof -top` / `-list` summaries.
#
# scouter-replay starts the server itself (fresh temp DB, loopback ports); this
# script adds --http=127.0.0.1:<port> through --server-arg and pulls
# /debug/pprof during the steady part of the run.
#
# Modes
#   default       CPU profile (--cpu-seconds) over HTTP, heap/allocs/goroutine
#                 snapshots. The server never enables block/mutex sampling by
#                 itself, so those HTTP profiles stay empty; they are not
#                 collected in this mode.
#   --contention  also passes --profile-dir to the server, which turns on
#                 SetBlockProfileRate(1ms) and SetMutexProfileFraction(10) at
#                 startup (cmd/scouter-go/profile.go). block/mutex deltas are
#                 then collected over HTTP for the same window. The server
#                 then already owns the CPU profiler (whole process life), so
#                 the HTTP CPU profile is unavailable; the whole-run cpu.pprof
#                 written at shutdown under <out>/shutdown/<pid>/ is summarized
#                 instead. syncx.Mutex (engine/store gate) is channel based:
#                 its waits show up in the block profile, not the mutex one.
#
# TPS is not computed here: the replay report (<out>/replay/report.json) holds
# the completed (stored and re-queried) XLog rate. Profiles cost CPU; compare
# TPS only between runs with the same mode.
#
# Usage
#   scripts/bench-profile.sh [options] [-- extra scouter-replay args]
# Options
#   --out DIR           output directory (default artifacts/bench-profile/<UTC stamp>)
#   --rate N            replay --rate, XLogs/s (default 1000)
#   --duration SEC      replay --duration (default 45)
#   --warmup SEC        replay --warmup (default 5)
#   --delay SEC         wait after the server is up before profiling (default: warmup)
#   --cpu-seconds SEC   profiling window (default 30)
#   --http-port PORT    pprof listener port (default: a free loopback port)
#   --contention        enable block/mutex sampling via --profile-dir (see above)
#   --list REGEX        pprof -list regex on the CPU (and block) profile; repeatable
#                       (default: ingest.*Engine.*AcceptFrom and storage.*Store.*PutXLog)
#   --nodecount N       pprof -top rows (default 25)
#   --server PATH       scouter-go binary (default dist/scouter-go)
#   --replay PATH       scouter-replay binary (default dist/scouter-replay)
#   --go PATH           go binary for `go tool pprof` (default: $GO, .tools/go/bin/go, go)
#
# Example (smoke, low load):
#   scripts/bench-profile.sh --rate 800 --warmup 1 --duration 9 --cpu-seconds 6 --delay 2
#   scripts/bench-profile.sh --contention --rate 800 --warmup 1 --duration 9 --cpu-seconds 6 --delay 2
# Extra replay args go after "--", e.g. -- --objects 40 --batch 32 --gomaxprocs 4
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="" rate=1000 duration=45 warmup=5 delay="" cpu_seconds=30 port="" contention=0 nodecount=25
server="$root/dist/scouter-go" replay="$root/dist/scouter-replay" gobin="${GO:-}"
lists=()
replay_extra=()

die() { echo "bench-profile: $*" >&2; exit 2; }
int_arg() { [[ $2 =~ ^[0-9]+$ ]] || die "$1 needs a non-negative integer, got '$2'"; }

while [[ $# -gt 0 ]]; do
	case $1 in
	--out) out=$2; shift 2 ;;
	--rate) rate=$2; shift 2 ;;
	--duration) int_arg "$1" "$2"; duration=$2; shift 2 ;;
	--warmup) int_arg "$1" "$2"; warmup=$2; shift 2 ;;
	--delay) int_arg "$1" "$2"; delay=$2; shift 2 ;;
	--cpu-seconds) int_arg "$1" "$2"; cpu_seconds=$2; shift 2 ;;
	--http-port) int_arg "$1" "$2"; port=$2; shift 2 ;;
	--contention) contention=1; shift ;;
	--list) lists+=("$2"); shift 2 ;;
	--nodecount) int_arg "$1" "$2"; nodecount=$2; shift 2 ;;
	--server) server=$2; shift 2 ;;
	--replay) replay=$2; shift 2 ;;
	--go) gobin=$2; shift 2 ;;
	--) shift; replay_extra=("$@"); break ;;
	-h | --help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
	*) die "unknown option $1 (use -- before scouter-replay args)" ;;
	esac
done

[[ -n $delay ]] || delay=$warmup
((cpu_seconds >= 1)) || die "--cpu-seconds must be >= 1"
((delay + cpu_seconds <= warmup + duration)) ||
	die "--delay ($delay) + --cpu-seconds ($cpu_seconds) must fit inside --warmup + --duration ($((warmup + duration))s of load)"
[[ -x $server ]] || die "server binary $server not found (make build)"
[[ -x $replay ]] || die "replay binary $replay not found (make build)"
if [[ -z $gobin ]]; then
	if [[ -x $root/.tools/go/bin/go ]]; then gobin=$root/.tools/go/bin/go; else gobin=$(command -v go || true); fi
fi
[[ -n $gobin && -x $gobin ]] || die "go binary not found (--go PATH or GO=...)"
command -v curl >/dev/null || die "curl is required"
((${#lists[@]})) || lists=('ingest.*Engine.*AcceptFrom' 'storage.*Store.*PutXLog$')
for a in ${replay_extra[@]+"${replay_extra[@]}"}; do
	case $a in --rate* | --duration* | --warmup* | --work-dir* | --report*) die "set $a through this script's options, not after --" ;; esac
done

if [[ -z $port ]]; then
	port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
fi
if [[ -z $out ]]; then
	out="$root/artifacts/bench-profile/$(date -u +%Y%m%dT%H%M%SZ)"
fi
[[ ! -e $out ]] || [[ -z $(ls -A "$out") ]] || die "--out $out exists and is not empty"
mkdir -p "$out"/{http,summary,replay}
out=$(cd "$out" && pwd)
base="http://127.0.0.1:$port"

server_args=("--server-arg=--http=127.0.0.1:$port")
((contention)) && server_args+=("--server-arg=--profile-dir=$out/shutdown")
replay_cmd=("$replay" --server "$server" --rate "$rate" --duration "${duration}s" --warmup "${warmup}s"
	--work-dir "$out/replay" --report "$out/replay/report.json" "${server_args[@]}" ${replay_extra[@]+"${replay_extra[@]}"})

{
	echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "git: $(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown) dirty=$(git -C "$root" status --porcelain --untracked-files=no 2>/dev/null | grep -q . && echo yes || echo no)"
	echo "go: $("$gobin" version)"
	echo "host: $(uname -sm) cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2) ncpu=$(getconf _NPROCESSORS_ONLN)"
	echo "mode: $([[ $contention == 1 ]] && echo contention || echo cpu)"
	echo "window: delay=${delay}s cpu_seconds=${cpu_seconds}s load=${warmup}s warmup+${duration}s steady rate=$rate/s"
	echo "pprof: $base/debug/pprof/"
	printf 'replay:'; printf ' %q' "${replay_cmd[@]}"; echo
} >"$out/conditions.txt"
cat "$out/conditions.txt"

replay_pid=""
cleanup() {
	if [[ -n $replay_pid ]] && kill -0 "$replay_pid" 2>/dev/null; then
		echo "bench-profile: stopping scouter-replay ($replay_pid)" >&2
		kill -TERM "$replay_pid" 2>/dev/null || true
		wait "$replay_pid" 2>/dev/null || true
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"${replay_cmd[@]}" >"$out/replay/replay.log" 2>&1 &
replay_pid=$!

# The pprof listener comes up with the server; the replay schedule starts right
# after the server reports ready.
up=0
for _ in $(seq 1 600); do
	if curl -fsS -m 2 "$base/health" -o "$out/http/health-start.json" 2>/dev/null; then up=1; break; fi
	kill -0 "$replay_pid" 2>/dev/null || break
	sleep 0.1
done
if ((!up)); then
	tail -20 "$out/replay/replay.log" >&2
	die "server pprof listener $base did not come up"
fi
echo "bench-profile: server up, profiling in ${delay}s for ${cpu_seconds}s"
sleep "$delay"

fetch() { # name url [timeout]
	local name=$1 url=$2 limit=${3:-30}
	if curl -fsS -m "$limit" "$url" -o "$out/http/$name" 2>"$out/http/$name.err"; then
		rm -f "$out/http/$name.err"
	else
		echo "bench-profile: fetch $name failed: $(cat "$out/http/$name.err")" >&2
		rm -f "$out/http/$name"
	fi
}

window=$((cpu_seconds + 15))
pids=()
if ((contention)); then
	fetch block.pprof "$base/debug/pprof/block?seconds=$cpu_seconds" "$window" &
	pids+=($!)
	fetch mutex.pprof "$base/debug/pprof/mutex?seconds=$cpu_seconds" "$window" &
	pids+=($!)
else
	fetch cpu.pprof "$base/debug/pprof/profile?seconds=$cpu_seconds" "$window" &
	pids+=($!)
fi
# Mid-window snapshots while the load is running.
sleep $((cpu_seconds / 2))
fetch goroutine-mid.txt "$base/debug/pprof/goroutine?debug=2"
fetch health-mid.json "$base/health"
wait "${pids[@]}" || true
fetch heap.pprof "$base/debug/pprof/heap?gc=1"
fetch allocs.pprof "$base/debug/pprof/allocs"
fetch goroutine.pprof "$base/debug/pprof/goroutine"
fetch goroutine-end.txt "$base/debug/pprof/goroutine?debug=1"
fetch health-end.json "$base/health"
echo "bench-profile: profiles collected; waiting for scouter-replay to finish (drain, restart, verify)"

set +e
wait "$replay_pid"
replay_rc=$?
set -e
replay_pid=""
echo "replay_exit: $replay_rc" >>"$out/conditions.txt"

pprof() { "$gobin" tool pprof "$@" 2>&1; }
summarize() { # label file [pprof options]
	local label=$1 file=$2
	shift 2
	[[ -s $file ]] || { echo "(missing $file)" >"$out/summary/$label.txt"; return; }
	pprof -top -nodecount="$nodecount" "$@" "$server" "$file" >"$out/summary/$label.txt"
}
cpu_file="$out/http/cpu.pprof"
if ((contention)); then
	# First pid directory in start order is the loaded server; a later one is
	# the replay restart (verification only).
	first=$(ls -dtr "$out"/shutdown/*/ 2>/dev/null | head -1 || true)
	if [[ -n $first ]]; then
		cpu_file="${first%/}/cpu.pprof"
		for k in block mutex heap; do
			summarize "shutdown-$k" "${first%/}/$k.pprof"
		done
	fi
	summarize block "$out/http/block.pprof"
	# Engine/Store gate (syncx.Mutex) waits only; the full block profile is
	# dominated by idle background selects.
	summarize block-gate "$out/http/block.pprof" -cum -focus='syncx\.\(\*Mutex\)'
	# Idle UDP workers wait in chanrecv on the datagram queue: near
	# workers x window seconds means the workers are not saturated.
	summarize block-udp-idle "$out/http/block.pprof" -focus='transport\.\(\*UDPReceiver\)\.Serve\.func' -ignore='syncx'
	summarize mutex "$out/http/mutex.pprof"
fi
summarize cpu "$cpu_file"
summarize cpu-cum "$cpu_file" -cum
summarize heap-inuse "$out/http/heap.pprof" -sample_index=inuse_space
summarize allocs-space "$out/http/allocs.pprof" -sample_index=alloc_space
summarize goroutine "$out/http/goroutine.pprof"
: >"$out/summary/list.txt"
for re in "${lists[@]}"; do
	for f in "$cpu_file" $([[ $contention == 1 ]] && echo "$out/http/block.pprof"); do
		[[ -s $f ]] || continue
		{
			echo "=== -list '$re' $(basename "$(dirname "$f")")/$(basename "$f")"
			# Lines with no samples in either column are dropped.
			pprof -list="$re" "$server" "$f" | awk '!/^ +\. +\. +[0-9]+:/' | head -200
		} >>"$out/summary/list.txt"
	done
done

echo
echo "== files ($out)"
(cd "$out" && find . -type f | sort | while read -r f; do printf '%10s  %s\n' "$(wc -c <"$f" | tr -d ' ')" "$f"; done)
for s in cpu cpu-cum block-gate block-udp-idle mutex heap-inuse allocs-space goroutine; do
	[[ -f $out/summary/$s.txt ]] || continue
	echo
	echo "== $s (head of summary/$s.txt)"
	sed -n '1,21p' "$out/summary/$s.txt"
done
if [[ -s $out/replay/report.json ]]; then
	echo
	echo "== replay report (excerpt; full: replay/report.json)"
	python3 -I -c '
import json, sys
r = json.load(open(sys.argv[1]))
def find(o, keys, path=""):
    if isinstance(o, dict):
        for k, v in o.items():
            p = path + "." + k if path else k
            if any(x in k.lower() for x in keys) and not isinstance(v, (dict, list)):
                print(f"  {p} = {v}")
            find(v, keys, p)
find(r, ("tps", "verdict", "stored", "missing", "rejected", "queue_full", "not_sent"))
' "$out/replay/report.json" | head -40
fi
for h in mid end; do
	[[ -s $out/http/health-$h.json ]] || continue
	echo
	echo "== /health at window $h (udp queue, ingest latency of the last --stats-interval window)"
	python3 -I -c '
import json, sys
h = json.load(open(sys.argv[1]))
u, i, st = h.get("udp", {}), h.get("ingest", {}), h.get("stats")
print("  udp: datagrams=%s rejected=%s queue=%s/%s" % (u.get("Datagrams"), u.get("Rejected"), u.get("QueueLen"), u.get("QueueCap")))
print("  ingest: completed=%s xlogs=%s rejected=%s storageErrors=%s" % (i.get("Completed"), i.get("XLogs"), i.get("Rejected"), i.get("StorageErrors")))
print("  stats:", json.dumps(st["ingestLatency"]) if st else "(none yet: add -- --server-arg=--stats-interval=5s)")
' "$out/http/health-$h.json"
done
echo
echo "bench-profile: done (replay exit $replay_rc); output $out"
exit "$replay_rc"
