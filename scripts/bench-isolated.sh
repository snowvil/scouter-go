#!/usr/bin/env bash
# bench-isolated.sh: measure the max sustained completed XLog/s of scouter-go
# on an isolated host (no other service, no production traffic), the same
# scenarios as docs/validation/2026-10-17-xlog-max-benchmark.md §16.
#
# The load generator (scouter-replay) only targets loopback and starts the
# server itself. On Linux the server and the generator can be pinned to
# separate CPUs (taskset), so the generator does not take the server's CPU.
# Nothing system-wide is changed: kernel limits such as net.core.rmem_max are
# only recorded (conditions.txt), never set.
#
# Usage
#   scripts/bench-isolated.sh --out DIR [--server-cpus LIST --load-cpus LIST] [--quick]
#     --out DIR            new result directory (must not exist)
#     --server-cpus LIST   taskset CPU list for scouter-go (Linux, e.g. 0-3)
#     --load-cpus LIST     taskset CPU list for the generator (e.g. 4-7)
#     --quick              one storm run and fewer search attempts
#     --smoke              4-second runs at low rates: checks the tooling only,
#                          the numbers mean nothing
#
# Binaries are taken from the script's directory (a bundle) or dist/:
# scouter-go, scouter-replay; bench-xlog-max.py next to this script.
# Results: DIR/summary.txt (one line per scenario), DIR/conditions.txt, and
# the driver's per-attempt reports. Keep result directories out of the
# repository; publish only measured numbers with their conditions.
set -euo pipefail

out="" server_cpus="" load_cpus="" quick=0 smoke=0
while [ $# -gt 0 ]; do
	case "$1" in
	--out) out=$2; shift 2 ;;
	--server-cpus) server_cpus=$2; shift 2 ;;
	--load-cpus) load_cpus=$2; shift 2 ;;
	--quick) quick=1; shift ;;
	--smoke) smoke=1 quick=1; shift ;;
	-h | --help) sed -n '2,26p' "$0"; exit 0 ;;
	*) echo "unknown option: $1" >&2; exit 2 ;;
	esac
done
[ -n "$out" ] || { echo "--out DIR required" >&2; exit 2; }
[ ! -e "$out" ] || { echo "$out exists; use a new directory" >&2; exit 2; }
if [ -n "$server_cpus$load_cpus" ] && { [ -z "$server_cpus" ] || [ -z "$load_cpus" ] || ! command -v taskset >/dev/null; }; then
	echo "--server-cpus and --load-cpus go together and need taskset (Linux)" >&2
	exit 2
fi

here=$(cd "$(dirname "$0")" && pwd)
find_bin() {
	for d in "$here" "$here/../dist"; do
		[ -x "$d/$1" ] && { echo "$d/$1"; return; }
	done
	echo "missing $1 (next to this script or in dist/)" >&2
	exit 2
}
server=$(find_bin scouter-go)
server_bin=$server
replay=$(find_bin scouter-replay)
driver="$here/bench-xlog-max.py"
[ -f "$driver" ] || { echo "missing $driver" >&2; exit 2; }
mkdir -p "$out"
out=$(cd "$out" && pwd)

# Server pinned to its CPUs; the driver and the generator run on the others.
if [ -n "$server_cpus" ]; then
	cat >"$out/scouter-go-pinned" <<EOF
#!/bin/sh
exec taskset -c $server_cpus "$server" "\$@"
EOF
	chmod +x "$out/scouter-go-pinned"
	server="$out/scouter-go-pinned"
fi
load() {
	if [ -n "$load_cpus" ]; then taskset -c "$load_cpus" "$@"; else "$@"; fi
}

{
	echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "uname: $(uname -srm)"
	echo "cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo ?)"
	echo "server_cpus: ${server_cpus:-unpinned} load_cpus: ${load_cpus:-unpinned}"
	echo "load_before: $(uptime | sed 's/.*load averages*: //')"
	for k in net.core.rmem_max net.core.rmem_default kern.ipc.maxsockbuf net.inet.udp.recvspace; do
		v=$(sysctl -n "$k" 2>/dev/null) && echo "$k: $v"
	done
	echo "server_binary_sha256: $( (sha256sum "$server_bin" 2>/dev/null || shasum -a 256 "$server_bin") | cut -d' ' -f1)"
	echo "note: the server asks for SO_RCVBUF 4 MiB (Java net_udp_so_rcvbuf_size); a smaller net.core.rmem_max caps it, as it caps Java"
} >"$out/conditions.txt"

attempts=20
[ $quick = 1 ] && attempts=6
smoke_args=()
storm_secs=30s storm_warmup=5s storm_rate=20000
if [ $smoke = 1 ]; then
	smoke_args=(--smoke)
	storm_secs=4s storm_warmup=0s storm_rate=2000
fi
common=(--kind go --replay "$replay" --server "$server" --objects 200 --senders 2 --duration 30 --warmup 5
	--batch 1 --query-concurrency 2 --max-attempts "$attempts")
search() { # name, extra driver args...
	local name=$1
	shift
	load python3 "$driver" "${common[@]}" ${smoke_args[@]+"${smoke_args[@]}"} --output "$out/$name" "$@" >"$out/$name.out" 2>&1 || true
	python3 - "$out/$name" "$name" >>"$out/summary.txt" <<'EOF'
import glob, json, sys
d, name = sys.argv[1], sys.argv[2]
try:
    s = json.load(open(d + "/summary.json"))
except Exception as e:
    print(f"{name}: no result ({e})"); sys.exit()
up, why = s.get("failing_upper_rate"), ""
for f in glob.glob(f"{d}/attempt-*-rate-{up}/decision.json"):
    why = ",".join(json.load(open(f))["decision"].get("failure_reasons") or [])
print(f"{name}: max {s.get('best_passing_rate')} XLog/s (fails at {up}: {why or s.get('termination')})")
EOF
}
search mix-text095 --initial-rate 10000 --max-rate 300000
search xlog-text095 --profile-ratio 0 --initial-rate 30000 --max-rate 500000
search xlog-text100 --profile-ratio 0 --replay-arg=--text-hit-ratio=1.0 --initial-rate 40000 --max-rate 500000
search mix-text100 --replay-arg=--text-hit-ratio=1.0 --initial-rate 25000 --max-rate 500000

# A storm of new texts (text hit 0.0) at a fixed 20,000 XLog/s: XLog loss
# must stay 0; tag counts may drop (secondary, counted).
storms=3
[ $quick = 1 ] && storms=1
for i in $(seq 1 $storms); do
	n="storm-$i"
	load "$replay" --server "$server" --rate "$storm_rate" --duration "$storm_secs" --warmup "$storm_warmup" --objects 200 --senders 2 --batch 1 \
		--profile-ratio 0 --query-concurrency 2 --text-hit-ratio 0.0 --seed $((900 + i)) \
		--work-dir "$out/$n" --report "$out/$n.json" >"$out/$n.log" 2>&1 || true
	python3 - "$out/$n.json" "$n" >>"$out/summary.txt" <<'EOF'
import json, sys
try:
    r = json.load(open(sys.argv[1]))
except Exception as e:
    print(f"{sys.argv[2]}: no result ({e})"); sys.exit()
sp = r["server_processes"][0]; line = sp.get("stats_line", "")
miss = r["send"]["transactions_sent_ok"] - r["verification"]["comparison"]["unique_expected_found"]
tag = line.split("PendingDropped:")[1].split(" ")[0] if "PendingDropped:" in line else "?"
print(f"{sys.argv[2]}: pass={r['pass']} xlog_missing={miss} udp_rejected={sp['stats']['udp']['Rejected']} tag_pending_dropped={tag}")
EOF
done
echo "load_after: $(uptime | sed 's/.*load averages*: //')" >>"$out/conditions.txt"
cat "$out/summary.txt"
