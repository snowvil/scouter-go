#!/usr/bin/env bash
# bench-ab.sh: paired A/B of two scouter-go builds by server CPU per 1,000
# completed (stored and re-queried) XLogs at a fixed replay rate.
#
# Runs of the same build vary by 10-13% on a shared or virtualised host
# (docs/performance-structural-review-2026-10-20.md §2.2), while the two runs
# of one alternating pair differ far less. So each pair runs A and B back to
# back (order alternating per pair: AB, BA, ...) and the decision uses the
# paired differences: their median and how many pairs agree in sign. A
# single run or a single profile is not evidence.
#
# Usage
#   scripts/bench-ab.sh --base BIN --new BIN [options] [-- extra scouter-replay args]
# Options
#   --out DIR       output directory (default artifacts/bench-ab/<UTC stamp>)
#   --pairs N       alternating pairs (default 8)
#   --rate N        replay --rate, XLogs/s (default 15000)
#   --duration SEC  replay --duration (default 30)
#   --warmup SEC    replay --warmup (default 5)
#   --replay PATH   scouter-replay binary (default dist/scouter-replay)
# Extra replay args go after "--", e.g.
#   -- --objects 200 --senders 2 --batch 1 --query-concurrency 2 --text-cardinality 2000
# A server wrapper script may be passed as --base/--new (e.g. to set GOGC).
#
# Output: <out>/{a,b}-<pair>.json (replay reports) and .log, <out>/conditions.txt,
# <out>/summary.txt.
# A run that fails the replay pass criteria is listed and kept out of the
# medians; the pair is then not compared.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="" pairs=8 rate=15000 duration=30 warmup=5 base="" new=""
replay="$root/dist/scouter-replay"
replay_extra=()

die() { echo "bench-ab: $*" >&2; exit 2; }
int_arg() { [[ $2 =~ ^[0-9]+$ ]] || die "$1 needs a non-negative integer, got '$2'"; }

while [[ $# -gt 0 ]]; do
	case $1 in
	--out) out=$2; shift 2 ;;
	--pairs) int_arg "$1" "$2"; pairs=$2; shift 2 ;;
	--rate) int_arg "$1" "$2"; rate=$2; shift 2 ;;
	--duration) int_arg "$1" "$2"; duration=$2; shift 2 ;;
	--warmup) int_arg "$1" "$2"; warmup=$2; shift 2 ;;
	--base) base=$2; shift 2 ;;
	--new) new=$2; shift 2 ;;
	--replay) replay=$2; shift 2 ;;
	--) shift; replay_extra=("$@"); break ;;
	-h | --help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
	*) die "unknown option $1 (use -- before scouter-replay args)" ;;
	esac
done

((pairs >= 1)) || die "--pairs must be >= 1"
[[ -x $base && -x $new ]] || die "--base and --new must be executable server binaries"
[[ -x $replay ]] || die "replay binary $replay not found (make build)"
command -v python3 >/dev/null || die "python3 is required"
for a in ${replay_extra[@]+"${replay_extra[@]}"}; do
	case $a in --rate* | --duration* | --warmup* | --work-dir* | --report* | --server=* | --server) die "set $a through this script's options, not after --" ;; esac
done
[[ -n $out ]] || out="$root/artifacts/bench-ab/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"

{
	echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "host: $(uname -srm) ncpu=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo ?)"
	echo "base: $base"
	echo "new: $new"
	echo "pairs: $pairs rate: $rate duration: ${duration}s warmup: ${warmup}s"
	echo "replay_extra: ${replay_extra[*]-}"
	if [[ -r /proc/sys/net/core/rmem_max ]]; then echo "net.core.rmem_max: $(cat /proc/sys/net/core/rmem_max)"; fi
} >"$out/conditions.txt"

run() { # label binary
	local label=$1 bin=$2
	rm -rf "$out/$label"
	"$replay" --server "$bin" --rate "$rate" --duration "${duration}s" --warmup "${warmup}s" \
		--work-dir "$out/$label" --report "$out/$label.json" ${replay_extra[@]+"${replay_extra[@]}"} >"$out/$label.log" 2>&1 || true
	echo "bench-ab: $label done" >&2
}

for ((i = 1; i <= pairs; i++)); do
	if ((i % 2)); then run "a-$i" "$base"; run "b-$i" "$new"; else run "b-$i" "$new"; run "a-$i" "$base"; fi
done

python3 -I - "$out" "$pairs" <<'PY' | tee "$out/summary.txt"
import json, os, statistics as st, sys

out, pairs = sys.argv[1], int(sys.argv[2])

def run(label):
    try:
        d = json.load(open(os.path.join(out, label + ".json")))
        p = d["server_processes"][0]
        v = d["verification"]["comparison"]["unique_expected_found"]
        cpu = p["cpu_user_seconds"] + p["cpu_system_seconds"]
        return {"pass": bool(d["pass"]), "per1k": cpu / v * 1000 if v else None,
                "rss": p.get("rss_sampled_max_kib", 0) / 1024}
    except (OSError, KeyError, ValueError, IndexError, TypeError) as e:
        return {"pass": False, "per1k": None, "rss": 0, "error": str(e)}

rows, diffs = [], []
for i in range(1, pairs + 1):
    a, b = run(f"a-{i}"), run(f"b-{i}")
    ok = a["pass"] and b["pass"] and a["per1k"] and b["per1k"]
    d = (b["per1k"] - a["per1k"]) / a["per1k"] * 100 if ok else None
    if d is not None:
        diffs.append(d)
    rows.append((i, a, b, d))

print("pair  base_cpu_per_1k  new_cpu_per_1k  diff%   base_rss_mib  new_rss_mib")
for i, a, b, d in rows:
    f = lambda r: f"{r['per1k']:.5f}" if r["per1k"] else "-"
    flag = "" if a["pass"] and b["pass"] else "  (failed run: " + ",".join(x for x, r in (("base", a), ("new", b)) if not r["pass"]) + ")"
    print(f"{i:>4}  {f(a):>15}  {f(b):>14}  {d:+6.2f}  {a['rss']:>12.0f}  {b['rss']:>11.0f}{flag}" if d is not None
          else f"{i:>4}  {f(a):>15}  {f(b):>14}  {'-':>6}  {a['rss']:>12.0f}  {b['rss']:>11.0f}{flag}")
good = [(a, b) for _, a, b, d in rows if d is not None]
if not diffs:
    print("no complete pair")
    sys.exit(0)
lower = sum(1 for d in diffs if d < 0)
print()
print(f"compared pairs: {len(diffs)} of {pairs}")
print(f"median cpu per 1k: base {st.median(a['per1k'] for a, _ in good):.5f}  new {st.median(b['per1k'] for _, b in good):.5f}")
print(f"paired diff (new vs base): median {st.median(diffs):+.2f}%  mean {st.mean(diffs):+.2f}%"
      + (f"  sd {st.stdev(diffs):.2f}%" if len(diffs) > 1 else ""))
print(f"pairs where new used less CPU: {lower} of {len(diffs)}")
print(f"median rss max (MiB): base {st.median(a['rss'] for a, _ in good):.0f}  new {st.median(b['rss'] for _, b in good):.0f}")
print("decide on the paired differences (median and sign agreement), not on single runs")
PY
