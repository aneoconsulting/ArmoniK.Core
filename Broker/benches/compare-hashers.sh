#!/bin/bash
# Compares the hashers of the hot-path tables (src/hashing.rs) on the scheduler benchmark.
# Builds one binary per hasher before measuring anything, then runs them in rotating order
# so that a drift of the machine hits every hasher alike, and prints for each benchmark
# the median over the rounds, its spread and its gap to SipHash, the default.
#
# Usage: benches/compare-hashers.sh [rounds, default 3]
# Environment:
#   FILTER   criterion regex of the benchmarks to run (default: those that hash the most)
#   CORE     core to pin the runs to with taskset (default 2; empty to disable)
#   MEASURE  measurement time per benchmark, in seconds (default 3)

set -euo pipefail

cd "$(dirname "$0")/.."
rounds="${1:-3}"
filter="${FILTER:-^scheduler/(cycle/affinity|affinity-scoring|affinity-nodes/100|affinity-mirror/250000|affinity-fleet)}"
core="${CORE-2}"
out="${CARGO_TARGET_DIR:-$PWD/target}/compare-hashers"
variants=(sip fx fold ahash)

pin=()
if [[ -n "$core" ]] && command -v taskset > /dev/null; then
  pin=(taskset -c "$core")
fi

rm -rf "$out"
mkdir -p "$out/bin"
for v in "${variants[@]}"; do
  features=()
  [[ "$v" == sip ]] || features=(--features "hash-$v")
  echo "build $v"
  exe="$(cargo bench --locked --bench scheduler --no-run "${features[@]}" 2>&1 \
    | tee "$out/build-$v.log" \
    | sed -n 's|.*Executable benches/scheduler.rs (\(.*\))|\1|p')"
  cp "$exe" "$out/bin/$v"
done

for ((r = 1; r <= rounds; r++)); do
  for ((k = 0; k < ${#variants[@]}; k++)); do
    v="${variants[$(((k + r - 1) % ${#variants[@]}))]}"
    echo "round $r/$rounds $v"
    CRITERION_HOME="$out/criterion" "${pin[@]}" "$out/bin/$v" --bench \
      --warm-up-time 1 --measurement-time "${MEASURE:-3}" \
      --save-baseline "$v-r$r" "$filter" > "$out/run-$v-r$r.log" 2>&1
  done
done

# One line per run: benchmark, hasher, cycles/s (from the criterion median).
find "$out/criterion" -name benchmark.json -path '*-r*' | while read -r f; do
  d="$(dirname "$f")"
  v="$(basename "$d")"
  printf '%s\t%s\t%s\n' "$(jq -r .full_id "$f")" "${v%-r*}" \
    "$(jq '1e9 / .median.point_estimate' "$d/estimates.json")"
done | sort -t $'\t' -k1,1 -k2,2 -k3,3g > "$out/runs.tsv"

echo
awk -F '\t' -v order="${variants[*]}" '
  function flush() {
    if (n == 0) return
    med[id, v] = (n % 2) ? x[(n + 1) / 2] : (x[n / 2] + x[n / 2 + 1]) / 2
    spread[id, v] = (x[n] - x[1]) / med[id, v] * 50
    n = 0
  }
  { if ($1 != id || $2 != v) { flush(); id = $1; v = $2; if (!(id in seen)) { seen[id]; ids[++nid] = id } }
    x[++n] = $3 }
  END {
    flush()
    nv = split(order, vs, " ")
    printf "%-52s", "cycles/s: median ±half spread, gap to sip"
    for (j = 1; j <= nv; j++) printf "%26s", vs[j]
    printf "\n"
    for (i = 1; i <= nid; i++) {
      printf "%-52s", ids[i]
      for (j = 1; j <= nv; j++) {
        m = med[ids[i], vs[j]]
        if (m == "") { printf "%26s", "-"; continue }
        gap = (j == 1) ? "" : sprintf("%+.0f%%", (m / med[ids[i], vs[1]] - 1) * 100)
        printf "%12.0f ±%3.0f%% %8s", m, spread[ids[i], vs[j]], gap
      }
      printf "\n"
    }
  }' "$out/runs.tsv"
echo
echo "raw runs: $out/runs.tsv"
