#!/bin/bash
# Fails when a benchmark falls under its floor (benches/floors.json, in cycles/s).
# Floors sit about ten times under the design targets: they catch gross regressions
# (quadratic path, blocking call, lock) on noisy shared runners, not a few percent.
# Usage: benches/check-floors.sh [criterion directory, default target/criterion]

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
dir="${1:-${CARGO_TARGET_DIR:-$here/../target}/criterion}"
status=0

while IFS=$'\t' read -r id floor; do
  file="$(find "$dir" -path '*/new/benchmark.json' -exec jq -r --arg id "$id" \
    'select(.full_id == $id) | input_filename' {} + | head -n1)"
  if [[ -z "$file" ]]; then
    echo "MISSING $id"
    status=1
    continue
  fi
  # Median time of one iteration in ns; one iteration is one cycle.
  ns="$(jq -r '.median.point_estimate' "$(dirname "$file")/estimates.json")"
  rate="$(jq -n --argjson ns "$ns" '1e9 / $ns | floor')"
  if (( rate < floor )); then
    echo "FAIL    $id: $rate cycles/s < floor $floor"
    status=1
  else
    echo "ok      $id: $rate cycles/s >= floor $floor"
  fi
done < <(jq -r 'to_entries[] | "\(.key)\t\(.value)"' "$here/floors.json")

exit $status
