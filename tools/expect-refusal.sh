#! /usr/bin/env bash

# Assert that a client run is refused, and refused for the reason expected.
#
# A leading `!` on the command is not on its own an assertion that authorization decided anything: an image
# build failure, a docker error, a TLS mismatch or a connection to a port nothing listens on all exit
# non-zero, and every one of them would pass as a refusal without a request ever reaching the control
# plane. So the gRPC status the control plane answered has to appear in the output too.

set -uo pipefail

usage() {
  cat <<EOF
USAGE $0 STATUS CMD...
Run CMD and require that it fails, with the gRPC status STATUS in its output.

Example:
  $0 Unauthenticated just runHtcmock
EOF
}

case "${1:-}" in
  '' | -h | --help)
    usage
    exit 2
    ;;
esac

expected="$1"
shift

if [ $# -eq 0 ]; then
  usage
  exit 2
fi

log="$(mktemp)"
trap 'rm -f "$log"' EXIT

# Streamed as it runs rather than captured, so a step killed by its timeout still leaves its output behind.
"$@" 2>&1 | tee "$log"
status="${PIPESTATUS[0]}"

if [ "$status" -eq 0 ]; then
  echo "FAIL: expected a refusal with $expected, but the command succeeded" >&2
  exit 1
fi

# The clients let an RpcException escape, so the status arrives as `Status(StatusCode="Unauthenticated",
# Detail="...")` in the unhandled-exception trace. Matching the rendering exactly means a future gRPC
# version that changes it fails this check rather than silently weakening it.
if ! grep -q "StatusCode=\"$expected\"" "$log"; then
  found="$(grep -o 'StatusCode="[A-Za-z]*"' "$log" | sort -u | paste -sd ' ' -)"
  echo "FAIL: expected a refusal with $expected, got ${found:-no gRPC status at all} — the command failed," \
       "but not by being refused" >&2
  exit 1
fi

echo "OK: refused with $expected"
