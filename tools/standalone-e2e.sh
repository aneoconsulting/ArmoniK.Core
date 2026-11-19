#!/usr/bin/env bash
# End-to-end test of the standalone host (Standalone/src).
#
# Builds and starts the standalone host with SLOTS compute slots, starts one worker of the repository per slot,
# runs the matching client, then stops everything. Fails when the client fails, when a compute slot does not start,
# or when the standalone host logs an error.
#
# Usage: tools/standalone-e2e.sh htcmock|bench
#
# Environment:
#   SLOTS          number of compute slots and workers (default: 2)
#   RUN_DIR        folder of the data and logs (default: a new temporary folder; keep it short, it holds the sockets)
#   START_TIMEOUT  seconds to wait for the host and the slots to start (default: 120)
#   CLIENT_TIMEOUT seconds to wait for the client (default: 600)
#   NO_BUILD       set to 1 to skip the build
#   Any HtcMock__* or BenchOptions__* variable is passed to the client.

set -euo pipefail
shopt -s nullglob

WORKER="${1:-}"
case "$WORKER" in
  htcmock)
    WORKER_PROJECT=Tests/HtcMock/Server/src
    CLIENT_PROJECT=Tests/HtcMock/Client/src
    ;;
  bench)
    WORKER_PROJECT=Tests/Bench/Server/src
    CLIENT_PROJECT=Tests/Bench/Client/src
    ;;
  *)
    echo "Usage: $0 htcmock|bench" >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SLOTS="${SLOTS:-2}"
RUN_DIR="${RUN_DIR:-$(mktemp -d)}"
START_TIMEOUT="${START_TIMEOUT:-120}"
CLIENT_TIMEOUT="${CLIENT_TIMEOUT:-600}"
DATA_DIR="$RUN_DIR/data"
LOG_DIR="$RUN_DIR/logs"
mkdir -p "$DATA_DIR" "$LOG_DIR"

PIDS=()

cleanup() {
  status=$?
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  if [ "$status" -ne 0 ]; then
    for log in "$LOG_DIR"/*.log; do
      echo "===== $log (last 200 lines) ====="
      tail -n 200 "$log"
    done
  fi
  echo "Logs in $LOG_DIR"
  exit "$status"
}
trap cleanup EXIT

if [ "${NO_BUILD:-0}" != 1 ]; then
  for project in Standalone/src "$WORKER_PROJECT" "$CLIENT_PROJECT"; do
    dotnet build "$ROOT/$project"
  done
fi

worker_socket() { echo "$DATA_DIR/slot$1/worker.sock"; }
agent_socket() { echo "$DATA_DIR/slot$1/agent.sock"; }

echo "Starting the standalone host with $SLOTS slots, data in $DATA_DIR"
(
  cd "$ROOT/Standalone/src"
  exec env Standalone__Slots="$SLOTS" Standalone__DataFolder="$DATA_DIR" \
    dotnet run --no-build --no-launch-profile
) >"$LOG_DIR/standalone.log" 2>&1 &
PIDS+=($!)

for ((slot = 0; slot < SLOTS; slot++)); do
  echo "Starting the $WORKER worker of slot $slot"
  mkdir -p "$DATA_DIR/slot$slot"
  (
    cd "$ROOT/$WORKER_PROJECT"
    exec env ComputePlane__WorkerChannel__Address="$(worker_socket "$slot")" \
      ComputePlane__WorkerChannel__SocketType=UnixDomainSocket \
      ComputePlane__AgentChannel__Address="$(agent_socket "$slot")" \
      ComputePlane__AgentChannel__SocketType=UnixDomainSocket \
      dotnet run --no-build --no-launch-profile
  ) >"$LOG_DIR/worker$slot.log" 2>&1 &
  PIDS+=($!)
done

echo "Waiting for the $SLOTS compute slots to start"
deadline=$((SECONDS + START_TIMEOUT))
until [ "$(grep -c 'Compute slot {Slot} started' "$LOG_DIR/standalone.log" || true)" -ge "$SLOTS" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "The compute slots did not start within ${START_TIMEOUT}s" >&2
    exit 1
  fi
  if ! kill -0 "${PIDS[0]}" 2>/dev/null; then
    echo "The standalone host exited" >&2
    exit 1
  fi
  sleep 1
done

echo "Running the $WORKER client"
(
  cd "$ROOT/$CLIENT_PROJECT"
  GrpcClient__Endpoint=http://localhost:5001 timeout "$CLIENT_TIMEOUT" dotnet run --no-build --no-launch-profile
) >"$LOG_DIR/client.log" 2>&1 || {
  status=$?
  echo "The $WORKER client failed with exit code $status" >&2
  exit "$status"
}
echo "The $WORKER client succeeded"

errors="$(grep -c '"@l":"\(Error\|Fatal\)"' "$LOG_DIR/standalone.log" || true)"
if [ "$errors" -ne 0 ]; then
  echo "The standalone host logged $errors errors:" >&2
  grep '"@l":"\(Error\|Fatal\)"' "$LOG_DIR/standalone.log" | head -n 20 >&2
  exit 1
fi
echo "No error in the logs of the standalone host"
