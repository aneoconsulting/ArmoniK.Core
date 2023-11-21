#! /usr/bin/env bash

now() {
  date '+%Y-%m-%d-%H.%M.%S'
}

prefix="${MONITOR_PREFIX:-monitor/$(now)/}"

case "$prefix" in
  */)
    prefix_dir="$prefix"
    prefix_file=""
    ;;
  */*)
    prefix_dir="$(dirname "$prefix")/"
    prefix_file="$(basename "$prefix")-"
    ;;
  *)
    prefix_dir="./"
    prefix_file="$prefix-"
    ;;
esac

mkdir -p "$prefix_dir"

mon() {
  while true; do
    time="$(now)"
    echo "========== ${time} =========="
    top -bn1 | head -20
    echo "-----------${time//?/-}-----------"
    df -ah

    # sleeps for 10 seconds and profile docker while we're at it
    curl -fsSL --unix-socket /var/run/docker.sock http://./debug/pprof/profile?seconds=10 --output "${prefix_dir}${prefix_file}docker-${time}.pprof.gz"
  done
}

mon > "${prefix_dir}${prefix_file}usage.log" 2>&1 & mon_pid=$!
echo "$mon_pid" > "$prefix.pid"

trap "kill $mon_pid" EXIT

if [ -n "$MONITOR_CD" ]; then
  cd "$MONITOR_CD"
fi

"$@"
