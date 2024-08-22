#! /bin/sh

prgm="$0"
retries=3
wait=0

usage() {
  cat <<EOF
USAGE $prgm [-r|--retries NB_RETRY] [-w|--wait WAIT] [--] CMD...
Repeat CMD at most NB_RETRY times until it succeed

Options:
  -h, --help              Print the help
  -r, --retries NB_RETRY  Specify the number of retries (default: 3)
  -w, --wait    WAIT      Number of seconds to wait between retries (default: 0)
EOF
}

while [ $# -gt 0 ]; do
  arg="$1"
  case "$arg" in
    -h|--help)
      usage
      exit
      ;;
    -r|--retries)
      retries="${2:?missing number of retries}"
      shift 2
      ;;
    -w|--wait)
      wait="${2:?missing wait duration}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

: "${1:?missing cmd}"

ret=128
for i in $(seq "$retries"); do
  if "$@"; then
    exit
  else
    ret=$?
  fi
  sleep "$wait"
done

exit $ret
