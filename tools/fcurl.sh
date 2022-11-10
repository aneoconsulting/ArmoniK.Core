#! /bin/bash

exec 3<>"/dev/tcp/$1/$2"
echo -e "GET $3 HTTP/1.1\r\nHost: $1:$2\r\nConnection: close\r\n\r\n">&3 &
read content <&3
wait
case "$content" in
  "HTTP/1.1 200"*)
    echo "OK"
    exit 0
    ;;
esac
echo "KO" >&2
exit 1
