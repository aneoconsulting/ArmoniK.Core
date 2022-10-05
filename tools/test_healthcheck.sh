#!/bin/bash
set -e

if [ $# -ne 1 ]; then
  echo This script expects only one parameter : the service to restart
  exit 1
fi

SERVICE=$1

curl -fsSL --http2-prior-knowledge localhost:5011/liveness
curl -fsSL --http2-prior-knowledge localhost:5011/startup

curl -fsSL localhost:9980/liveness
curl -fsSL localhost:9980/startup
curl -fsSL localhost:9980/readiness
echo

docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml stop $SERVICE

for i in $(seq 0 2);
do
  curl -sSL --http2-prior-knowledge localhost:5011/liveness || true
  curl -sSL localhost:9980/liveness || true
  echo
  sleep 1
done

docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml restart $SERVICE


for i in $(seq 0 2);
do
  curl -sSL --http2-prior-knowledge localhost:5011/liveness || true
  curl -sSL --http2-prior-knowledge localhost:5011/startup || true
  curl -sSL localhost:9980/liveness || true
  curl -sSL localhost:9980/startup || true
  curl -sSL localhost:9980/readiness || true
  echo
  sleep 1
done

docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml restart armonik.control.submitter
docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml restart armonik.compute.pollingagent0
docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml restart armonik.compute.pollingagent1
docker-compose -f docker-compose/docker-compose.yml -f docker-compose/docker-compose.queue-activemqp.yml restart armonik.compute.pollingagent2

for i in $(seq 0 2);
do
  sleep 1
  curl -sSL --http2-prior-knowledge localhost:5011/liveness || true
  curl -sSL --http2-prior-knowledge localhost:5011/startup || true
  curl -sSL localhost:9980/liveness || true
  curl -sSL localhost:9980/startup || true
  curl -sSL localhost:9980/readiness || true
  echo
done

curl -fsSL --http2-prior-knowledge localhost:5011/liveness
curl -fsSL --http2-prior-knowledge localhost:5011/startup

curl -fsSL localhost:9980/liveness
curl -fsSL localhost:9980/startup
curl -fsSL localhost:9980/readiness
echo
