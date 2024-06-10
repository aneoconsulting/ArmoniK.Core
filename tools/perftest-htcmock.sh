#!/bin/bash

just buildHtcmockClient

just destroy

just worker=mock replicas=30 partitions=1 build-deploy

SHA=$(git rev-parse --short HEAD)
TAG=$(git describe --tags)
DATE=$(date '+%Y%m%d_%H%M%S')

mkdir -p "logs/$SHA/$DATE"

docker run --net armonik_network --rm \
	-e HtcMock__TotalCalculationTime=00:00:10 \
	-e HtcMock__NTasks=300 \
	-e HtcMock__SubTasksLevels=4 \
	-e HtcMock__EnableFastCompute=true \
	-e GrpcClient__Endpoint=http://armonik.control.submitter:1080 \
	dockerhubaneo/armonik_core_htcmock_test_client:0.0.0.0-local


for i in {0..6}
do
docker run --net armonik_network --rm \
	-e HtcMock__TotalCalculationTime=00:00:10 \
	-e HtcMock__NTasks=10000 \
	-e HtcMock__SubTasksLevels=100 \
	-e HtcMock__EnableFastCompute=true \
	-e GrpcClient__Endpoint=http://armonik.control.submitter:1080 \
	dockerhubaneo/armonik_core_htcmock_test_client:0.0.0.0-local | tee "logs/$SHA/$DATE/mock$i.log"

done

echo
echo
echo ===== Results =====
echo
echo

set -x
grep -h "Throughput for session" "logs/$SHA/$DATE/mock"* | jq -r .sessionThroughput
set +x

AVG=$(grep -h "Throughput for session" "logs/$SHA/$DATE/mock"* | jq -r .sessionThroughput | awk 'BEGIN {sum=0; i=0} {i+=1 ; sum+=$1}; END {printf "%.1f", sum/i}')

echo "| $SHA | $TAG | $AVG |"

