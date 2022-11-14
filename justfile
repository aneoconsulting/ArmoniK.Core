# Enable positional args
set positional-arguments

# Default values for the deployment
tag    := "test"
queue  := "activemqp"
worker := "htcmock"

# Base compose file
export COMPOSE_BASE := "./docker-compose/docker-compose.yml"

# loggin level of deployment
export LOGGING_LEVEL := "Information"

# Sets the queue
export QUEUE := if queue == "rabbitmq" {
  "./docker-compose/docker-compose.queue-rabbitmq.yml"
} else if queue == "rabbitmq091" {
  "./docker-compose/docker-compose.queue-rabbitmq.yml"
} else if queue == "artemis" {
  "./docker-compose/docker-compose.queue-artemis.yml"
} else {
  "./docker-compose/docker-compose.queue-activemqp.yml"
}

# Sets the override to feed docker-compose
export OVERRIDE := if queue == "rabbitmq091" {
  "./docker-compose/docker-compose.override-rabbitmq091.yml"
} else {
  "./docker-compose/docker-compose.override.yml"
}

# Defines worker and enviroment variables for deployment
defaultWorkerImage := if worker == "stream" {
  "dockerhubaneo/armonik_core_stream_test_worker:" + tag
} else if worker == "bench" {
  "dockerhubaneo/armonik_core_bench_test_worker:" + tag
} else {
  "dockerhubaneo/armonik_core_htcmock_test_worker:" + tag
}
defaultWorkerDockerFile := if worker == "stream" {
  "./Tests/Stream/Server/Dockerfile"
} else if worker == "bench" {
  "./Tests/Bench/Server/src/Dockerfile"
} else {
  "./Tests/HtcMock/Server/src/Dockerfile"
}
export ARMONIK_WORKER             := env_var_or_default('WORKER_IMAGE', defaultWorkerImage)
export ARMONIK_WORKER_DOCKER_FILE := env_var_or_default('WORKER_DOCKER_FILE', defaultWorkerDockerFile)
export ARMONIK_METRICS            := "dockerhubaneo/armonik_control_metrics:" + tag
export ARMONIK_PARTITIONMETRICS   := "dockerhubaneo/armonik_control_partition_metrics:" + tag
export ARMONIK_SUBMITTER          := "dockerhubaneo/armonik_control:" + tag
export ARMONIK_POLLINGAGENT       := "dockerhubaneo/armonik_pollingagent:" + tag

# List recipes and their usage
@default:
  just --list
  just _usage
  echo

_usage:
  #!/usr/bin/env bash
  set -euo pipefail
  cat <<-EOF

  The recipe deploy uses three variables
    usage: just tag=<tag> queue=<queue> worker=<worker> deploy
            if any of the variables is not set, its default value is used

      tag: The core tag image to use, defaults to test
      queue: allowed values below
        activemqp   :  for activemq (1.0.0 protocol) (default)
        rabbitmq    :  for rabbitmq (1.0.0 protocol)
        rabbitmq091 :  for rabbitmq (0.9.1 protocol)
        artemis     :  for artemismq (1.0.0 protocol)

      worker: allowed values below
        htcmock: for HtcMock V3 (default)
        stream: for Stream worker
        bench:  for Benchmark worker

  It is possible to use a custom worker, this is handled by
  defining either of the following environment variables:

        WORKER_IMAGE:       to pull an already compiled image
        WORKER_DOCKER_FILE: to compile the image locally
  EOF

# Custom command to build a single image
build $imageTag $dockerFile:
  docker build -t "$imageTag" -f "$dockerFile" ./

# Build all images necessary for the deployment 
build-all: (build ARMONIK_WORKER ARMONIK_WORKER_DOCKER_FILE) (build ARMONIK_METRICS "./Control/Metrics/src/Dockerfile") (build ARMONIK_PARTITIONMETRICS "./Control/PartitionMetrics/src/Dockerfile") (build ARMONIK_SUBMITTER "./Control/Submitter/src/Dockerfile") (build ARMONIK_POLLINGAGENT "./Compute/PollingAgent/src/Dockerfile")

# Insert partitions in database
set-partitions:
  docker run --net armonik-backend --rm rtsp/mongosh mongosh mongodb://database:27017/database \
    --eval 'db.PartitionData.insertMany([{ _id: "TestPartition0", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: "TestPartition1", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null},{ _id: "TestPartition2", ParentPartitionIds: [], PodReserved: 50, PodMax: 100, PreemptionPercentage: 20, Priority: 1, PodConfiguration: null}])'

# Custom compose generic rule
compose *args:
  docker-compose -f "$COMPOSE_BASE" -f "$OVERRIDE" -f "$QUEUE" "$@"

# Call custom docker-compose
compose-invoke serviceName: (compose "rm" "-f" "-s" serviceName)

# Call custom docker-compose up
compose-up: (compose "--compatibility" "up" "-d" "--build" "--force-recreate" "--remove-orphans")

# Deploy ArmoniK Core
deploy: (compose-invoke "database") (compose-invoke "queue") (compose-invoke "object") (compose-invoke "seq") (compose-up) (set-partitions)

# Build and Deploy ArmoniK Core
build-deploy: build-all deploy

# Run health checks
healthChecks:
  #!/usr/bin/env bash
  set -euo pipefail
  for i in {0..2}; do 
    echo -e "\nHealth Checking PollingAggent${i}"
    echo -n "  startup: " && curl -sSL localhost:998${i}/startup
    echo -n "  liveness: " && curl -sSL localhost:998${i}/liveness
    echo -n "  readiness: " && curl -sSL localhost:998${i}/readiness
  done

  echo -e "\nHealth Checking Submitter"
  echo -n "  startup: " && curl -sSL localhost:5011/startup
  echo -n "  liveness: " && curl -sSL localhost:5011/liveness

# Remove dangling images
remove-dangling:
  docker images --quiet --filter=dangling=true | xargs --no-run-if-empty docker rmi

# Remove deployment images
remove:
  docker rmi -f "$ARMONIK_WORKER" "$ARMONIK_METRICS" "$ARMONIK_PARTITIONMETRICS" "$ARMONIK_SUBMITTER" "$ARMONIK_POLLINGAGENT"

# Destroy deployment with docker-compose down
destroy: (compose "down")

# Custom command to restart the given service
restart serviceName: (compose "restart" serviceName)

# Custom command to stop the given service
stop serviceName: (compose "stop" serviceName)

# Custom command to restore a deployment after restarting a given service
restoreDeployment serviceName:  (restart serviceName) (restart "armonik.control.submitter") (restart "armonik.compute.pollingagent0") (restart "armonik.compute.pollingagent1") (restart "armonik.compute.pollingagent2")