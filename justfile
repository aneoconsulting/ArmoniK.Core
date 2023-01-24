# Enable positional args
set positional-arguments

# Enable bash output
set shell := ["bash", "-exc"]

# Default values for the deployment
tag          := "0.8.0"
local_images := "false"
log_level    := "Information"
queue        := "activemq"
worker       := "htcmock"
object       := "local"
replicas     := "3"
partitions   := "2"

# Export them as terraform environment variables
export TF_VAR_core_tag        := tag
export TF_VAR_use_local_image := local_images
export TF_VAR_serilog_level   := log_level
export TF_VAR_num_replicas    := replicas
export TF_VAR_num_partitions  := partitions

# Sets the queue
export TF_VAR_queue_storage := if queue == "rabbitmq" {
  '{ name = "rabbitmq", image = "rabbitmq:3-management" }'
} else if queue == "rabbitmq091" {
  '{ name = "rabbitmq", image = "rabbitmq:3-management", protocol = "amqp0_9_1" }'
} else {
  '{ name = "activemq", image = "symptoma/activemq:5.16.3" }'
}

# Sets the object storage
export TF_VAR_object_storage := if object == "redis" {
  '{ name = "redis", image = "redis:bullseye" }'
} else {
  '{ name = "local", image = "" }'
}

# Defines worker and environment variables for deployment
defaultWorkerImage := if worker == "stream" {
  "dockerhubaneo/armonik_core_stream_test_worker"
} else if worker == "bench" {
  "dockerhubaneo/armonik_core_bench_test_worker"
} else {
  "dockerhubaneo/armonik_core_htcmock_test_worker"
}
# The path is given relative to the terraform directory
defaultWorkerDockerFilePath := if worker == "stream" {
  "../Tests/Stream/Server/"
} else if worker == "bench" {
  "../Tests/Bench/Server/src/"
} else {
  "../Tests/HtcMock/Server/src/"
}

export TF_VAR_worker_image            := env_var_or_default('WORKER_IMAGE', defaultWorkerImage)
export TF_VAR_worker_docker_file_path := env_var_or_default('WORKER_DOCKER_FILE_PATH', defaultWorkerDockerFilePath)

# List recipes and their usage
@default:
  just --list
  just _usage

_usage:
  #!/usr/bin/env bash
  set -euo pipefail
  cat <<-EOF

  The recipe deploy takes variables
    usage: just tag=<tag> queue=<queue> worker=<worker> object=<object> replicas=<replicas> partitions=<number of partitions> local_images=<bool> deploy
            if any of the variables is not set, its default value is used

      tag: The core tag image to use, defaults to test

      queue: allowed values below
        activemq    :  for activemq (1.0.0 protocol) (default)
        rabbitmq    :  for rabbitmq (1.0.0 protocol)
        rabbitmq091 :  for rabbitmq (0.9.1 protocol)

      worker: allowed values below
        htcmock: for HtcMock V3 (default)
        stream: for Stream worker
        bench:  for Benchmark worker

        It is possible to use a custom worker, this is handled by
        defining either of the following environment variables:

        WORKER_IMAGE:            to pull an already compiled image
        WORKER_DOCKER_FILE_PATH: to compile the image locally

      object: allowed values below
        local: to mount a local volume for object storage (default)
        redis: to use redis for object storage

      replicas: Number of polling agents / worker to be replicated (default = 3)

      partitions: Number of partitions (default = 2)

      local_images: Build local docker images (default = false)

    IMPORTANT: In order to properly destroy the resources created you should call the recipe destroy with the
    same parameters used for deploy
  EOF

# Call terraform init
init:
  terraform -chdir=./terraform init

# Validate deployment
validate:
  terraform -chdir=./terraform validate

# Invoke terraform console
console:
  terraform -chdir=./terraform console

# Deploy ArmoniK Core
deploy: (init)
  terraform -chdir=./terraform apply -auto-approve

# Destroy ArmoniK Core
destroy:
  terraform -chdir=./terraform destroy -auto-approve

# Custom docker generic rule
container *args:
  docker container "$@"

# Custom command to stop the given service
stop serviceName: (container "stop" serviceName)

# Custom command to start the given service
start serviceName: (container "start" serviceName)

# Custom command to restart the given service
restart serviceName: (container "restart" serviceName)

# Custom command to restore a deployment after restarting a given service
restoreDeployment serviceName:  (restart serviceName) (restart "armonik.control.submitter")
  #!/usr/bin/env bash
  set -euo pipefail
  for (( i=0; i<{{replicas}}; i++ )); do
    docker container restart "armonik.compute.pollingagent${i}"
  done

# Remove dangling images
remove-dangling:
  docker images --quiet --filter=dangling=true | xargs --no-run-if-empty docker rmi

# Run health checks
healthChecks:
  #!/usr/bin/env bash
  set -euo pipefail
  for (( i=0; i<{{replicas}}; i++ )); do
    echo -e "\nHealth Checking PollingAggent${i}"
    echo -n "  startup: " && curl -sSL localhost:998${i}/startup
    echo -n "  liveness: " && curl -sSL localhost:998${i}/liveness
    echo -n "  readiness: " && curl -sSL localhost:998${i}/readiness
  done

  echo -e "\nHealth Checking Submitter"
  echo -n "  startup: " && curl -sSL localhost:5011/startup
  echo -n "  liveness: " && curl -sSL localhost:5011/liveness
