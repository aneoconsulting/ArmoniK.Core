# Enable positional args
set positional-arguments

# Enable bash output
set shell := ["bash", "-exc"]

# Default values for the deployment
tag       := "0.8.0"
log_level := "Information"
queue     := "activemq"
worker    := "htcmock"
object    := "local"

# Export them as terraform environment variables
export TF_VAR_core_tag      := tag
export TF_VAR_serilog_level := log_level

# Sets the queue
export TF_VAR_queue_storage := if queue == "rabbitmq" {
  '{ name = "rabbitmq", image = "rabbitmq:3-management", protocol = "amqp1_0" }'
} else if queue == "rabbitmq091" {
  '{ name = "rabbitmq", image = "rabbitmq:3-management", protocol = "amqp0_9_1" }'
} else {
  '{ name = "activemq", image = "symptoma/activemq:5.16.3", protocol = "amqp1_0" }'
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
defaultWorkerDockerFile := if worker == "stream" {
  "./Tests/Stream/Server/Dockerfile"
} else if worker == "bench" {
  "./Tests/Bench/Server/src/Dockerfile"
} else {
  "./Tests/HtcMock/Server/src/Dockerfile"
}

export TF_VAR_worker_image            := env_var_or_default('WORKER_IMAGE', defaultWorkerImage)
export TF_VAR_worker_docker_file_path := env_var_or_default('WORKER_DOCKER_FILE', defaultWorkerDockerFile)

# List recipes and their usage
@default:
  just --list
  just _usage

_usage:
  #!/usr/bin/env bash
  set -euo pipefail
  cat <<-EOF

  The recipe deploy uses three variables
    usage: just tag=<tag> queue=<queue> worker=<worker> object=<object> deploy
            if any of the variables is not set, its default value is used

      tag: The core tag image to use, defaults to test
      queue: allowed values below
        activemqp   :  for activemq (1.0.0 protocol) (default)
        rabbitmq    :  for rabbitmq (1.0.0 protocol)
        rabbitmq091 :  for rabbitmq (0.9.1 protocol)

      worker: allowed values below
        htcmock: for HtcMock V3 (default)
        stream: for Stream worker
        bench:  for Benchmark worker

      object: allowed values below
        local: to mount a local volume for object storage (default)
        redis: to use redis for object storage

  It is possible to use a custom worker, this is handled by
  defining either of the following environment variables:

        WORKER_IMAGE:       to pull an already compiled image
        WORKER_DOCKER_FILE: to compile the image locally
  EOF

# Call terraform init
init:
  terraform -chdir=./terraform init

# Validate deployment
validate:
  terraform -chdir=./terraform validate

# Deploy ArmoniK Core
deploy: (init)
  terraform -chdir=./terraform apply -auto-approve

# Destroy ArmoniK Core
destroy:
  terraform -chdir=./terraform destroy -auto-approve

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
