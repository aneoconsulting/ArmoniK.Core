# Enable bash output
set shell := ["bash", "-exc"]

# use powershell on windows
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Default values for the deployment
tag          := "0.0.0.0-local"
local_images := "false"
log_level    := "Information"
queue        := "activemq"
worker       := "htcmock"
object       := "redis"
replicas     := "3"
partitions   := "2"
platform     := ""
push         := "false"
load         := "true"
ingress      := "true"
prometheus   := "true"
grafana      := "true"
seq          := "true"

# Export them as terraform environment variables
export TF_VAR_core_tag          := tag
export TF_VAR_use_local_image   := local_images
export TF_VAR_serilog           := '{ loggin_level = "' + log_level + '" }'
export TF_VAR_num_replicas      := replicas
export TF_VAR_num_partitions    := partitions
export TF_VAR_enable_grafana    := grafana
export TF_VAR_enable_seq        := seq
export TF_VAR_enable_prometheus := prometheus


# Sets the queue
export TF_VAR_queue_storage := if queue == "rabbitmq" {
  if os_family() == "windows" {
    '{ name = "rabbitmq", image = "micdenny/rabbitmq-windows:3.6.12" }'
  } else {
    '{ name = "rabbitmq", image = "rabbitmq:3-management" }'
  }
} else if queue == "rabbitmq091" {
  if os_family() == "windows" {
    '{ name = "rabbitmq", image = "micdenny/rabbitmq-windows:3.6.12", protocol = "amqp0_9_1" }'
  } else {
    '{ name = "rabbitmq", image = "rabbitmq:3-management", protocol = "amqp0_9_1" }'
  }
} else if queue == "artemis" {
  '{ name = "artemis", image = "quay.io/artemiscloud/activemq-artemis-broker:artemis.2.28.0" }'
} else if queue == "activemq" {
  '{ name = "activemq", image = "symptoma/activemq:5.16.3" }'
} else if queue == "pubsub" {
  '{ name = "pubsub", image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest" }'
} else if queue == "sqs" {
  '{ name = "sqs", image = "localstack/localstack:latest" }'
} else {
  '{ name = "none" }'
}

# Sets the object storage
object_storage := if object == "redis" {
  '{ name = "redis", image = "redis:bullseye" '
} else if object == "minio" {
  '{ name = "minio", image = "quay.io/minio/minio" '
} else {
  '{ name = "local", image = "" '
}

export TF_VAR_object_storage := object_storage + if os_family() == "windows" { ', "local_storage_path" : "c:/local_storage" }' } else {  '}' }

# Defines worker and environment variables for deployment
image_worker := if worker == "stream" {
  "dockerhubaneo/armonik_core_stream_test_worker" + ":" + tag
} else if worker == "bench" {
  "dockerhubaneo/armonik_core_bench_test_worker" + ":" + tag
} else if worker == "crashingworker" {
  "dockerhubaneo/armonik_core_crashingworker_test_worker" + ":" + tag
} else {
  "dockerhubaneo/armonik_core_htcmock_test_worker" + ":" + tag
}
# The path is given relative to ArmoniK.Core's root directory
dockerfile_worker := if worker == "stream" {
  "./Tests/Stream/Server/"
} else if worker == "bench" {
  "./Tests/Bench/Server/src/"
} else if worker == "crashingworker" {
  "./Tests/CrashingWorker/Server/src/"
} else {
  "./Tests/HtcMock/Server/src/"
}

export TF_VAR_worker_image            := env_var_or_default('WORKER_IMAGE', image_worker)
export TF_VAR_worker_docker_file_path := env_var_or_default('WORKER_DOCKER_FILE_PATH', dockerfile_worker)

# Armonik docker image names
image_metrics               := env_var_or_default('METRICS_IMAGE', "dockerhubaneo/armonik_control_metrics")
image_partition_metrics     := env_var_or_default('PARTITION_METRICS_IMAGE', "dockerhubaneo/armonik_control_partition_metrics")
image_submitter             := env_var_or_default('SUBMITTER_IMAGE', "dockerhubaneo/armonik_control")
image_polling_agent         := env_var_or_default('POLLING_AGENT_IMAGE', "dockerhubaneo/armonik_pollingagent")
image_client_mock           := env_var_or_default('MOCK_CLIENT_IMAGE', "dockerhubaneo/armonik_core_htcmock_test_client")
image_client_bench          := env_var_or_default('BENCH_CLIENT_IMAGE', "dockerhubaneo/armonik_core_bench_test_client")
image_client_stream         := env_var_or_default('STREAM_CLIENT_IMAGE', "dockerhubaneo/armonik_core_stream_test_client")
image_client_crashingworker := env_var_or_default('CRASHINGWORKER_CLIENT_IMAGE', "dockerhubaneo/armonik_core_crashingworker_test_client")

# Armonik docker images full name (image + tag)
export ARMONIK_METRICS             := image_metrics + ":" + tag
export ARMONIK_PARTITIONMETRICS    := image_partition_metrics + ":" + tag
export ARMONIK_SUBMITTER           := image_submitter + ":" + tag
export ARMONIK_POLLINGAGENT        := image_polling_agent + ":" + tag
export HTCMOCK_CLIENT_IMAGE        := image_client_mock + ":" + tag
export STREAM_CLIENT_IMAGE         := image_client_stream + ":" + tag
export BENCH_CLIENT_IMAGE          := image_client_bench + ":" + tag
export CRASHINGWORKER_CLIENT_IMAGE := image_client_crashingworker + ":" + tag

export TF_VAR_submitter                       := '{ image = "' + image_submitter + '" }'
export TF_VAR_armonik_metrics_image           := image_metrics
export TF_VAR_armonik_partition_metrics_image := image_partition_metrics

export TF_VAR_ingress:= if ingress == "false" {
  '{"configs": {} }'
} else {
  '{}'
}

export TF_VAR_log_driver_image:= if os_family() == "windows" {
  "fluent/fluent-bit:windows-2022-3.1.4"
} else {
  "fluent/fluent-bit:latest"
}

export TF_VAR_mongodb_params:= if os_family() == "windows" {
  '{"windows": "true"}'
} else {
  '{}'
}

export TF_VAR_compute_plane:= if os_family() == "windows" {
  '{ "polling_agent" : { "image" : "' + image_polling_agent + '", "shared_socket" : "c:/cache", "shared_data" : "c:/cache" }, "worker" = {}}'
} else {
  '{ "polling_agent" : { "image" : "' + image_polling_agent + '" }, "worker" = {}}'
}

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

      tag: The core tag image to use, defaults to 0.0.0.0-local

      queue: allowed values below
        activemq    :  for activemq (1.0.0 protocol) (default)
        rabbitmq    :  for rabbitmq (1.0.0 protocol)
        rabbitmq091 :  for rabbitmq (0.9.1 protocol)
        artemis     :  for artemis  (1.0.0 protocol)
        pubsub      :  for Google PubSub
        none        :  for external queue configurations

      worker: allowed values below
        htcmock: for HtcMock V3 (default)
        stream: for Stream worker
        bench:  for Benchmark worker

        It is possible to use a custom worker, this is handled by
        defining either of the following environment variables:

        WORKER_IMAGE:            to pull an already compiled image
        WORKER_DOCKER_FILE_PATH: to compile the image locally

      object: allowed values below
        redis: to use redis for object storage (default)
        minio: to use minio for object storage.
        local: to mount a local volume for object storage
        
      replicas: Number of polling agents / worker to be replicated (default = 3)

      partitions: Number of partitions (default = 2)

      local_images: Let terraform build the docker images locally (default = false)

    IMPORTANT: In order to properly destroy the resources created you should call the recipe destroy with the
    same parameters used for deploy
  EOF

env:
  env

# Call terraform init
init:
  terraform -chdir=terraform init -upgrade

# Validate deployment
validate:
  terraform -chdir=terraform validate

# Invoke terraform console
console:
  terraform -chdir=terraform console

# Plan ArmoniK Core deployment
plan: (init)
  terraform -chdir=terraform plan

# Deploy ArmoniK Core
deploy: (init)
  terraform -chdir=terraform apply -auto-approve

# Deploy target: object standalone
deployTargetObject: (init)
  terraform -chdir=terraform apply -target="module.object_{{object}}" -auto-approve

# Destroy target: queue standalone
destroyTargetObject:
  terraform -chdir=terraform destroy -target="module.object_{{object}}" -auto-approve

# Deploy target: queue standalone
deployTargetQueue: (init)
  #!/usr/bin/env bash
  which_module="module.queue_{{queue}}"
  if [ {{queue}} = "rabbitmq091" ]; then
    which_module="module.queue_rabbitmq"
  fi
  terraform -chdir=terraform apply -target="${which_module}" -auto-approve

# Destroy target: queue standalone
destroyTargetQueue:
  #!/usr/bin/env bash
  which_module="module.queue_{{queue}}"
  if [ {{queue}} = "rabbitmq091" ]; then
    which_module="module.queue_rabbitmq"
  fi
  terraform -chdir=terraform destroy -target="${which_module}" -auto-approve

# Destroy ArmoniK Core
destroy:
  terraform -chdir=terraform destroy -auto-approve

# Custom docker generic rule
container *args:
  docker container "$@"

# Custom command to stop the given service
stop serviceName: (container "stop" serviceName)

# Custom command to start the given service
start serviceName: (container "start" serviceName)

# Custom command to restart the given service
restart serviceName: (container "restart" serviceName)


# Custom command to build a single image
build imageTag dockerFile target="":
  #!/usr/bin/env bash

  target_parameter=""
  if [ "{{target}}" != "" ]; then
    target_parameter="--target {{target}}"
  fi
  platform_parameter=""
  if [ "{{platform}}" != "" ]; then
    platform_parameter="--platform {{platform}}"
  fi
  push_parameter=""
  if [ "{{push}}" == "true" ]; then
    push_parameter="--push --provenance true"
  fi
  load_parameter=""
  if [ "{{load}}" == "true" ]; then
    load_parameter="--load"
  fi

  set -x
  docker buildx build --progress=plain --build-arg VERSION={{tag}} $platform_parameter $load_parameter $push_parameter $target_parameter -t "{{imageTag}}" -f "{{dockerFile}}" ./

# Build Worker
buildWorker: (build TF_VAR_worker_image TF_VAR_worker_docker_file_path + "Dockerfile")

# Build Metrics
buildMetrics: (build ARMONIK_METRICS "./Dockerfile" "metrics")

# Build Partition Metrics
buildPartitionMetrics: (build ARMONIK_PARTITIONMETRICS "./Dockerfile" "partition_metrics")

# Build Submitter
buildSubmitter: (build ARMONIK_SUBMITTER "./Dockerfile" "submitter")

# Build Polling Agent
buildPollingAgent: (build ARMONIK_POLLINGAGENT "./Dockerfile" "polling_agent")

# Build Htcmock Client
buildHtcmockClient: (build HTCMOCK_CLIENT_IMAGE  "./Tests/HtcMock/Client/src/Dockerfile")

# Build Stream Client
buildStreamClient: (build STREAM_CLIENT_IMAGE  "./Tests/Stream/Client/Dockerfile")

# Build Bench Client
buildBenchClient: (build BENCH_CLIENT_IMAGE  "./Tests/Bench/Client/src/Dockerfile")

# Build Crashing Worker Client
buildCrashingWorkerClient: (build CRASHINGWORKER_CLIENT_IMAGE  "./Tests/CrashingWorker/Client/src/Dockerfile")

# Build all images necessary for the deployment
build-core: buildMetrics buildPartitionMetrics buildSubmitter buildPollingAgent

# Build all images necessary for the deployment and the worker
build-all: buildWorker build-core

# Build and Deploy ArmoniK Core; this recipe should only be used with local_images=false
build-deploy: build-all deploy


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
    echo -e "\nHealth Checking PollingAgent${i}"
    echo -n "  startup: " && curl -sSL localhost:998${i}/startup 2>/dev/null || echo refused
    echo -n "  liveness: " && curl -sSL localhost:998${i}/liveness 2>/dev/null || echo refused
    echo -n "  readiness: " && curl -sSL localhost:998${i}/readiness 2>/dev/null || echo refused
  done

  echo -e "\nHealth Checking Submitter"
  echo -n "  startup: " && curl -sSL localhost:5011/startup 2>/dev/null || echo refused
  echo -n "  liveness: " && curl -sSL localhost:5011/liveness 2>/dev/null || echo refused
