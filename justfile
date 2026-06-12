# Enable bash output
set shell := ["bash", "-exc"]

# use powershell on windows
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Default values for the deployment
tag          := "0.0.0.0-local"  # Docker image tag; use "0.0.0.0-local" for locally built images
local_images := "false"          # If true, Terraform builds images locally instead of pulling from registry
log_level    := "Information"    # Serilog log level: Verbose, Debug, Information, Warning, Error, Fatal
queue        := "activemq"       # Queue backend: activemq, rabbitmq, nats, sqs, pubsub, none
worker       := "htcmock"        # Test worker: htcmock, stream, bench, crashingworker
object       := "redis"          # Object storage: redis, minio, gcs, local, embed, null
replicas     := "3"              # Number of PollingAgent+Worker pairs to deploy
partitions   := "2"              # Number of ArmoniK partitions
platform     := ""               # Docker build platform override (e.g. "linux/amd64"); empty = host platform
push         := "false"          # If true, push built images to registry (requires login)
load         := "true"           # If true, load built images into local Docker daemon
ingress      := "true"           # If true, deploy the ingress (load balancer) container
prometheus   := "true"           # If true, deploy Prometheus for metrics collection
grafana      := "true"           # If true, deploy Grafana dashboards
seq          := "true"           # If true, deploy Seq for structured log search (UI at http://localhost:9080)
socket_type  := "unixdomainsocket" # Socket type for PollingAgent↔Worker communication: unixdomainsocket or tcp
cinit        := "true"           # If true, run a one-shot init container to initialize the database before services start; services self-initialize when false

# Shared test parameters
ntasks    := "100"
network   := "armonik_network"
net_arg   := if network != "" { "--net " + network } else { "" }
partition := ""
endpoint  := "http://armonik.control.submitter:1080"

# HtcMock test client parameters (defaults match HtcMock.cs)
htcmock_time               := "00:00:00.100"
htcmock_datasize           := "1"
htcmock_memsize            := "1"
htcmock_levels             := "4"
htcmock_fast_compute       := "true"
htcmock_low_mem            := "true"
htcmock_small_output       := "true"
htcmock_purge_data         := "true"
htcmock_task_rpc_exception := ""
htcmock_task_error         := ""

# Bench test client parameters (defaults match BenchOptions.cs)
bench_duration_ms              := "100"
bench_payload_size             := "1"
bench_result_size              := "1"
bench_batch_size               := "100"
bench_max_retries              := "1"
bench_degree_of_parallelism    := "1"
bench_show_events              := "false"
bench_purge_data               := "true"
bench_download_results         := "true"
bench_exit_after_submission    := "false"
bench_pause_session            := "false"
bench_max_duration             := "01:00:00"
bench_priority                 := "1"
bench_task_rpc_exception       := ""
bench_task_error               := ""

# Export them as terraform environment variables
export TF_VAR_core_tag          := tag
export TF_VAR_use_local_image   := local_images
export TF_VAR_serilog           := '{ loggin_level = "' + log_level + '" }'
export TF_VAR_num_replicas      := replicas
export TF_VAR_num_partitions    := partitions
export TF_VAR_enable_grafana    := grafana
export TF_VAR_enable_seq        := seq
export TF_VAR_enable_prometheus := prometheus
export TF_VAR_socket_type       := socket_type
export TF_VAR_container_init    := cinit


# Sets the queue
export TF_VAR_queue_storage := if queue == "rabbitmq" {
  if os_family() == "windows" {
    '{ name = "rabbitmq", image = "micdenny/rabbitmq-windows:4.1.0" }'
  } else {
    '{ name = "rabbitmq", image = "rabbitmq:4-management" }'
  }
} else if queue == "artemis" {
  '{ name = "artemis", image = "quay.io/artemiscloud/activemq-artemis-broker:artemis.2.28.0" }'
} else if queue == "activemq" {
  '{ name = "activemq", image = "apache/activemq-classic:latest" }'
} else if queue == "pubsub" {
  '{ name = "pubsub", image = "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest" }'
} else if queue == "nats" { 
  if os_family() == "windows" {
    '{ name = "nats", image = "nats:nanoserver-ltsc2022"}'
  } else {
  '{ name = "nats", image = "nats:alpine" }'
  }
} else if queue == "sqs" {
  '{ name = "sqs", image = "softwaremill/elasticmq:latest" }'
} else {
  '{ name = "none" }'
}

# Sets the object storage
object_storage := if object == "redis" {
  '{ name = "redis", image = "redis:bullseye" '
} else if object == "minio" {
  '{ name = "minio", image = "quay.io/minio/minio" '
} else if object == "gcs" {
  '{ name = "gcs", image = "fsouza/fake-gcs-server:latest" '
} else if object == "embed" {
  '{ name = "embed"'
} else if object == "null" {
  '{ name = "null"'
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
image_submitter             := env_var_or_default('SUBMITTER_IMAGE', "dockerhubaneo/armonik_control")
image_polling_agent         := env_var_or_default('POLLING_AGENT_IMAGE', "dockerhubaneo/armonik_pollingagent")
image_client_mock           := env_var_or_default('MOCK_CLIENT_IMAGE', "dockerhubaneo/armonik_core_htcmock_test_client")
image_client_bench          := env_var_or_default('BENCH_CLIENT_IMAGE', "dockerhubaneo/armonik_core_bench_test_client")
image_client_stream         := env_var_or_default('STREAM_CLIENT_IMAGE', "dockerhubaneo/armonik_core_stream_test_client")
image_client_crashingworker := env_var_or_default('CRASHINGWORKER_CLIENT_IMAGE', "dockerhubaneo/armonik_core_crashingworker_test_client")

# Armonik docker images full name (image + tag)
export ARMONIK_METRICS             := image_metrics + ":" + tag
export ARMONIK_SUBMITTER           := image_submitter + ":" + tag
export ARMONIK_POLLINGAGENT        := image_polling_agent + ":" + tag
export HTCMOCK_CLIENT_IMAGE        := image_client_mock + ":" + tag
export STREAM_CLIENT_IMAGE         := image_client_stream + ":" + tag
export BENCH_CLIENT_IMAGE          := image_client_bench + ":" + tag
export CRASHINGWORKER_CLIENT_IMAGE := image_client_crashingworker + ":" + tag

export TF_VAR_submitter                       := '{ image = "' + image_submitter + '" }'
export TF_VAR_armonik_metrics_image           := image_metrics

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

export TF_VAR_windows:= if os_family() == "windows" {
  "true"
} else {
  "false"
}

export TF_VAR_compute_plane:= if os_family() == "windows" {
  '{ "polling_agent" : { "image" : "' + image_polling_agent + '", "shared_socket" : "c:/cache", "shared_data" : "c:/comm" }, "worker" = {}}'
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
        activemq    :  for ActiveMQ (default)
        rabbitmq    :  for RabbitMQ
        artemis     :  for ActiveMQ Artemis
        sqs         :  for AWS SQS (using elasticmq)
        pubsub      :  for Google PubSub
        nats        :  for Nats with JetStream
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
        gcs: to use fake-gcs-server (Google Cloud Storage emulator) for object storage.
        local: to mount a local volume for object storage
        embed: to use the database as an object storage
        
      replicas: Number of polling agents / worker to be replicated (default = 3)

      partitions: Number of partitions (default = 2)

      local_images: Let terraform build the docker images locally (default = false)

      socket_type: Socket type used by agent and worker to communicate (default = unixdomainsocket)

    IMPORTANT: In order to properly destroy the resources created you should call the recipe destroy with the
    same parameters used for deploy
  EOF

# Print all environment variables (useful for debugging Terraform variable values)
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

# Destroy target: object standalone
destroyTargetObject:
  terraform -chdir=terraform destroy -target="module.object_{{object}}" -auto-approve

# Deploy target: queue standalone
deployTargetQueue: (init)
  terraform -chdir=terraform apply -target=local_file.queue_env -auto-approve

# Destroy target: queue standalone
destroyTargetQueue:
  terraform -chdir=terraform destroy -target=local_file.queue_env -auto-approve

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
    push_parameter="--push --provenance true --sbom true"
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

# Run HtcMock test client against a running deployment
# Note: always rebuilds the client image to pick up local changes
# Override any variable: just ntasks=1000 htcmock_levels=1 runHtcmock
runHtcmock: buildHtcmockClient
  docker run {{net_arg}} --rm \
    -e HtcMock__NTasks={{ntasks}} \
    -e HtcMock__TotalCalculationTime={{htcmock_time}} \
    -e HtcMock__DataSize={{htcmock_datasize}} \
    -e HtcMock__MemorySize={{htcmock_memsize}} \
    -e HtcMock__SubTasksLevels={{htcmock_levels}} \
    -e HtcMock__EnableFastCompute={{htcmock_fast_compute}} \
    -e HtcMock__EnableUseLowMem={{htcmock_low_mem}} \
    -e HtcMock__EnableSmallOutput={{htcmock_small_output}} \
    -e HtcMock__Partition={{partition}} \
    -e HtcMock__PurgeData={{htcmock_purge_data}} \
    -e HtcMock__TaskRpcException={{htcmock_task_rpc_exception}} \
    -e HtcMock__TaskError={{htcmock_task_error}} \
    -e GrpcClient__Endpoint={{endpoint}} \
    {{HTCMOCK_CLIENT_IMAGE}}

# Run Bench test client against a running deployment
# Note: always rebuilds the client image to pick up local changes
# Override any variable: just ntasks=1000 bench_payload_size=1000 runBench
runBench: buildBenchClient
  docker run {{net_arg}} --rm \
    -e BenchOptions__NTasks={{ntasks}} \
    -e BenchOptions__TaskDurationMs={{bench_duration_ms}} \
    -e BenchOptions__PayloadSize={{bench_payload_size}} \
    -e BenchOptions__ResultSize={{bench_result_size}} \
    -e BenchOptions__BatchSize={{bench_batch_size}} \
    -e BenchOptions__Partition={{partition}} \
    -e BenchOptions__MaxRetries={{bench_max_retries}} \
    -e BenchOptions__DegreeOfParallelism={{bench_degree_of_parallelism}} \
    -e BenchOptions__ShowEvents={{bench_show_events}} \
    -e BenchOptions__PurgeData={{bench_purge_data}} \
    -e BenchOptions__DownloadResults={{bench_download_results}} \
    -e BenchOptions__ExitAfterSubmission={{bench_exit_after_submission}} \
    -e BenchOptions__PauseSessionDuringSubmission={{bench_pause_session}} \
    -e BenchOptions__MaxDuration={{bench_max_duration}} \
    -e BenchOptions__TaskPriority={{bench_priority}} \
    -e BenchOptions__TaskRpcException={{bench_task_rpc_exception}} \
    -e BenchOptions__TaskError={{bench_task_error}} \
    -e GrpcClient__Endpoint={{endpoint}} \
    {{BENCH_CLIENT_IMAGE}}

# Build Crashing Worker Client
buildCrashingWorkerClient: (build CRASHINGWORKER_CLIENT_IMAGE  "./Tests/CrashingWorker/Client/src/Dockerfile")

# Build all images necessary for the deployment
build-core: buildMetrics buildSubmitter buildPollingAgent

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

# format c# code with jb cleanupcode
[positional-arguments]
cleanupcode *args:
  #! /bin/sh

  SOURCE_DIR="{{source_directory()}}"
  FILES=
  for file in "$@"; do
    file="$(realpath --relative-base="$SOURCE_DIR" "$file")"
    FILES="${FILES:+$FILES;}$file"
  done
  
  cd "$SOURCE_DIR"

  if [ -z "$FILES" ] ; then
    FILES=$( (git diff --name-only --diff-filter=AM --cached && git diff --name-only) | sort -u | grep ".*\.cs" | paste -sd';')
  fi

  if [ -z "$FILES" ] ; then
    echo "No files to cleanup"
    exit 0
  fi

  dotnet build ArmoniK.Core.sln
  jb cleanupcode --profile="Full Cleanup With Headers" --include="$FILES" ArmoniK.Core.sln --no-build
