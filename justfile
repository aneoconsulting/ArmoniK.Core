# Enable bash output
set shell := ["bash", "-exc"]

# use powershell on windows
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Default values for the deployment
tag          := "0.0.0.0-local"  # Docker image tag; use "0.0.0.0-local" for locally built images
local_images := "false"          # If true, Terraform builds images locally instead of pulling from registry
log_level    := "Information"    # Serilog log level: Verbose, Debug, Information, Warning, Error, Fatal
queue        := "activemq"       # Queue backend: activemq, rabbitmq, nats, sqs, pubsub, none
database     := "mongo"          # Database backend: mongo, postgresql
worker       := "htcmock"        # Test worker: htcmock, stream, bench, crashingworker
object       := "redis"          # Object storage: redis, seaweedfs, gcs, local, embed, null
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
authz        := "false"          # If true, authorize requests with the grants of TF_VAR_authorization
keycloak     := "false"          # If true, deploy Keycloak and validate tokens against it
kc_user          := "alice"          # Keycloak user; holds the modelling-team role
kc_password      := "alice"
kc_client_id     := "armonik-client" # other-client mints tokens for another audience
kc_client_secret := "armonik-secret"
kc_token         := ""               # Token presented by runHtcmockTokenRaw

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
export TF_VAR_enable_keycloak   := keycloak

# Permission sets of the access model (none is shipped) and grants for the generated mTLS client certificate (cn:client)
# and the Keycloak role modelling-team: create sessions in the default partition and own them, plus what sharing and
# delegation confer (access model v0.98, section 9). The supervisor certificate (OU=supervisors) follows and stops
# every session, without reading their data
export TF_VAR_authorization := if authz == "true" { '{ enabled = true, permission_sets = [ ' +
  '{ name = "session-create", permissions = [ "session:create", "partition:read" ] }, ' +
  '{ name = "session-view", permissions = [ "session:read", "task:read", "data:read", "data:download", "event:watch" ] }, ' +
  '{ name = "session-own", permissions = [ "session:read", "task:read", "data:read", "data:download", "event:watch", "task:submit", "session:share", "session:delegate", "session:transfer", "session:pause", "session:resume", "session:stop-submission", "session:cancel", "session:close", "session:purge", "session:delete", "task:cancel", "data:declare", "data:provide", "data:delete" ] }, ' +
  '{ name = "session-supervise", permissions = [ "session:read", "session:pause", "session:resume", "session:stop-submission", "session:cancel", "task:read", "task:cancel" ] } ], grants = [ ' +
  '{ id = "client-create", principal = "cn:client", permission_set = "session-create", condition = "request.partition == \"TestPartition0\"" }, ' +
  '{ id = "client-own", principal = "cn:client", permission_set = "session-own", condition = "session.owner == request.caller && grant.principal in session.createdUnder" }, ' +
  '{ id = "team-create", principal = "role:modelling-team", permission_set = "session-create", condition = "request.partition == \"TestPartition0\"" }, ' +
  '{ id = "team-own", principal = "role:modelling-team", permission_set = "session-own", condition = "session.owner == request.caller && grant.principal in session.createdUnder" }, ' +
  '{ id = "share", principal = "*", permission_set = "session-view", condition = "request.caller in session.sharedWith" }, ' +
  '{ id = "delegate", principal = "*", permission_set = "session-own", condition = "request.caller in session.delegatedTo" }, ' +
  '{ id = "supervise", principal = "role:supervisors", permission_set = "session-supervise" } ] }'
} else { '{}' }


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

# Sets the database storage
export TF_VAR_database_storage := if database == "postgresql" {
  '{ name = "postgresql", image = "postgres" }'
} else if database == "mongo" {
  '{ name = "mongo", image = "mongo" }'
} else {
  error("Unknown database: " + database + ". Must be mongo or postgresql")
}

# Sets the object storage
object_storage := if object == "redis" {
  '{ name = "redis", image = "redis:bullseye" '
} else if object == "seaweedfs" {
  '{ name = "seaweedfs", image = "chrislusf/seaweedfs:4.47" '
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
        artemis     :  for ActiveMQ Artemis (abandoned, unsupported - use activemq instead)
        sqs         :  for AWS SQS (using elasticmq)
        pubsub      :  for Google PubSub
        nats        :  for Nats with JetStream
        none        :  for external queue configurations

      database: allowed values below
        mongo      : for MongoDB (default)
        postgresql : for PostgreSQL

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
        seaweedfs: to use SeaweedFS (S3 compatible) for object storage.
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

# Publish a self-contained single-file client binary
# Supported rid values: linux-x64, linux-arm64, win-x64
_publishClient project name rid:
  dotnet publish {{project}} \
    -r {{rid}} \
    --self-contained true \
    -p:PublishSingleFile=true \
    -p:PublishReadyToRun=true \
    -p:Version={{tag}} \
    -c Release \
    -o ./publish/{{name}}-{{rid}}

publishHtcmockClient rid="linux-x64": (_publishClient "Tests/HtcMock/Client/src/ArmoniK.Samples.HtcMock.Client.csproj" "htcmock-client" rid)

# Publish self-contained HtcMock client for all supported platforms
publishHtcmockClientAll: (publishHtcmockClient "linux-x64") (publishHtcmockClient "linux-arm64") (publishHtcmockClient "win-x64")

publishBenchClient rid="linux-x64": (_publishClient "Tests/Bench/Client/src/ArmoniK.Samples.Bench.Client.csproj" "bench-client" rid)

# Publish self-contained Bench client for all supported platforms
publishBenchClientAll: (publishBenchClient "linux-x64") (publishBenchClient "linux-arm64") (publishBenchClient "win-x64")

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

# Run HtcMock through the mTLS ingress with the generated client certificate
# Runs as the host user because terraform writes the client key with 0600 permissions
runHtcmockMtls: buildHtcmockClient
  docker run {{net_arg}} --rm \
    --user "$(id -u):$(id -g)" \
    -v {{justfile_directory()}}/terraform/generated/ingress_mtls:/certs:ro \
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
    -e GrpcClient__Endpoint=https://ingress_mtls:9443 \
    -e GrpcClient__CaCert=/certs/server/ca.crt \
    -e GrpcClient__CertPem=/certs/client/client.crt \
    -e GrpcClient__KeyPem=/certs/client/client.key \
    {{HTCMOCK_CLIENT_IMAGE}}

# Fetch a token from Keycloak for the user (field: access_token or id_token)
kcToken field="access_token":
  #!/usr/bin/env bash
  set -euo pipefail
  # 8180 is the exposed_port of the keycloak module
  curl -sf -X POST "http://localhost:8180/realms/armonik/protocol/openid-connect/token" \
    -d grant_type=password \
    --data-urlencode "client_id={{kc_client_id}}" \
    --data-urlencode "client_secret={{kc_client_secret}}" \
    --data-urlencode "username={{kc_user}}" \
    --data-urlencode "password={{kc_password}}" \
    -d scope=openid \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["{{field}}"])'

# Run HtcMock through the TLS ingress with a token fetched from Keycloak
runHtcmockToken: buildHtcmockClient
  #!/usr/bin/env bash
  set -euo pipefail
  # A nested just starts from the defaults: forward every variable runHtcmockTokenRaw reads
  TOKEN="$(just kc_client_id={{kc_client_id}} kc_client_secret={{kc_client_secret}} kc_user={{kc_user}} kc_password={{kc_password}} kcToken)"
  just tag={{tag}} kc_token="$TOKEN" partition={{partition}} ntasks={{ntasks}} \
       htcmock_time={{htcmock_time}} htcmock_datasize={{htcmock_datasize}} htcmock_memsize={{htcmock_memsize}} \
       htcmock_levels={{htcmock_levels}} htcmock_fast_compute={{htcmock_fast_compute}} \
       htcmock_low_mem={{htcmock_low_mem}} htcmock_small_output={{htcmock_small_output}} \
       htcmock_purge_data={{htcmock_purge_data}} htcmock_task_rpc_exception={{htcmock_task_rpc_exception}} \
       htcmock_task_error={{htcmock_task_error}} runHtcmockTokenRaw

# Run HtcMock through the TLS ingress with the token in kc_token, valid or not
runHtcmockTokenRaw: buildHtcmockClient
  docker run {{net_arg}} --rm \
    --user "$(id -u):$(id -g)" \
    -v {{justfile_directory()}}/terraform/generated/ingress_tls:/certs:ro \
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
    -e GrpcClient__Endpoint=https://ingress_tls:9443 \
    -e GrpcClient__CaCert=/certs/server/ca.crt \
    -e GrpcClient__BearerToken={{kc_token}} \
    {{HTCMOCK_CLIENT_IMAGE}}

# A supervisor follows and stops the sessions of others without reading their data. The client certificate creates
# a session and a result; the supervisor certificate (OU=supervisors, granted session-supervise) reads the session, is
# refused the download with PermissionDenied (it sees the session, so not NotFound) and cancels the session
runSupervisorScenario:
  #!/usr/bin/env bash
  set -euo pipefail
  certs="{{justfile_directory()}}/terraform/generated/ingress_mtls"
  partition="TestPartition0"
  grpcurl="fullstorydev/grpcurl:v1.9.1"

  sessions="armonik.api.grpc.v1.sessions.Sessions"
  results="armonik.api.grpc.v1.results.Results"

  # The ingress only forwards the ArmoniK API, so the descriptors come from the reflection of the Submitter itself
  descriptors="$(mktemp -d)"
  trap 'rm -rf "$descriptors"' EXIT
  for service in "$sessions" "$results"; do
    docker run {{net_arg}} --rm --user "$(id -u):$(id -g)" -v "$descriptors:/out" "$grpcurl" \
      -plaintext -protoset-out "/out/$service.protoset" armonik.control.submitter:1080 describe "$service" >/dev/null
  done

  # Call METHOD with DATA as the client WHO through the mTLS ingress, printing the answer; fails on any status but OK
  call() {
    local who="$1" method="$2" data="$3"
    docker run {{net_arg}} --rm --user "$(id -u):$(id -g)" -v "$certs:/certs:ro" -v "$descriptors:/descriptors:ro" "$grpcurl" \
      -protoset "/descriptors/$sessions.protoset" -protoset "/descriptors/$results.protoset" \
      -cacert /certs/server/ca.crt -cert "/certs/client/$who.crt" -key "/certs/client/$who.key" \
      -d "$data" ingress_mtls:9443 "$method" 2>&1
  }

  # Print the value of the field NAME of the answer on stdin, as grpcurl prints it under its proto name; fails with
  # the answer when it holds no such field
  field() {
    local answer value
    answer="$(cat)"
    value="$(grep -o "\"$1\": *\"[^\"]*\"" <<<"$answer" | head -1 | sed 's/.*: *"\(.*\)"/\1/' || true)"
    if [ -z "$value" ]; then
      echo "FAIL: no $1 in the answer" >&2
      echo "$answer" >&2
      exit 1
    fi
    echo "$value"
  }

  # Call as for `call`, requiring success: print the answer, or fail with it
  expect_success() {
    local output
    if ! output="$(call "$@")"; then
      echo "FAIL: $2 as $1 was refused" >&2
      echo "$output" >&2
      exit 1
    fi
    echo "$output"
  }

  expect_refusal() {
    local expected="$1" output
    shift
    if output="$(call "$@")"; then
      echo "FAIL: $2 as $1 succeeded, expected $expected" >&2
      echo "$output" >&2
      exit 1
    fi
    if ! grep -q "Code: $expected" <<<"$output"; then
      echo "FAIL: $2 as $1 failed, but not with $expected" >&2
      echo "$output" >&2
      exit 1
    fi
    echo "OK: $2 as $1 refused with $expected"
  }

  answer="$(expect_success client "$sessions/CreateSession" \
    "{\"defaultTaskOption\": {\"maxDuration\": \"3600s\", \"maxRetries\": 1, \"priority\": 1, \"partitionId\": \"$partition\"}, \"partitionIds\": [\"$partition\"]}")"
  session="$(field session_id <<<"$answer")"
  echo "Session created by the client: $session"

  answer="$(expect_success client "$results/CreateResultsMetaData" "{\"sessionId\": \"$session\", \"results\": [{\"name\": \"data\"}]}")"
  result="$(field result_id <<<"$answer")"
  echo "Result created by the client: $result"

  expect_success supervisor "$sessions/GetSession" "{\"sessionId\": \"$session\"}" >/dev/null
  echo "OK: GetSession as supervisor"

  expect_refusal PermissionDenied supervisor "$results/DownloadResultData" "{\"sessionId\": \"$session\", \"resultId\": \"$result\"}"

  expect_success supervisor "$sessions/CancelSession" "{\"sessionId\": \"$session\"}" >/dev/null
  echo "OK: CancelSession as supervisor"

  answer="$(expect_success client "$sessions/GetSession" "{\"sessionId\": \"$session\"}")"
  status="$(field status <<<"$answer")"
  if [ "$status" != "SESSION_STATUS_CANCELLED" ]; then
    echo "FAIL: the client sees the session $status, expected SESSION_STATUS_CANCELLED" >&2
    exit 1
  fi
  echo "OK: the client sees the session cancelled"

# Run HtcMock against an ingress that does not verify client certificates, sending the genuine
# mTLS client certificate as the X-Forwarded-Client-Cert header: the ingress must drop it
# Use with tools/expect-refusal.sh on an enforcing deployment
runHtcmockForgedCert ingress="ingress_tls" scheme="https": buildHtcmockClient
  #!/usr/bin/env bash
  set -euo pipefail
  ca=()
  port=9080
  if [ "{{scheme}}" = "https" ]; then
    ca=(-e "GrpcClient__CaCert=/gen/{{ingress}}/server/ca.crt")
    port=9443
  fi
  docker run {{net_arg}} --rm \
    --user "$(id -u):$(id -g)" \
    -v {{justfile_directory()}}/terraform/generated:/gen:ro \
    -e HtcMock__NTasks={{ntasks}} \
    -e HtcMock__TotalCalculationTime={{htcmock_time}} \
    -e HtcMock__SubTasksLevels=1 \
    -e HtcMock__Partition={{partition}} \
    -e GrpcClient__Endpoint={{scheme}}://{{ingress}}:$port \
    "${ca[@]}" \
    -e GrpcClient__ForgedClientCert=/gen/ingress_mtls/client/client.crt \
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

# Run the Bench client through the mTLS ingress with the generated client certificate
runBenchMtls: buildBenchClient
  docker run {{net_arg}} --rm \
    --user "$(id -u):$(id -g)" \
    -v {{justfile_directory()}}/terraform/generated/ingress_mtls:/certs:ro \
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
    -e GrpcClient__Endpoint=https://ingress_mtls:9443 \
    -e GrpcClient__CaCert=/certs/server/ca.crt \
    -e GrpcClient__CertPem=/certs/client/client.crt \
    -e GrpcClient__KeyPem=/certs/client/client.key \
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
