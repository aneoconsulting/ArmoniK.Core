# ArmoniK.Core — AI Agent Guide

> **Local overrides**: create `.agents-local.md` in the repo root for machine-specific notes (gitignored). Merge its guidance with this file when present.

## Project overview

ArmoniK.Core is a distributed task processing system. It exposes a gRPC API and uses a plugin/adapter architecture so queue, object storage, and database backends are interchangeable at deploy time. Three core services run in Docker containers orchestrated by Terraform:

- **PollingAgent** (`Compute/PollingAgent/`) — pulls tasks from the queue, dispatches them to workers
- **Submitter** (`Control/Submitter/`) — accepts task submissions from clients
- **Metrics** (`Control/Metrics/`) — collects and exposes Prometheus metrics

## Repository layout

```
Common/           Core library: Pollster, gRPC services, storage abstractions, state machines
Adaptors/         Pluggable backends
  AMQP/           Queue — AMQP 1.0
  RabbitMQ/       Queue — RabbitMQ
  Nats/           Queue — NATS JetStream
  SQS/            Queue — AWS SQS
  PubSub/         Queue — Google Pub/Sub
  Memory/         Queue — in-memory (testing)
  MongoDB/        Database — MongoDB
  Redis/          Object storage — Redis
  S3/             Object storage — AWS S3
  LocalStorage/   Object storage — local filesystem
  Embed/          Object storage — embedded
  NullStorage/    Object storage — no-op (testing)
Compute/          PollingAgent service
Control/          Submitter and Metrics services
Tests/            Integration test workers: HtcMock, Bench, Stream, CrashingWorker, Connectivity
terraform/        Terraform modules for Docker-based deployment (no docker-compose)
tools/            Helper scripts: deployment, testing, monitoring, DB ops
.github/          CI/CD workflows
```

Each adapter lives under `Adaptors/<Name>/src/` with a sibling `Adaptors/<Name>/tests/` test project.

## Documentation

Full documentation lives in `.docs/content/` and is published at [armonikcore.readthedocs.io](https://armonikcore.readthedocs.io):

- **Installation & local deployment**: `.docs/content/0.installation/1.local-deployment.md`
- **Running tests**: `.docs/content/0.installation/2.tests.md`, `3.execute-tests.md`
- **Concepts** (components, tasks, auth, adapters, partitions, cache): `.docs/content/1.concepts/`
- **gRPC service APIs** (tasks, sessions, results): `.docs/content/services/`
- **Adapter guide**: `Adaptors/README.md`
- **Coding standards & naming conventions**: `CONTRIBUTING.md#coding-standards`

## Build

```bash
dotnet build                          # build entire solution
dotnet build ArmoniK.Core.sln -c Release   # release build with warnings-as-errors subset

just build-core                       # build core Docker images (PollingAgent, Submitter, Metrics)
just build-all                        # build all images including test workers
just tag=<tag> queue=<q> worker=<w> object=<o> build-deploy   # full build + deploy
just destroy                          # tear down the deployment
```

Justfile variables: `tag` (default `0.0.0.0-local`), `queue` (activemq|rabbitmq|artemis|pubsub|nats|sqs), `worker` (htcmock|stream|bench|crashingworker), `object` (redis|minio|local|embed|null), `replicas`, `partitions`, `log_level`.

## Tests

```bash
dotnet test                           # run all unit tests
dotnet test Common/tests/             # Common library tests only
dotnet test Adaptors/<Name>/tests/    # specific adapter tests
```

Test categories:
- **Unit** (no external services): Common, Memory, MongoDB (uses EphemeralMongo), Redis adapters
- **Partial** (need one service running): AMQP, RabbitMQ, S3, LocalStorage adapters
- **Integration** (full deployment required): HtcMock, Stream, Bench, Connectivity

Integration tests run against a live deployment. Deploy first, then run the client:

```bash
# HtcMock — defaults: 100 tasks, 4 levels, 100ms total computation
just worker=htcmock build-deploy
just runHtcmock
just ntasks=1000 htcmock_levels=1 runHtcmock             # override any variable

# Bench — defaults: 100 tasks, 100ms each, payload 1KB, result 1KB
just worker=bench build-deploy
just runBench
just ntasks=1000 bench_duration_ms=100 runBench          # override any variable
```

Shared variables (apply to both `runHtcmock` and `runBench`): `ntasks` (default `100`) `partition` (default `""` = default partition) `endpoint` (default `http://armonik.control.submitter:1080`) `network` (default `armonik_network`)

Overridable variables for `runHtcmock`: `htcmock_time` `htcmock_datasize` `htcmock_memsize` `htcmock_levels` `htcmock_fast_compute` `htcmock_low_mem` `htcmock_small_output` `htcmock_purge_data` `htcmock_task_rpc_exception` `htcmock_task_error`

Overridable variables for `runBench`: `bench_duration_ms` `bench_payload_size` `bench_result_size` `bench_batch_size` `bench_max_retries` `bench_degree_of_parallelism` `bench_show_events` `bench_purge_data` `bench_download_results` `bench_exit_after_submission` `bench_pause_session` `bench_max_duration` `bench_priority` `bench_task_rpc_exception` `bench_task_error`

Test framework: **NUnit**. Mock library: **Moq**.

## Viewing logs

All containers ship logs to a **Fluent Bit** container named `fluentd` via the Docker `fluentd` log driver. Fluent Bit aggregates them into a single JSON-lines file and also forwards to Seq.

### All logs in one file (preferred)

```bash
# dump to stdout
docker cp fluentd:/armonik-logs.json -

# save to a local file
docker cp fluentd:/armonik-logs.json ./armonik-logs.json

# filter by level with jq
docker cp fluentd:/armonik-logs.json - | jq 'select(.@l == "Error")'

# grep for a keyword
docker cp fluentd:/armonik-logs.json - | grep "TaskId"

# extract a structured field with jq
docker cp fluentd:/armonik-logs.json - | jq '{time: .@t, msg: .@mt, task: .TaskId}'
```

The file format is CLEF (Compact Log Event Format) — one JSON object per line. Common fields: `@t` (timestamp), `@mt` (message template), `@l` (level, absent = Information), `@x` (exception).

### Per-container logs

```bash
docker logs <container-name>          # e.g. armonik.control.submitter
docker logs -f <container-name>       # follow
docker logs <container-name> 2>&1 | grep "ERROR"
```

### Seq UI

Seq is deployed alongside the stack (enabled by default via `seq=true`). Open **http://localhost:9080** in a browser for structured log search and filtering.

### Log verbosity

```bash
just log_level=Verbose build-deploy   # default is "Information"
```

## Code formatting

```bash
just cleanupcode          # formats all staged/modified .cs files (detected from git diff)
just cleanupcode <file>   # formats specific files
sh tools/applyformatpatch.sh   # apply formatting patch downloaded from a failed CI run
```

Internally uses JetBrains `jb cleanupcode --profile="Full Cleanup With Headers"`. CI checks formatting on every PR and uploads a patch artifact when it fails.

Pre-commit hooks (`.pre-commit-config.yaml`) enforce: Terraform format, YAML/XML validity, LF line endings, no large files.

## Architecture patterns

- **Adapter registration**: all adapters implement `IDependencyInjectionBuildable` and register services via `Build()`. Never bypass DI with `new ConcreteType()`.
- **Task lifecycle**: driven by a state machine in `Common/src/StateMachines/`. Do not mutate task status outside the state machine.
- **Logging**: Serilog structured logging. Inject `ILogger<T>`; never use `Console.Write*`.
- **Nullable**: `<Nullable>enable</Nullable>` is set project-wide. All new code must handle nullability correctly.
- **New adapters**: must include a `/tests/` sibling project with integration tests.

## Conventions

- Target framework for new projects: `net10.0`.
- Use NUnit + Moq for tests; match the style of the nearest existing `tests/` directory.
- Inter-service communication uses gRPC.
- Run `just cleanupcode` before committing C# changes.
- PRs go to `main`; follow the PR template at `.github/PULL_REQUEST_TEMPLATE.md`.
- Versioning is semantic and computed by CI — do not manually bump version files.
- Naming conventions and style rules: see `CONTRIBUTING.md#coding-standards`.

## Things to avoid

- Do not add `docker-compose.yml` — deployment is Terraform-managed.
- Do not bypass DI with `new ConcreteType()` for services.
- Do not use `Console.Write*` for logging.
- Do not target `net8.0` in new projects without an explicit reason.
- Do not push breaking changes to public adapter interfaces without a documented migration path.
- Do not run `just destroy` or other destructive Terraform operations without confirming with the user.
