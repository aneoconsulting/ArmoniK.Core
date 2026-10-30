# Graph test

Generates reproducible, configurable task graphs to benchmark the database of ArmoniK, in
particular the resolution of the data dependencies.

```bash
just worker=graph database=postgresql build-deploy
just ntasks=10000 graph_parent_choice=zipf:1.2 runGraph
```

## The budget rule

- Every task has a **budget**: the number of tasks it stands for, itself included.
- A task with budget **1** is a plain task: it sleeps its duration and writes its output.
- A task with budget **B > 1** sleeps its duration, counts for 1 and spreads the **B − 1** remaining
  tasks over a sub-graph that it submits itself. Its output is **delegated** to the last task
  of this sub-graph: that task takes the parent's expected output as its own expected output.
- The client is the root, with budget **NTasks**. It is not a task, so it spreads the whole
  NTasks over the client graph.

So the session has exactly **NTasks** tasks, however the graph is nested.

## Shape of a graph

The client graph and every sub-graph follow the same rules:

- A graph with budget B has `n = min(B, draw(GraphTasks))` tasks. They share the budget at
  random, each getting at least 1 (NTasks for the client graph, B − 1 for a sub-graph).
- The tasks are spread over `min(Levels, n)` levels according to `TasksPerLevel`, with at least
  one task per level.
- In a sub-graph, the last level holds only the **aggregator**, the task that produces the
  parent's output. When `Levels` is 1 the aggregator still gets a level of its own.
- Level 0 tasks have no dependencies. A task of level l ≥ 1 depends on `draw(DependenciesPerTask)`
  distinct outputs of the same graph (at least 1 for the aggregator): from level l − 1, except
  that with probability `SkipLevels` each one comes from a uniformly chosen older level.
  `ParentChoice` picks the outputs inside that level.
- Each task sleeps `draw(TaskDuration)` ms and writes `draw(OutputSize)` KB. The aggregator of
  a sub-graph writes the output size expected for its parent.

## Reproducibility

The settings are stored in the default task options of the session, and sub-tasks are submitted
without options, so every worker sees exactly the client's settings. Each task has a logical
position, `level.index` for each nesting joined by `/` (`2.17/0.3` is task 3 of level 0 of the
sub-graph of task 17 of level 2 of the client graph). A sub-graph is generated only from the
seed, the position and the budget of its parent, with a xoshiro256** generator seeded by a
SplitMix64/FNV-1a hash. Payloads carry only the 64-bit hash of the position (computed
incrementally) and results are named `depth:level.index`, so their size does not grow with the
nesting depth. ArmoniK ids, `System.Random` and `string.GetHashCode` are never used,
so the same seed and options give the same full graph on every machine and on every retry.
Before submitting anything, the client generates the whole graph, nested sub-graphs included,
and prints its statistics.

## Running without just

The test has two images: the worker, `dockerhubaneo/armonik_core_graph_test_worker`, to deploy
as the worker of a partition of any ArmoniK deployment, and the client,
`dockerhubaneo/armonik_core_graph_test_client`, a console application configured only through
environment variables (see [Options](#options)). The client needs the endpoint of the control
plane; the other `GrpcClient__*` variables (`CaCert`, `CertPem`, `KeyPem`,
`AllowUnsafeConnection`, ...) configure TLS as for the other ArmoniK clients.

```bash
docker run --rm \
  -e GrpcClient__Endpoint=<control plane endpoint, e.g. http://host:port> \
  -e GraphOptions__Partition=<partition of the graph worker> \
  -e GraphOptions__NTasks=10000 \
  -e GraphOptions__ParentChoice=zipf:1.2 \
  dockerhubaneo/armonik_core_graph_test_client:<tag>
```

Every option can also be given as a command line argument, which overrides the environment
variable of the same option, e.g. `--GraphOptions:NTasks=10000` or
`--GrpcClient:Endpoint=http://host:port` (arguments after the image name with `docker run`).

The client can also run from the sources with the same variables:

```bash
GrpcClient__Endpoint=<control plane endpoint, e.g. http://host:port> GraphOptions__NTasks=10000 \
  dotnet run --project Tests/Graph/Client/src
```

The worker reads the graph settings from the task options of the session, so it needs no
configuration besides the usual ArmoniK worker settings. Client and worker must be built from
the same version: the graph is generated on both sides and must match.

## Options

Environment variables `GraphOptions__<Name>` of the client (just variables in parentheses):

| Option                         | Default        | Description                                                                 |
|--------------------------------|----------------|-----------------------------------------------------------------------------|
| `NTasks` (`ntasks`)            | `100`          | Total number of tasks of the session                                        |
| `Seed` (`graph_seed`)          | `1`            | Seed of the graph                                                           |
| `GraphTasks` (`graph_tasks`)   | `uniform:5:20` | Number of tasks of a graph (distribution)                                   |
| `Levels` (`graph_levels`)      | `3`            | Maximum number of levels of a graph                                         |
| `TasksPerLevel` (`graph_tasks_per_level`) | `uniform` | `uniform`, `increasing`, `decreasing` or weights such as `1,10,100`     |
| `DependenciesPerTask` (`graph_dependencies`) | `uniform:1:4` | Number of dependencies of a task (distribution)                     |
| `ParentChoice` (`graph_parent_choice`) | `uniform` | `uniform`, `zipf:S` (a few outputs are depended on by many tasks) or `local` |
| `SkipLevels` (`graph_skip_levels`) | `0`        | Probability, between 0 and 1, to take each dependency from an older level (not a distribution) |
| `TaskDuration` (`graph_duration`) | `const:100` | Task duration in ms (distribution)                                          |
| `OutputSize` (`graph_output_size`) | `const:1`  | Output size in KB (distribution, 0 allowed)                                 |
| `Partition` (`partition`)      | `""`           | Partition of the tasks                                                      |
| `MaxRetries` (`graph_max_retries`) | `1`        | Maximum number of retries of a task                                         |
| `BatchSize` (`graph_batch_size`) | `100`        | Number of tasks submitted per request by the client                         |
| `SubmitOrder` (`graph_submit_order`) | `reverse` | `reverse` (last level first, so dependents always wait) or `natural`      |
| `Timeout` (`graph_timeout`)    | `01:00:00`     | Maximum time to wait for the tasks                                          |
| `PurgeData` (`graph_purge_data`) | `true`       | Purge the session at the end                                                |
| `ExitAfterSubmission` (`graph_exit_after_submission`) | `false` | Exit after submitting the client graph, leaving the session running (no result) |

Distributions: `const:V`, `uniform:MIN:MAX`, `normal:MEAN:STDDEV`, `lognormal:MEDIAN:SHAPE`,
`exponential:MEAN` or `poisson:MEAN`, optionally followed by `:min=X` and/or `:max=Y` to clamp the
drawn values (e.g. `lognormal:100:1:max=5000`). Counts are rounded to the nearest integer. For
counts, `uniform` with integer bounds draws every integer between MIN and MAX with the same probability.
An invalid distribution stops the client with exit code 2 before anything is submitted.

The maximum duration of the tasks in ArmoniK (`MaxDuration` of the session, inherited by the
sub-tasks) is derived from the upper bound of `TaskDuration`: the constant, the maximum of
`uniform` or the `max=` bound, times 1.5 plus 1 minute, and at least 5 minutes. An unbounded
`TaskDuration` (`normal`, `lognormal`, `exponential` or `poisson` without `max=`) gets 1 hour and
a warning: a task drawn longer than that ends in `Timeout`.

## Output

The client prints a `GRAPH_STATS {json}` line before submitting anything. It holds the total
number of tasks, dependency edges, the maximum number of dependencies of a task, the maximum
number of tasks waiting on the same output, the nesting depth, the number of expanding tasks,
the critical path (in tasks) and the theoretical minimum duration. At the end it prints a
`GRAPH_RESULT {json}` line with the wall-clock duration from the first submission, the
throughput, the counts by status and the outcome (`finished`, `failed`, `timeout` or
`interrupted`). The exit code is non-zero if a task did not complete or if the session does not
hold exactly NTasks tasks.

The client waits for the outputs of the client graph with the events API. The output of an
expanding task is produced by the aggregator of its sub-graph, so these outputs complete once
the sub-graphs that produce them are done; the client then polls the task counts only until the
last tasks whose output no task consumes are done. Ctrl+C stops the client, then cancels the
session (and purges it if `PurgeData` is set).
