// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Samples.Graph.Client.Options;
using ArmoniK.Samples.Graph.Common;
using ArmoniK.Utils.Pool;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Formatting.Compact;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Samples.Graph.Client;

internal static class Program
{
  /// <summary>Prefix of the line holding the statistics of the generated graph, for scripts</summary>
  private const string StatsPrefix = "GRAPH_STATS ";

  /// <summary>Prefix of the final line holding all the numbers of the run, for scripts</summary>
  private const string ResultPrefix = "GRAPH_RESULT ";

  private static readonly TimeSpan PollingInterval  = TimeSpan.FromSeconds(1);
  private static readonly TimeSpan ProgressInterval = TimeSpan.FromSeconds(5);

  private static readonly TaskStatus[] FinalStatuses =
  {
    TaskStatus.Completed,
    TaskStatus.Error,
    TaskStatus.Cancelled,
    TaskStatus.Timeout,
  };

  private static readonly JsonSerializerOptions JsonOptions = new()
                                                              {
                                                                WriteIndented = false,
                                                              };

  private static async Task<int> Main()
  {
    var builder       = new ConfigurationBuilder().AddEnvironmentVariables();
    var configuration = builder.Build();
    var seriLogger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                              .Enrich.FromLogContext()
                                              .WriteTo.Console(new CompactJsonFormatter())
                                              .CreateBootstrapLogger();

    var logger = new LoggerFactory().AddSerilog(seriLogger)
                                    .CreateLogger("Graph Program");

    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();
    logger.LogInformation("gRPC options : {@GrpcOptions}",
                          options);
    var graphOptions = new GraphOptions();
    configuration.GetSection(GraphOptions.SettingSection)
                 .Bind(graphOptions);
    logger.LogInformation("graph options : {@GraphOptions}",
                          graphOptions);
    using var _ = logger.BeginPropertyScope(("@graphOptions", graphOptions));

    GraphSettings settings;
    bool          reverse;
    try
    {
      if (graphOptions.NTasks < 1)
      {
        throw new FormatException($"NTasks should be at least 1, got {graphOptions.NTasks}");
      }

      if (graphOptions.BatchSize < 1)
      {
        throw new FormatException($"BatchSize should be at least 1, got {graphOptions.BatchSize}");
      }

      reverse = graphOptions.SubmitOrder.Trim()
                            .ToLowerInvariant() switch
                {
                  "reverse" => true,
                  "natural" => false,
                  _         => throw new FormatException($"SubmitOrder should be reverse or natural, got {graphOptions.SubmitOrder}"),
                };

      settings = GraphSettings.Parse(graphOptions.Seed,
                                     graphOptions.GraphTasks,
                                     graphOptions.Levels,
                                     graphOptions.TasksPerLevel,
                                     graphOptions.DependenciesPerTask,
                                     graphOptions.ParentChoice,
                                     graphOptions.SkipLevels,
                                     graphOptions.TaskDuration,
                                     graphOptions.OutputSize);
    }
    catch (FormatException e)
    {
      logger.LogError(e,
                      "Invalid graph options");
      return 2;
    }

    // 1. Generate the whole graph, nested sub-graphs included, and print its statistics before submitting anything
    var generationStart = Stopwatch.GetTimestamp();
    var root = GraphGenerator.GenerateRoot(settings,
                                           graphOptions.NTasks);
    var stats = GraphStats.Compute(settings,
                                   root);
    var generationTime = Stopwatch.GetElapsedTime(generationStart);

    logger.LogInformation("Graph settings : {@GraphSettings}",
                          settings.ToOptions());
    logger.LogInformation("Graph stats : {TotalTasks} tasks, {DependencyEdges} dependency edges, {MaxDependencies} max dependencies per task, {MaxDependents} max tasks waiting on the same output, nesting depth {NestingDepth}, {ExpandingTasks} expanding tasks, {ClientTasks} client tasks over {ClientLevels} levels, critical path {CriticalPathTasks} tasks, theoretical minimum duration {MinimumDurationMs} ms, total task duration {TotalDurationMs} ms, generated in {GenerationTime}",
                          stats.TotalTasks,
                          stats.DependencyEdges,
                          stats.MaxDependencies,
                          stats.MaxDependents,
                          stats.NestingDepth,
                          stats.ExpandingTasks,
                          stats.ClientTasks,
                          stats.ClientLevels,
                          stats.CriticalPathTasks,
                          stats.MinimumDurationMs,
                          stats.TotalDurationMs,
                          generationTime);
    // Plain line on the standard output (not a log) so that scripts can grep it
    Console.Out.WriteLine(StatsPrefix + JsonSerializer.Serialize(stats,
                                                                 JsonOptions));

    if (stats.TotalTasks != graphOptions.NTasks)
    {
      logger.LogError("The generated graph has {TotalTasks} tasks instead of {NTasks}",
                      stats.TotalTasks,
                      graphOptions.NTasks);
      return 1;
    }

    var channelPool = new ObjectPool<ChannelBase>(() => GrpcChannelFactory.CreateChannel(options!));

    // 2. Create the session: the graph settings go in its default task options, so that the sub-tasks submitted by the
    // workers (without task options) see them too
    var sessionId = await channelPool.WithInstanceAsync(async channel =>
                                                        {
                                                          var client = new Sessions.SessionsClient(channel);
                                                          var reply = await client.CreateSessionAsync(new CreateSessionRequest
                                                                                                      {
                                                                                                        DefaultTaskOption = new TaskOptions
                                                                                                                            {
                                                                                                                              MaxDuration =
                                                                                                                                Duration.FromTimeSpan(TimeSpan
                                                                                                                                                        .FromHours(1)),
                                                                                                                              MaxRetries  = graphOptions.MaxRetries,
                                                                                                                              Priority    = 1,
                                                                                                                              PartitionId = graphOptions.Partition,
                                                                                                                              Options =
                                                                                                                              {
                                                                                                                                settings.ToOptions(),
                                                                                                                              },
                                                                                                                            },
                                                                                                        PartitionIds =
                                                                                                        {
                                                                                                          graphOptions.Partition,
                                                                                                        },
                                                                                                      });
                                                          return reply.SessionId;
                                                        },
                                                        CancellationToken.None)
                                     .ConfigureAwait(false);
    logger.LogInformation("Session Id : {SessionId}",
                          sessionId);

    Console.CancelKeyPress += (_,
                               args) =>
                              {
                                args.Cancel = true;
                                using var channel = channelPool.Get();
                                var       client  = new Sessions.SessionsClient(channel);
                                client.CancelSession(new CancelSessionRequest
                                                     {
                                                       SessionId = sessionId,
                                                     });
                                if (graphOptions.PurgeData)
                                {
                                  client.PurgeSession(new PurgeSessionRequest
                                                      {
                                                        SessionId = sessionId,
                                                      });
                                }

                                Environment.Exit(1);
                              };

    // Outputs of the client graph, created first so that every task can reference its dependencies
    var resultIds = new string[root.Tasks.Count];
    foreach (var chunk in root.Tasks.Chunk(graphOptions.BatchSize))
    {
      var created = await channelPool.WithInstanceAsync(async channel => await new Results.ResultsClient(channel).CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                                                                                                {
                                                                                                                                                  SessionId = sessionId,
                                                                                                                                                  Results =
                                                                                                                                                  {
                                                                                                                                                    chunk.Select(task => new CreateResultsMetaDataRequest
                                                                                                                                                                         .Types.ResultCreate
                                                                                                                                                                         {
                                                                                                                                                                           Name = task.Position,
                                                                                                                                                                         }),
                                                                                                                                                  },
                                                                                                                                                }),
                                                      CancellationToken.None)
                                     .ConfigureAwait(false);
      var idByName = created.Results.ToDictionary(result => result.Name,
                                                  result => result.ResultId);
      foreach (var task in chunk)
      {
        resultIds[task.Id] = idByName[task.Position];
      }
    }

    // Reverse: last level first, so that the dependents are always submitted before their dependencies and wait
    var submissionOrder = reverse
                            ? root.Tasks.OrderByDescending(task => task.Level)
                                  .ThenBy(task => task.Index)
                                  .ToList()
                            : root.Tasks.ToList();

    var submissionStart = Stopwatch.GetTimestamp();
    foreach (var chunk in submissionOrder.Chunk(graphOptions.BatchSize))
    {
      await channelPool.WithInstanceAsync(async channel =>
                                          {
                                            var payloads = await new Results.ResultsClient(channel).CreateResultsAsync(new CreateResultsRequest
                                                                                                                       {
                                                                                                                         SessionId = sessionId,
                                                                                                                         Results =
                                                                                                                         {
                                                                                                                           chunk.Select(task => new CreateResultsRequest.Types
                                                                                                                                                .ResultCreate
                                                                                                                                                {
                                                                                                                                                  Name =
                                                                                                                                                    $"payload {task.Position}",
                                                                                                                                                  Data = UnsafeByteOperations
                                                                                                                                                    .UnsafeWrap(TaskPayload
                                                                                                                                                                .Create(root,
                                                                                                                                                                        task,
                                                                                                                                                                        resultIds)
                                                                                                                                                                .Serialize()),
                                                                                                                                                }),
                                                                                                                         },
                                                                                                                       });
                                            var payloadIds = payloads.Results.ToDictionary(result => result.Name,
                                                                                           result => result.ResultId);

                                            var submitted = await new Tasks.TasksClient(channel).SubmitTasksAsync(new SubmitTasksRequest
                                                                                                                  {
                                                                                                                    SessionId = sessionId,
                                                                                                                    TaskCreations =
                                                                                                                    {
                                                                                                                      chunk.Select(task => new SubmitTasksRequest.Types
                                                                                                                                           .TaskCreation
                                                                                                                                           {
                                                                                                                                             PayloadId =
                                                                                                                                               payloadIds
                                                                                                                                                 [$"payload {task.Position}"],
                                                                                                                                             ExpectedOutputKeys =
                                                                                                                                             {
                                                                                                                                               resultIds[task.Id],
                                                                                                                                             },
                                                                                                                                             DataDependencies =
                                                                                                                                             {
                                                                                                                                               task.Dependencies
                                                                                                                                                   .Select(dependency =>
                                                                                                                                                             resultIds
                                                                                                                                                               [dependency]),
                                                                                                                                             },
                                                                                                                                           }),
                                                                                                                    },
                                                                                                                  });

                                            if (logger.IsEnabled(LogLevel.Debug))
                                            {
                                              foreach (var info in submitted.TaskInfos)
                                              {
                                                logger.LogDebug("task created {TaskId}",
                                                                info.TaskId);
                                              }
                                            }
                                          },
                                          CancellationToken.None)
                       .ConfigureAwait(false);
    }

    var submissionTime = Stopwatch.GetElapsedTime(submissionStart);
    logger.LogInformation("{ClientTasks} client tasks submitted in {SubmissionTime}",
                          root.Tasks.Count,
                          submissionTime);

    // 3. Wait until every task is in a final status, by polling the task counts of the session
    var                          outcome      = "timeout";
    var                          lastProgress = Stopwatch.GetTimestamp();
    IReadOnlyDictionary<TaskStatus, int> counts = new Dictionary<TaskStatus, int>();
    while (Stopwatch.GetElapsedTime(submissionStart) < graphOptions.Timeout)
    {
      counts = await channelPool.WithInstanceAsync(async channel => await CountTasksAsync(channel,
                                                                                          sessionId)
                                                                     .ConfigureAwait(false),
                                                   CancellationToken.None)
                                .ConfigureAwait(false);

      var final  = FinalStatuses.Sum(status => counts.GetValueOrDefault(status));
      var active = counts.Where(pair => !FinalStatuses.Contains(pair.Key) && pair.Key != TaskStatus.Retried)
                         .Sum(pair => pair.Value);
      var failed = final - counts.GetValueOrDefault(TaskStatus.Completed);

      if (Stopwatch.GetElapsedTime(lastProgress) >= ProgressInterval)
      {
        lastProgress = Stopwatch.GetTimestamp();
        logger.LogInformation("Progress : {Final}/{NTasks} tasks in a final status ({Completed} completed, {Failed} failed), {Active} active, {Retried} retried, elapsed {Elapsed}",
                              final,
                              graphOptions.NTasks,
                              counts.GetValueOrDefault(TaskStatus.Completed),
                              failed,
                              active,
                              counts.GetValueOrDefault(TaskStatus.Retried),
                              Stopwatch.GetElapsedTime(submissionStart));
      }

      // Sub-tasks are created (in status Creating) before their parent ends, so no active task means nothing more
      // will be created: either every task is final, or a failure prevented sub-graphs from being submitted
      if (active == 0 && (final >= graphOptions.NTasks || failed > 0))
      {
        outcome = "finished";
        break;
      }

      await Task.Delay(PollingInterval)
                .ConfigureAwait(false);
    }

    var wallClock = Stopwatch.GetElapsedTime(submissionStart);

    // 4. Results
    var completed = counts.GetValueOrDefault(TaskStatus.Completed);
    var errors    = counts.GetValueOrDefault(TaskStatus.Error);
    var total = counts.Where(pair => pair.Key != TaskStatus.Retried)
                      .Sum(pair => pair.Value);
    var success = outcome == "finished" && completed == graphOptions.NTasks && total == graphOptions.NTasks;
    if (outcome == "finished" && !success)
    {
      outcome = "failed";
    }

    var throughput = completed / Math.Max(wallClock.TotalSeconds,
                                          1e-3);

    logger.LogInformation("Graph run {Outcome} : {Completed}/{NTasks} tasks completed, {Errors} in error, {Total} tasks counted (retried excluded) in {WallClock} from the first submission, throughput {Throughput} tasks/s, theoretical minimum duration {MinimumDurationMs} ms, counts by status {@Counts}",
                          outcome,
                          completed,
                          graphOptions.NTasks,
                          errors,
                          total,
                          wallClock,
                          throughput,
                          stats.MinimumDurationMs,
                          counts.ToDictionary(pair => pair.Key.ToString(),
                                              pair => pair.Value));

    // 5. Clean the session; a failure here must not hide the result of the run
    try
    {
      await channelPool.WithInstanceAsync(async channel =>
                                          {
                                            var client = new Sessions.SessionsClient(channel);
                                            if (!success)
                                            {
                                              await client.CancelSessionAsync(new CancelSessionRequest
                                                                              {
                                                                                SessionId = sessionId,
                                                                              });
                                            }
                                            else
                                            {
                                              await client.CloseSessionAsync(new CloseSessionRequest
                                                                             {
                                                                               SessionId = sessionId,
                                                                             });
                                            }

                                            if (graphOptions.PurgeData)
                                            {
                                              await client.PurgeSessionAsync(new PurgeSessionRequest
                                                                             {
                                                                               SessionId = sessionId,
                                                                             });
                                            }
                                          },
                                          CancellationToken.None)
                       .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      logger.LogWarning(e,
                        "Could not clean the session {SessionId}",
                        sessionId);
    }

    var result = new Dictionary<string, object>
                 {
                   ["SessionId"]         = sessionId,
                   ["Outcome"]           = outcome,
                   ["Success"]           = success,
                   ["NTasks"]            = graphOptions.NTasks,
                   ["Settings"]          = settings.ToOptions(),
                   ["Stats"]             = stats,
                   ["GenerationMs"]      = generationTime.TotalMilliseconds,
                   ["SubmissionMs"]      = submissionTime.TotalMilliseconds,
                   ["WallClockMs"]       = wallClock.TotalMilliseconds,
                   ["ThroughputTasksPerSecond"] = throughput,
                   ["MinimumDurationMs"] = stats.MinimumDurationMs,
                   ["Completed"]         = completed,
                   ["Errors"]            = errors,
                   ["TotalCounted"]      = total,
                   ["CountsByStatus"] = counts.ToDictionary(pair => pair.Key.ToString(),
                                                            pair => pair.Value),
                 };
    Console.Out.WriteLine(ResultPrefix + JsonSerializer.Serialize(result,
                                                                  JsonOptions));

    return success
             ? 0
             : 1;
  }

  /// <summary>
  ///   Counts the tasks of the session by status
  /// </summary>
  private static async Task<IReadOnlyDictionary<TaskStatus, int>> CountTasksAsync(ChannelBase channel,
                                                                                  string      sessionId)
  {
    var reply = await new Tasks.TasksClient(channel).CountTasksByStatusAsync(new CountTasksByStatusRequest
                                                                             {
                                                                               Filters = new Filters
                                                                                         {
                                                                                           Or =
                                                                                           {
                                                                                             new FiltersAnd
                                                                                             {
                                                                                               And =
                                                                                               {
                                                                                                 new FilterField
                                                                                                 {
                                                                                                   FilterString = new FilterString
                                                                                                                  {
                                                                                                                    Operator = FilterStringOperator.Equal,
                                                                                                                    Value    = sessionId,
                                                                                                                  },
                                                                                                   Field = new TaskField
                                                                                                           {
                                                                                                             TaskSummaryField = new TaskSummaryField
                                                                                                                                {
                                                                                                                                  Field = TaskSummaryEnumField
                                                                                                                                    .SessionId,
                                                                                                                                },
                                                                                                           },
                                                                                                 },
                                                                                               },
                                                                                             },
                                                                                           },
                                                                                         },
                                                                             })
                                                    .ConfigureAwait(false);

    return reply.Status.GroupBy(count => count.Status)
                .ToDictionary(group => group.Key,
                              group => group.Sum(count => count.Count));
  }
}
