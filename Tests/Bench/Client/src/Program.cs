// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Tests.Client;
using ArmoniK.Samples.Bench.Client.Options;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

using Serilog;
using Serilog.Formatting.Compact;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Samples.Bench.Client;

/// <summary>
///   Policy for creating a <see cref="GrpcChannel" /> for the <see cref="ObjectPool" />
/// </summary>
internal sealed class GrpcChannelObjectPolicy : IPooledObjectPolicy<GrpcChannel>
{
  private readonly GrpcClient options_;

  /// <summary>
  ///   Initializes a Policy for <see cref="GrpcChannel" />
  /// </summary>
  /// <param name="options">Options for creating a GrpcChannel</param>
  public GrpcChannelObjectPolicy(GrpcClient options)
    => options_ = options;

  /// <inheritdoc />
  public GrpcChannel Create()
    => GrpcChannelFactory.CreateChannel(options_);

  /// <inheritdoc />
  public bool Return(GrpcChannel obj)
    => true;
}

internal static class Program
{
  private static async Task Main()
  {
    var builder       = new ConfigurationBuilder().AddEnvironmentVariables();
    var configuration = builder.Build();
    var seriLogger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                              .Enrich.FromLogContext()
                                              .WriteTo.Console(new CompactJsonFormatter())
                                              .CreateBootstrapLogger();

    var logger = new LoggerFactory().AddSerilog(seriLogger)
                                    .CreateLogger("Bench Program");

    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();
    logger.LogInformation("gRPC options : {@grpcOptions}",
                          options);
    var benchOptions = new BenchOptions();
    configuration.GetSection(BenchOptions.SettingSection)
                 .Bind(benchOptions);
    logger.LogInformation("bench options : {@benchOptions}",
                          benchOptions);
    using var _ = logger.BeginPropertyScope(("@benchOptions", benchOptions));

    var channel          = GrpcChannelFactory.CreateChannel(options);
    var partitionsClient = new Partitions.PartitionsClient(channel);

    var channelPool = new DefaultObjectPool<GrpcChannel>(new GrpcChannelObjectPolicy(options));

    var partitions = await partitionsClient.ListPartitionsAsync(new ListPartitionsRequest
                                                                {
                                                                  Filter = new ListPartitionsRequest.Types.Filter
                                                                           {
                                                                             Id                   = "",
                                                                             ParentPartitionId    = "",
                                                                             PodMax               = 0,
                                                                             PodReserved          = 0,
                                                                             PreemptionPercentage = 0,
                                                                             Priority             = 0,
                                                                           },
                                                                  Sort = new ListPartitionsRequest.Types.Sort
                                                                         {
                                                                           Direction = ListPartitionsRequest.Types.OrderDirection.Desc,
                                                                           Field     = ListPartitionsRequest.Types.OrderByField.Id,
                                                                         },
                                                                  PageSize = 10,
                                                                  Page     = 0,
                                                                });

    logger.LogInformation("{@partitions}",
                          partitions);

    var submitterClient = new Submitter.SubmitterClient(channel);

    var createSessionRequest = new CreateSessionRequest
                               {
                                 DefaultTaskOption = new TaskOptions
                                                     {
                                                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                                       MaxRetries  = 2,
                                                       Priority    = 1,
                                                       PartitionId = benchOptions.Partition,
                                                       Options =
                                                       {
                                                         {
                                                           "TaskDurationMs", benchOptions.TaskDurationMs.ToString()
                                                         },
                                                         {
                                                           "TaskError", benchOptions.TaskError
                                                         },
                                                         {
                                                           "TaskRpcException", benchOptions.TaskRpcException
                                                         },
                                                         {
                                                           "PayloadSize", benchOptions.PayloadSize.ToString()
                                                         },
                                                         {
                                                           "ResultSize", benchOptions.ResultSize.ToString()
                                                         },
                                                       },
                                                     },
                                 PartitionIds =
                                 {
                                   benchOptions.Partition,
                                 },
                               };

    var start              = Stopwatch.GetTimestamp();
    var createSessionReply = submitterClient.CreateSession(createSessionRequest);
    var sessionCreated     = Stopwatch.GetTimestamp();
    logger.LogInformation("Session Id : {sessionId}",
                          createSessionReply.SessionId);

    var cts       = new CancellationTokenSource();
    var eventTask = Task.CompletedTask;
    if (benchOptions.ShowEvents)
    {
      eventTask = Task.Factory.StartNew(async () =>
                                        {
                                          var eventsClient = new Events.EventsClient(channel);

                                          using var eventsCall = eventsClient.GetEvents(new EventSubscriptionRequest
                                                                                        {
                                                                                          SessionId = createSessionReply.SessionId,
                                                                                        });

                                          while (await eventsCall.ResponseStream.MoveNext(cts.Token)
                                                                 .ConfigureAwait(false))
                                          {
                                            logger.LogInformation("{@eventUpdate}",
                                                                  eventsCall.ResponseStream.Current);
                                          }
                                        },
                                        cts.Token)
                      .Unwrap();
    }

    Console.CancelKeyPress += (_,
                               args) =>
                              {
                                args.Cancel = true;
                                submitterClient.CancelSession(new Session
                                                              {
                                                                Id = createSessionReply.SessionId,
                                                              });
                                Environment.Exit(0);
                              };

    var results = Enumerable.Range(0,
                                   benchOptions.NTasks)
                            .Select(i => Guid.NewGuid() + "root" + i)
                            .ToList();
    var rnd = new Random();

    foreach (var chunk in results.Chunk(benchOptions.BatchSize))
    {
      var createTaskReply = await submitterClient.CreateTasksAsync(createSessionReply.SessionId,
                                                                   null,
                                                                   chunk.Select(resultId =>
                                                                                {
                                                                                  var dataBytes = new byte[benchOptions.PayloadSize * 1024];
                                                                                  rnd.NextBytes(dataBytes);
                                                                                  return new TaskRequest
                                                                                         {
                                                                                           ExpectedOutputKeys =
                                                                                           {
                                                                                             resultId,
                                                                                           },
                                                                                           Payload = UnsafeByteOperations.UnsafeWrap(dataBytes),
                                                                                         };
                                                                                }))
                                                 .ConfigureAwait(false);

      if (logger.IsEnabled(LogLevel.Debug))
      {
        foreach (var status in createTaskReply.CreationStatusList.CreationStatuses)
        {
          logger.LogDebug("task created {taskId}",
                          status.TaskInfo.TaskId);
        }
      }
    }

    var taskCreated = Stopwatch.GetTimestamp();

    foreach (var resultId in results)
    {
      var resultRequest = new ResultRequest
                          {
                            ResultId = resultId,
                            Session  = createSessionReply.SessionId,
                          };

      var availabilityReply = submitterClient.WaitForAvailability(resultRequest);

      switch (availabilityReply.TypeCase)
      {
        case AvailabilityReply.TypeOneofCase.None:
          throw new Exception("Issue with Server !");
        case AvailabilityReply.TypeOneofCase.Ok:
          break;
        case AvailabilityReply.TypeOneofCase.Error:
          throw new Exception($"Task in Error - {availabilityReply.Error.TaskId} : {availabilityReply.Error.Errors}");
        case AvailabilityReply.TypeOneofCase.NotCompletedTask:
          throw new Exception($"Task not completed - result id {resultId}");
        default:
          throw new ArgumentOutOfRangeException(nameof(availabilityReply.TypeCase));
      }
    }

    var resultsAvailable = Stopwatch.GetTimestamp();

    var countRes = 0;

    results.AsParallel()
           .WithDegreeOfParallelism(benchOptions.DegreeOfParallelism)
           .ForAll(resultId =>
                   {
                     for (var i = 0; i < benchOptions.MaxRetries; i++)
                     {
                       var localChannel = channelPool.Get();
                       try
                       {
                         var resultRequest = new ResultRequest
                                             {
                                               ResultId = resultId,
                                               Session  = createSessionReply.SessionId,
                                             };

                         var client = new Submitter.SubmitterClient(localChannel);

                         var result = client.GetResultAsync(resultRequest,
                                                            CancellationToken.None)
                                            .Result;

                         if (result.Length != benchOptions.ResultSize * 1024)
                         {
                           logger.LogInformation("Received length {received}, expected length {expected}",
                                                 result.Length,
                                                 benchOptions.ResultSize * 1024);
                           throw new InvalidOperationException("The result size from the task should have the same size as the one specified");
                         }

                         Interlocked.Increment(ref countRes);
                         // If successful, return
                         return;
                       }
                       catch (RpcException e) when (e.StatusCode == StatusCode.Unavailable)
                       {
                         logger.LogWarning(e,
                                           "Error during result retrieving, retrying to get {resultId}",
                                           resultId);
                       }
                       finally
                       {
                         channelPool.Return(localChannel);
                       }
                     }

                     // in this case, retries are all made so we need to tell that it did not work
                     throw new InvalidOperationException("Too many retries");
                   });

    logger.LogInformation("Results retrieved {number}",
                          countRes);
    if (countRes != results.Count)
    {
      throw new InvalidOperationException("All results were not retrieved");
    }

    var resultsReceived = Stopwatch.GetTimestamp();

    var countAll = await submitterClient.CountTasksAsync(new TaskFilter
                                                         {
                                                           Session = new TaskFilter.Types.IdsRequest
                                                                     {
                                                                       Ids =
                                                                       {
                                                                         createSessionReply.SessionId,
                                                                       },
                                                                     },
                                                         });

    var countFinished = Stopwatch.GetTimestamp();


    var stats = new ExecutionStats
                {
                  ElapsedTime          = TimeSpan.FromTicks((resultsReceived  - start)            / 100),
                  SubmissionTime       = TimeSpan.FromTicks((taskCreated      - sessionCreated)   / 100),
                  ResultRetrievingTime = TimeSpan.FromTicks((resultsReceived  - resultsAvailable) / 100),
                  TasksExecutionTime   = TimeSpan.FromTicks((resultsAvailable - taskCreated)      / 100),
                  CountExecutionTime   = TimeSpan.FromTicks((countFinished    - resultsReceived)  / 100),
                  TotalTasks           = countAll.Values.Sum(count => count.Count),
                  CompletedTasks = countAll.Values.Where(count => count.Status == TaskStatus.Completed)
                                           .Sum(count => count.Count),
                  ErrorTasks = countAll.Values.Where(count => count.Status == TaskStatus.Error)
                                       .Sum(count => count.Count),
                  CancelledTasks = countAll.Values.Where(count => count.Status is TaskStatus.Cancelled or TaskStatus.Cancelling)
                                           .Sum(count => count.Count),
                };
    logger.LogInformation("executions stats {@stats}",
                          stats);

    await channel.LogStatsFromSessionAsync(createSessionReply.SessionId,
                                           logger)
                 .ConfigureAwait(false);

    if (benchOptions.ShowEvents)
    {
      cts.CancelAfter(TimeSpan.FromSeconds(1));
      try
      {
        await eventTask.WaitAsync(CancellationToken.None)
                       .ConfigureAwait(false);
      }
      catch (RpcException e) when (e.StatusCode == StatusCode.Cancelled)
      {
        logger.LogWarning(e,
                          $"{nameof(Events.EventsClient.GetEvents)} interrupted.");
      }
    }
  }
}
