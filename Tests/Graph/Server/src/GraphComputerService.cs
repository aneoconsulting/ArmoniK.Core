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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Samples.Graph.Common;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.Graph.Server;

/// <summary>
///   Worker of the graph test: a task sleeps, then either writes its output (budget 1) or submits its sub-graph and
///   delegates its output to the last task of this sub-graph (budget greater than 1).
/// </summary>
[UsedImplicitly]
public class GraphComputerService : WorkerStreamWrapper
{
  /// <summary>
  ///   Maximum number of results or tasks created by a single request to the agent
  /// </summary>
  private const int BatchSize = 500;

  public GraphComputerService(ILoggerFactory      loggerFactory,
                              ComputePlane        options,
                              GrpcChannelProvider provider)
    : base(loggerFactory,
           options,
           provider)
    => logger_ = loggerFactory.CreateLogger<GraphComputerService>();

  public override async Task<Output> Process(ITaskHandler taskHandler)
  {
    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId));
    using var logFunction = logger_.LogFunction("ProcessGraph",
                                                LogLevel.Information);

    try
    {
      var settings = GraphSettings.FromOptions(taskHandler.TaskOptions.Options);
      var payload  = TaskPayload.Deserialize(taskHandler.Payload);

      logger_.LogDebug("Task {Label} ({PositionHash}) with budget {Budget}, duration {DurationMs} ms and output size {OutputSize} bytes",
                       payload.Label,
                       payload.PositionHash,
                       payload.Budget,
                       payload.DurationMs,
                       payload.OutputSize);

      CheckDependencies(taskHandler,
                        payload);

      await Task.Delay(payload.DurationMs)
                .ConfigureAwait(false);

      var output = taskHandler.ExpectedResults.Single();

      if (payload.Budget <= 1)
      {
        var data = new byte[payload.OutputSize];
        new DeterministicRandom(payload.PositionHash).NextBytes(data);
        await taskHandler.SendResult(output,
                                     data)
                         .ConfigureAwait(false);
      }
      else
      {
        await SubmitSubGraph(taskHandler,
                             settings,
                             payload,
                             output)
          .ConfigureAwait(false);
      }

      return new Output
             {
               Ok = new Empty(),
             };
    }
    catch (RpcException ex)
    {
      logger_.LogError(ex,
                       "Error while computing task");

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message + ex.StackTrace,
                       },
             };
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Error while computing task");

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message + ex.StackTrace,
                       },
             };
    }
  }

  /// <summary>
  ///   Checks that the task received exactly the data dependencies described in its payload, with the expected sizes
  /// </summary>
  private static void CheckDependencies(ITaskHandler taskHandler,
                                        TaskPayload  payload)
  {
    if (taskHandler.DataDependencies.Count != payload.Dependencies.Count)
    {
      throw new
        InvalidOperationException($"Task {payload.Label} expects {payload.Dependencies.Count} data dependencies but received {taskHandler.DataDependencies.Count}");
    }

    foreach (var (id, size) in payload.Dependencies)
    {
      if (!taskHandler.DataDependencies.TryGetValue(id,
                                                    out var data))
      {
        throw new InvalidOperationException($"Task {payload.Label} did not receive its data dependency {id}");
      }

      if (data.Length != size)
      {
        throw new InvalidOperationException($"Data dependency {id} of task {payload.Label} has {data.Length} bytes instead of {size}");
      }
    }
  }

  /// <summary>
  ///   Generates and submits the sub-graph of an expanding task; its last task produces <paramref name="output" />
  /// </summary>
  private async Task SubmitSubGraph(ITaskHandler  taskHandler,
                                    GraphSettings settings,
                                    TaskPayload   payload,
                                    string        output)
  {
    var graph = GraphGenerator.GenerateSubGraph(settings,
                                                payload.PositionHash,
                                                payload.Budget,
                                                payload.OutputSize);
    var depth = payload.Depth + 1;

    logger_.LogInformation("Task {Label} submits a sub-graph of {Count} tasks over {Levels} levels",
                           payload.Label,
                           graph.Tasks.Count,
                           graph.LevelSizes.Count);

    // Outputs of the sub-graph; the aggregator takes over the output of the current task
    var resultIds = new string[graph.Tasks.Count];
    resultIds[^1] = output;
    foreach (var chunk in graph.Tasks.SkipLast(1)
                               .Chunk(BatchSize))
    {
      var created = await taskHandler.CreateResultsMetaDataAsync(chunk.Select(task => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                      {
                                                                                        Name = TaskPayload.Name(depth,
                                                                                                                task.Level,
                                                                                                                task.Index),
                                                                                      }))
                                     .ConfigureAwait(false);
      var idByName = created.Results.ToDictionary(result => result.Name,
                                                  result => result.ResultId);
      foreach (var task in chunk)
      {
        resultIds[task.Id] = idByName[TaskPayload.Name(depth,
                                                       task.Level,
                                                       task.Index)];
      }
    }

    foreach (var chunk in graph.Tasks.Chunk(BatchSize))
    {
      var payloads = await taskHandler.CreateResultsAsync(chunk.Select(task => new CreateResultsRequest.Types.ResultCreate
                                                                               {
                                                                                 Name = $"payload {TaskPayload.Name(depth, task.Level, task.Index)}",
                                                                                 Data = UnsafeByteOperations.UnsafeWrap(TaskPayload.Create(graph,
                                                                                                                                           task,
                                                                                                                                           resultIds,
                                                                                                                                           depth)
                                                                                                                                   .Serialize()),
                                                                               }))
                                      .ConfigureAwait(false);
      var payloadIds = payloads.Results.ToDictionary(result => result.Name,
                                                     result => result.ResultId);

      await taskHandler.SubmitTasksAsync(chunk.Select(task => new SubmitTasksRequest.Types.TaskCreation
                                                              {
                                                                PayloadId = payloadIds[$"payload {TaskPayload.Name(depth, task.Level, task.Index)}"],
                                                                ExpectedOutputKeys =
                                                                {
                                                                  resultIds[task.Id],
                                                                },
                                                                DataDependencies =
                                                                {
                                                                  task.Dependencies.Select(dependency => resultIds[dependency]),
                                                                },
                                                              }),
                                         // Without options, the sub-tasks get the default options of the session
                                         null)
                       .ConfigureAwait(false);
    }
  }
}
