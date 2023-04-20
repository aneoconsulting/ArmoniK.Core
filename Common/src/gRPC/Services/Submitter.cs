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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

public class Submitter : ISubmitter
{
  private readonly ActivitySource              activitySource_;
  private readonly ILogger<Submitter>          logger_;
  private readonly IObjectStorage              objectStorage_;
  private readonly IPartitionTable             partitionTable_;
  private readonly IPushQueueStorage           pushQueueStorage_;
  private readonly IResultTable                resultTable_;
  private readonly ISessionTable               sessionTable_;
  private readonly Injection.Options.Submitter submitterOptions_;
  private readonly ITaskTable                  taskTable_;

  [UsedImplicitly]
  public Submitter(IPushQueueStorage           pushQueueStorage,
                   IObjectStorage              objectStorage,
                   ILogger<Submitter>          logger,
                   ISessionTable               sessionTable,
                   ITaskTable                  taskTable,
                   IResultTable                resultTable,
                   IPartitionTable             partitionTable,
                   Injection.Options.Submitter submitterOptions,
                   ActivitySource              activitySource)
  {
    objectStorage_    = objectStorage;
    logger_           = logger;
    sessionTable_     = sessionTable;
    taskTable_        = taskTable;
    resultTable_      = resultTable;
    partitionTable_   = partitionTable;
    submitterOptions_ = submitterOptions;
    activitySource_   = activitySource;
    pushQueueStorage_ = pushQueueStorage;
  }

  /// <inheritdoc />
  public Task<Configuration> GetServiceConfiguration(Empty             request,
                                                     CancellationToken cancellationToken)
    => Task.FromResult(new Configuration
                       {
                         DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                       });

  /// <inheritdoc />
  public async Task CancelSession(string            sessionId,
                                  CancellationToken cancellationToken)
  {
    using var _        = logger_.LogFunction();
    using var activity = activitySource_.StartActivity($"{nameof(CancelSession)}");

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var sessionCancelTask = sessionTable_.CancelSessionAsync(sessionId,
                                                             cancellationToken);

    await taskTable_.CancelSessionAsync(sessionId,
                                        cancellationToken)
                    .ConfigureAwait(false);

    await sessionCancelTask.ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task FinalizeTaskCreation(IEnumerable<Storage.TaskRequest> requests,
                                         int                              priority,
                                         string                           partitionId,
                                         string                           sessionId,
                                         string                           parentTaskId,
                                         CancellationToken                cancellationToken)
  {
    var taskRequests = requests.ToList();

    await ChangeResultOwnership(sessionId,
                                parentTaskId,
                                taskRequests,
                                cancellationToken)
      .ConfigureAwait(false);

    var readyTasks = new List<string>();

    foreach (var request in taskRequests)
    {
      var dependencies = request.DataDependencies.ToList();

      logger_.LogDebug("Process task request {request}",
                       request);

      if (dependencies.Any())
      {
        // If there is dependencies, we need to register the current task as a dependant of its dependencies.
        // This should be done *before* verifying if the dependencies are satisfied in order to avoid missing
        // any result completion.
        // If a result is completed at the same time, either the submitter will see the result has been completed,
        // Or the Agent will remove the result from the remaining dependencies of the task.
        await resultTable_.AddTaskDependency(sessionId,
                                             dependencies,
                                             new List<string>
                                             {
                                               request.Id,
                                             },
                                             cancellationToken)
                          .ConfigureAwait(false);

        // Get the dependencies that are already completed in order to remove them from the remaining dependencies.
        var completedDependencies = await resultTable_.GetResults(sessionId,
                                                                  dependencies,
                                                                  cancellationToken)
                                                      .Where(result => result.Status == ResultStatus.Completed)
                                                      .Select(result => result.Name)
                                                      .ToListAsync(cancellationToken)
                                                      .ConfigureAwait(false);

        // Remove all the dependencies that are already completed from the task.
        // If an Agent has completed one of the dependencies between the GetResults and this remove,
        // One will succeed removing the dependency, the other will silently fail.
        // In either case, the task will be submitted without error by the Agent.
        // If the agent completes the dependencies _before_ the GetResults, both will try to remove it,
        // and both will queue the task.
        // This is benign as it will be handled during dequeue with message deduplication.
        await taskTable_.RemoveRemainingDataDependenciesAsync(new[]
                                                              {
                                                                request.Id,
                                                              },
                                                              completedDependencies,
                                                              cancellationToken)
                        .ConfigureAwait(false);

        // If all dependencies were already completed, the task is ready to be started.
        if (dependencies.Count == completedDependencies.Count)
        {
          readyTasks.Add(request.Id);
        }
      }
      else
      {
        readyTasks.Add(request.Id);
      }
    }

    if (readyTasks.Any())
    {
      await pushQueueStorage_.PushMessagesAsync(readyTasks,
                                                partitionId,
                                                priority,
                                                cancellationToken)
                             .ConfigureAwait(false);
      await taskTable_.FinalizeTaskCreation(readyTasks,
                                            cancellationToken)
                      .ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                      TaskOptions       defaultTaskOptions,
                                                      CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateSession)}");
    if (!partitionIds.Any())
    {
      partitionIds.Add(submitterOptions_.DefaultPartition);
    }

    if (partitionIds.Count == 1 && string.IsNullOrEmpty(partitionIds.Single()))
    {
      partitionIds.Clear();
      partitionIds.Add(submitterOptions_.DefaultPartition);
    }

    if (!await partitionTable_.ArePartitionsExistingAsync(partitionIds,
                                                          cancellationToken)
                              .ConfigureAwait(false))
    {
      throw new PartitionNotFoundException("One of the partitions does not exist");
    }

    if (string.IsNullOrEmpty(defaultTaskOptions.PartitionId))
    {
      defaultTaskOptions.PartitionId = submitterOptions_.DefaultPartition;
    }

    if (!await partitionTable_.ArePartitionsExistingAsync(new[]
                                                          {
                                                            defaultTaskOptions.PartitionId,
                                                          },
                                                          cancellationToken)
                              .ConfigureAwait(false))
    {
      throw new PartitionNotFoundException("The partition in the task options does not exist");
    }

    var sessionId = await sessionTable_.SetSessionDataAsync(partitionIds,
                                                            defaultTaskOptions,
                                                            cancellationToken)
                                       .ConfigureAwait(false);
    return new CreateSessionReply
           {
             SessionId = sessionId,
           };
  }

  /// <inheritdoc />
  public async Task TryGetResult(ResultRequest                    request,
                                 IServerStreamWriter<ResultReply> responseStream,
                                 CancellationToken                cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(TryGetResult)}");

    var result = await resultTable_.GetResult(request.Session,
                                              request.ResultId,
                                              cancellationToken)
                                   .ConfigureAwait(false);

    if (result.Status != ResultStatus.Completed)
    {
      var taskData = await taskTable_.ReadTaskAsync(result.OwnerTaskId,
                                                    cancellationToken)
                                     .ConfigureAwait(false);

      switch (taskData.Status)
      {
        case TaskStatus.Processed:
        case TaskStatus.Completed:
          break;
        case TaskStatus.Error:
        case TaskStatus.Timeout:
        case TaskStatus.Cancelled:
        case TaskStatus.Cancelling:
          await responseStream.WriteAsync(new ResultReply
                                          {
                                            Error = new TaskError
                                                    {
                                                      TaskId = taskData.TaskId,
                                                      Errors =
                                                      {
                                                        new Error
                                                        {
                                                          Detail     = taskData.Output.Error,
                                                          TaskStatus = taskData.Status,
                                                        },
                                                      },
                                                    },
                                          },
                                          CancellationToken.None)
                              .ConfigureAwait(false);
          return;
        case TaskStatus.Creating:
        case TaskStatus.Submitted:
        case TaskStatus.Dispatched:
        case TaskStatus.Processing:
          await responseStream.WriteAsync(new ResultReply
                                          {
                                            NotCompletedTask = taskData.TaskId,
                                          },
                                          CancellationToken.None)
                              .ConfigureAwait(false);
          return;

        case TaskStatus.Unspecified:
        default:
          throw new ArgumentOutOfRangeException();
      }
    }

    await foreach (var chunk in objectStorage_.GetValuesAsync(request.ResultId,
                                                              cancellationToken)
                                              .ConfigureAwait(false))
    {
      await responseStream.WriteAsync(new ResultReply
                                      {
                                        Result = new DataChunk
                                                 {
                                                   Data = UnsafeByteOperations.UnsafeWrap(new ReadOnlyMemory<byte>(chunk)),
                                                 },
                                      },
                                      CancellationToken.None)
                          .ConfigureAwait(false);
    }

    await responseStream.WriteAsync(new ResultReply
                                    {
                                      Result = new DataChunk
                                               {
                                                 DataComplete = true,
                                               },
                                    },
                                    CancellationToken.None)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<Count> WaitForCompletion(WaitRequest       request,
                                             CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(WaitForCompletion)}");

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    Task<IEnumerable<TaskStatusCount>> CountUpdateFunc()
      => taskTable_.CountTasksAsync(request.Filter,
                                    cancellationToken);

    var output              = new Count();
    var countUpdateFunc     = CountUpdateFunc;
    var currentPollingDelay = taskTable_.PollingDelayMin;
    while (true)
    {
      var counts = await countUpdateFunc()
                     .ConfigureAwait(false);
      var notCompleted = 0;
      var error        = false;
      var cancelled    = false;

      // ReSharper disable once PossibleMultipleEnumeration
      foreach (var (status, count) in counts)
      {
        switch (status)
        {
          case TaskStatus.Creating:
            notCompleted += count;
            break;
          case TaskStatus.Submitted:
            notCompleted += count;
            break;
          case TaskStatus.Dispatched:
            notCompleted += count;
            break;
          case TaskStatus.Completed:
            break;
          case TaskStatus.Timeout:
            notCompleted += count;
            break;
          case TaskStatus.Cancelling:
            notCompleted += count;
            cancelled    =  true;
            break;
          case TaskStatus.Cancelled:
            notCompleted += count;
            cancelled    =  true;
            break;
          case TaskStatus.Processing:
            notCompleted += count;
            break;
          case TaskStatus.Error:
            notCompleted += count;
            error        =  true;
            break;
          case TaskStatus.Unspecified:
            notCompleted += count;
            break;
          case TaskStatus.Processed:
            notCompleted += count;
            break;
          default:
            throw new ArmoniKException($"Unknown TaskStatus {status}");
        }
      }

      if (notCompleted == 0 || (request.StopOnFirstTaskError && error) || (request.StopOnFirstTaskCancellation && cancelled))
      {
        // ReSharper disable once PossibleMultipleEnumeration
        output.Values.AddRange(counts.Select(tuple => new StatusCount
                                                      {
                                                        Count  = tuple.Count,
                                                        Status = tuple.Status,
                                                      }));
        logger_.LogDebug("All sub tasks have completed. Returning count={count}",
                         output);
        break;
      }


      await Task.Delay(currentPollingDelay,
                       cancellationToken)
                .ConfigureAwait(false);
      if (2 * currentPollingDelay < taskTable_.PollingDelayMax)
      {
        currentPollingDelay = 2 * currentPollingDelay;
      }
    }

    return output;
  }

  /// <inheritdoc />
  public async Task CompleteTaskAsync(TaskData          taskData,
                                      bool              resubmit,
                                      Output            output,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CompleteTaskAsync)}");

    Storage.Output cOutput = output;

    if (cOutput.Success)
    {
      await taskTable_.SetTaskSuccessAsync(taskData.TaskId,
                                           cancellationToken)
                      .ConfigureAwait(false);

      logger_.LogInformation("Remove input payload of {task}",
                             taskData.TaskId);

      //Discard value is used to remove warnings CS4014 !!
      _ = Task.Factory.StartNew(async () => await objectStorage_.TryDeleteAsync(taskData.TaskId,
                                                                                CancellationToken.None)
                                                                .ConfigureAwait(false),
                                cancellationToken);
    }
    else
    {
      // not done means that another pod put this task in error so we do not need to do it a second time
      // so nothing to do
      if (!await taskTable_.SetTaskErrorAsync(taskData.TaskId,
                                              cOutput.Error,
                                              cancellationToken)
                           .ConfigureAwait(false))
      {
        return;
      }

      // TODO FIXME: nothing will resubmit the task if there is a crash there
      if (resubmit && taskData.RetryOfIds.Count < taskData.Options.MaxRetries)
      {
        logger_.LogWarning("Resubmit {task}",
                           taskData.TaskId);

        var newTaskId = await taskTable_.RetryTask(taskData,
                                                   cancellationToken)
                                        .ConfigureAwait(false);

        await FinalizeTaskCreation(new List<Storage.TaskRequest>
                                   {
                                     new(newTaskId,
                                         taskData.ExpectedOutputIds,
                                         taskData.DataDependencies),
                                   },
                                   taskData.Options.Priority,
                                   taskData.Options.PartitionId,
                                   taskData.SessionId,
                                   taskData.TaskId,
                                   cancellationToken)
          .ConfigureAwait(false);
      }
      else
      {
        await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                        resultTable_,
                                                        taskData.TaskId,
                                                        CancellationToken.None)
                                   .ConfigureAwait(false);
      }
    }
  }

  /// <inheritdoc />
  public async Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                                CancellationToken contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(WaitForAvailabilityAsync)}");

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      contextCancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var currentPollingDelay = taskTable_.PollingDelayMin;
    while (true)
    {
      var result = await resultTable_.GetResult(request.Session,
                                                request.ResultId,
                                                contextCancellationToken)
                                     .ConfigureAwait(false);

      switch (result.Status)
      {
        case ResultStatus.Completed:
          return new AvailabilityReply
                 {
                   Ok = new Empty(),
                 };
        case ResultStatus.Created:
          break;
        case ResultStatus.Aborted:
          var taskData = await taskTable_.ReadTaskAsync(result.OwnerTaskId,
                                                        contextCancellationToken)
                                         .ConfigureAwait(false);

          return new AvailabilityReply
                 {
                   Error = new TaskError
                           {
                             TaskId = taskData.TaskId,
                             Errors =
                             {
                               new Error
                               {
                                 Detail     = taskData.Output.Error,
                                 TaskStatus = taskData.Status,
                               },
                             },
                           },
                 };
        case ResultStatus.Unspecified:
        default:
          throw new ArgumentOutOfRangeException();
      }

      await Task.Delay(currentPollingDelay,
                       contextCancellationToken)
                .ConfigureAwait(false);
      if (2 * currentPollingDelay < taskTable_.PollingDelayMax)
      {
        currentPollingDelay = 2 * currentPollingDelay;
      }
    }
  }

  /// <inheritdoc />
  public async Task SetResult(string                                 sessionId,
                              string                                 ownerTaskId,
                              string                                 key,
                              IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                              CancellationToken                      cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");

    await objectStorage_.AddOrUpdateAsync(key,
                                          chunks,
                                          cancellationToken)
                        .ConfigureAwait(false);

    await resultTable_.SetResult(sessionId,
                                 ownerTaskId,
                                 key,
                                 cancellationToken)
                      .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Storage.TaskRequest> requests, int priority, string partitionId)> CreateTasks(string                        sessionId,
                                                                                                               string                        parentTaskId,
                                                                                                               TaskOptions                   options,
                                                                                                               IAsyncEnumerable<TaskRequest> taskRequests,
                                                                                                               CancellationToken             cancellationToken)
  {
    var sessionData = await sessionTable_.GetSessionAsync(sessionId,
                                                          cancellationToken)
                                         .ConfigureAwait(false);
    options = options != null
                ? Storage.TaskOptions.Merge(options,
                                            sessionData.Options)
                : sessionData.Options;

    using var logFunction = logger_.LogFunction(parentTaskId);
    using var activity    = activitySource_.StartActivity($"{nameof(CreateTasks)}");
    using var sessionScope = logger_.BeginPropertyScope(("Session", sessionData.SessionId),
                                                        ("TaskId", parentTaskId),
                                                        ("PartitionId", options.PartitionId));

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var availablePartitionIds = sessionData.PartitionIds ?? Array.Empty<string>();
    if (!availablePartitionIds.Contains(options.PartitionId))
    {
      throw new InvalidOperationException($"The session {sessionData.SessionId} is assigned to the partitions " +
                                          $"[{string.Join(", ", availablePartitionIds)}], but TaskRequest is assigned to partition {options.PartitionId}");
    }

    if (options.Priority >= pushQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new Status(StatusCode.InvalidArgument,
                                                  $"Max priority is {pushQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }


    var requests           = new List<Storage.TaskRequest>();
    var payloadUploadTasks = new List<Task>();

    await foreach (var taskRequest in taskRequests.WithCancellation(cancellationToken)
                                                  .ConfigureAwait(false))
    {
      var taskId = Guid.NewGuid()
                       .ToString();
      requests.Add(new Storage.TaskRequest(taskId,
                                           taskRequest.ExpectedOutputKeys,
                                           taskRequest.DataDependencies));
      payloadUploadTasks.Add(objectStorage_.AddOrUpdateAsync(taskId,
                                                             taskRequest.PayloadChunks,
                                                             cancellationToken));
    }

    var parentTaskIds = new List<string>();

    if (!parentTaskId.Equals(sessionData.SessionId))
    {
      var res = await taskTable_.GetParentTaskIds(parentTaskId,
                                                  cancellationToken)
                                .ConfigureAwait(false);
      parentTaskIds.AddRange(res);
    }

    parentTaskIds.Add(parentTaskId);

    await payloadUploadTasks.WhenAll()
                            .ConfigureAwait(false);

    await taskTable_.CreateTasks(requests.Select(request => new TaskData(sessionData.SessionId,
                                                                         request.Id,
                                                                         "",
                                                                         "",
                                                                         request.Id,
                                                                         parentTaskIds,
                                                                         request.DataDependencies.ToList(),
                                                                         request.ExpectedOutputKeys.ToList(),
                                                                         Array.Empty<string>(),
                                                                         TaskStatus.Creating,
                                                                         options,
                                                                         new Storage.Output(false,
                                                                                            ""))),
                                 cancellationToken)
                    .ConfigureAwait(false);

    return (requests, options.Priority, options.PartitionId);
  }

  private async Task ChangeResultOwnership(string                           session,
                                           string                           parentTaskId,
                                           IEnumerable<Storage.TaskRequest> requests,
                                           CancellationToken                cancellationToken = default)
  {
    using var _        = logger_.LogFunction($"{session}.{parentTaskId}");
    using var activity = activitySource_.StartActivity($"{nameof(ChangeResultOwnership)}");
    activity?.AddTag("sessionId",
                     session);
    activity?.AddTag("parentTaskId",
                     parentTaskId);

    var parentExpectedOutputKeys = new List<string>();

    if (!parentTaskId.Equals(session))
    {
      parentExpectedOutputKeys.AddRange(await taskTable_.GetTaskExpectedOutputKeys(parentTaskId,
                                                                                   cancellationToken)
                                                        .ConfigureAwait(false));
    }

    var taskDataModels = requests.Select(request =>
                                         {
                                           var intersect = parentExpectedOutputKeys.Intersect(request.ExpectedOutputKeys)
                                                                                   .ToList();

                                           var resultModel = request.ExpectedOutputKeys.Except(intersect)
                                                                    .Select(key => new Result(session,
                                                                                              key,
                                                                                              request.Id,
                                                                                              ResultStatus.Created,
                                                                                              new List<string>(),
                                                                                              DateTime.UtcNow,
                                                                                              Array.Empty<byte>()));

                                           return (Result: resultModel, Req: new IResultTable.ChangeResultOwnershipRequest(intersect,
                                                                                                                           request.Id));
                                         });

    await resultTable_.ChangeResultOwnership(session,
                                             parentTaskId,
                                             taskDataModels.Select(tuple => tuple.Req),
                                             cancellationToken)
                      .ConfigureAwait(false);

    await resultTable_.Create(taskDataModels.SelectMany(task => task.Result),
                              cancellationToken)
                      .ConfigureAwait(false);
  }
}
