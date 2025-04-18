// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Output = ArmoniK.Core.Common.Storage.Output;
using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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
  public async Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                      TaskOptions       defaultTaskOptions,
                                                      CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateSession)}");

    return new CreateSessionReply
           {
             SessionId = await SessionLifeCycleHelper.CreateSession(sessionTable_,
                                                                    partitionTable_,
                                                                    partitionIds,
                                                                    defaultTaskOptions,
                                                                    submitterOptions_.DefaultPartition,
                                                                    cancellationToken)
                                                     .ConfigureAwait(false),
           };
  }

  /// <inheritdoc />
  [SuppressMessage("Usage",
                   "CA2208:Instantiate argument exceptions correctly",
                   Justification = $"{nameof(ArgumentOutOfRangeException)} is used in nested code")]
  public async Task TryGetResult(ResultRequest                    request,
                                 IServerStreamWriter<ResultReply> responseStream,
                                 CancellationToken                cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(TryGetResult)}");

    var result = await resultTable_.GetResult(request.ResultId,
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
                                                          TaskStatus = taskData.Status.ToGrpcStatus(),
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

        case TaskStatus.Retried: // TODO: If this case is not used, maybe remove it completely?
        case TaskStatus.Unspecified:
        default:
          throw new ArgumentOutOfRangeException(nameof(taskData.Status));
      }
    }

    await foreach (var chunk in objectStorage_.GetValuesAsync(result.OpaqueId,
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
                                                        Status = tuple.Status.ToGrpcStatus(),
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
  [SuppressMessage("Usage",
                   "CA2208:Instantiate argument exceptions correctly",
                   Justification = $"{nameof(ArgumentOutOfRangeException)} is used in nested code")]
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
      var result = await resultTable_.GetResult(request.ResultId,
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
                                 TaskStatus = taskData.Status.ToGrpcStatus(),
                               },
                             },
                           },
                 };
        case ResultStatus.Unspecified:
        default:
          throw new ArgumentOutOfRangeException(nameof(result.Status));
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

    var (id, size) = await objectStorage_.AddOrUpdateAsync(new ObjectData
                                                           {
                                                             ResultId  = key,
                                                             SessionId = sessionId,
                                                           },
                                                           chunks,
                                                           cancellationToken)
                                         .ConfigureAwait(false);

    await resultTable_.SetResult(sessionId,
                                 ownerTaskId,
                                 key,
                                 size,
                                 id,
                                 cancellationToken)
                      .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<ICollection<TaskCreationRequest>> CreateTasks(string                        sessionId,
                                                                  string                        parentTaskId,
                                                                  TaskOptions?                  options,
                                                                  IAsyncEnumerable<TaskRequest> taskRequests,
                                                                  CancellationToken             cancellationToken)
  {
    var sessionData = await sessionTable_.GetSessionAsync(sessionId,
                                                          cancellationToken)
                                         .ConfigureAwait(false);

    options = TaskLifeCycleHelper.ValidateSession(sessionData,
                                                  options,
                                                  sessionId,
                                                  pushQueueStorage_.MaxPriority,
                                                  logger_,
                                                  cancellationToken);

    using var logFunction = logger_.LogFunction(parentTaskId);
    using var activity    = activitySource_.StartActivity($"{nameof(CreateTasks)}");
    using var sessionScope = logger_.BeginPropertyScope(("Session", sessionData.SessionId),
                                                        ("TaskId", parentTaskId),
                                                        ("PartitionId", options.PartitionId));

    var requests           = new List<TaskCreationRequest>();
    var payloadUploadTasks = new List<Task<(byte[] id, long size)>>();

    await foreach (var taskRequest in taskRequests.WithCancellation(cancellationToken)
                                                  .ConfigureAwait(false))
    {
      var taskId = Guid.NewGuid()
                       .ToString();
      var payloadId = Guid.NewGuid()
                          .ToString();

      requests.Add(new TaskCreationRequest(taskId,
                                           payloadId,
                                           options,
                                           taskRequest.ExpectedOutputKeys.ToList(),
                                           taskRequest.DataDependencies.ToList()));
      payloadUploadTasks.Add(objectStorage_.AddOrUpdateAsync(new ObjectData
                                                             {
                                                               ResultId  = payloadId,
                                                               SessionId = sessionId,
                                                             },
                                                             taskRequest.PayloadChunks,
                                                             cancellationToken));
    }

    var payloadSizes = await payloadUploadTasks.WhenAll()
                                               .ConfigureAwait(false);

    var now = DateTime.UtcNow;

    await resultTable_.Create(requests.Zip(payloadSizes,
                                           (request,
                                            r) => new Result(sessionId,
                                                             request.PayloadId,
                                                             "",
                                                             parentTaskId.Equals(sessionId)
                                                               ? ""
                                                               : parentTaskId,
                                                             parentTaskId.Equals(sessionId)
                                                               ? ""
                                                               : parentTaskId,
                                                             parentTaskId,
                                                             ResultStatus.Completed,
                                                             new List<string>(),
                                                             now,
                                                             now,
                                                             r.size,
                                                             r.id,
                                                             false))
                                      .AsICollection(),
                              cancellationToken)
                      .ConfigureAwait(false);

    await TaskLifeCycleHelper.CreateTasks(taskTable_,
                                          resultTable_,
                                          sessionId,
                                          parentTaskId,
                                          requests,
                                          logger_,
                                          cancellationToken)
                             .ConfigureAwait(false);

    return requests;
  }

  /// <inheritdoc />
  public async Task CompleteTaskAsync(TaskData          taskData,
                                      SessionData       sessionData,
                                      bool              resubmit,
                                      Output            output,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CompleteTaskAsync)}");

    await TaskLifeCycleHelper.CompleteTaskAsync(taskTable_,
                                                resultTable_,
                                                objectStorage_,
                                                pushQueueStorage_,
                                                submitterOptions_,
                                                taskData,
                                                sessionData,
                                                resubmit,
                                                output,
                                                logger_,
                                                cancellationToken)
                             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                                         SessionData                      sessionData,
                                         string                           parentTaskId,
                                         CancellationToken                cancellationToken)
    => await TaskLifeCycleHelper.FinalizeTaskCreation(taskTable_,
                                                      resultTable_,
                                                      pushQueueStorage_,
                                                      requests.ToList(),
                                                      sessionData,
                                                      parentTaskId,
                                                      logger_,
                                                      cancellationToken)
                                .ConfigureAwait(false);
}
