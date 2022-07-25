// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.Worker.Options;
using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

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
  private readonly ActivitySource        activitySource_;
  private readonly IQueueStorage         lockedQueueStorage_;
  private readonly ILogger<Submitter>    logger_;
  private readonly IObjectStorageFactory objectStorageFactory_;
  private readonly IResultTable          resultTable_;


  private readonly ISessionTable sessionTable_;
  private readonly ITaskTable    taskTable_;

  [UsedImplicitly]
  public Submitter(IQueueStorage         lockedQueueStorage,
                   IObjectStorageFactory objectStorageFactory,
                   ILogger<Submitter>    logger,
                   ISessionTable         sessionTable,
                   ITaskTable            taskTable,
                   IResultTable          resultTable,
                   ActivitySource        activitySource)
  {
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    sessionTable_         = sessionTable;
    taskTable_            = taskTable;
    resultTable_          = resultTable;
    activitySource_       = activitySource;
    lockedQueueStorage_   = lockedQueueStorage;
  }

  /// <inheritdoc />
  public Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
    => taskTable_.StartTask(taskId,
                            cancellationToken);

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
  public async Task CancelTasks(TaskFilter        request,
                                CancellationToken cancellationToken)
  {
    using var _        = logger_.LogFunction();
    using var activity = activitySource_.StartActivity($"{nameof(CancelTasks)}");

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    await taskTable_.CancelTasks(request,
                                 cancellationToken)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Storage.TaskRequest> requests, int priority)> CreateTasks(string                        sessionId,
                                                                                           string                        parentTaskId,
                                                                                           TaskOptions                   options,
                                                                                           IAsyncEnumerable<TaskRequest> taskRequests,
                                                                                           CancellationToken             cancellationToken)
  {
    using var logFunction = logger_.LogFunction(parentTaskId);
    using var activity    = activitySource_.StartActivity($"{nameof(CreateTasks)}");
    using var sessionScope = logger_.BeginPropertyScope(("Session", sessionId),
                                                        ("TaskId", parentTaskId));

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    options ??= await sessionTable_.GetDefaultTaskOptionAsync(sessionId,
                                                              cancellationToken)
                                   .ConfigureAwait(false);

    if (options.Priority >= lockedQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new Status(StatusCode.InvalidArgument,
                                                  $"Max priority is {lockedQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }


    var requests           = new List<Storage.TaskRequest>();
    var payloadUploadTasks = new List<Task>();

    await foreach (var taskRequest in taskRequests.WithCancellation(cancellationToken)
                                                  .ConfigureAwait(false))
    {
      requests.Add(new Storage.TaskRequest(taskRequest.Id,
                                           taskRequest.ExpectedOutputKeys,
                                           taskRequest.DataDependencies));
      payloadUploadTasks.Add(PayloadStorage(sessionId)
                               .AddOrUpdateAsync(taskRequest.Id,
                                                 taskRequest.PayloadChunks,
                                                 cancellationToken));
    }

    var parentTaskIds = new List<string>();

    if (!parentTaskId.Equals(sessionId))
    {
      var res = await taskTable_.GetParentTaskIds(parentTaskId,
                                                  cancellationToken)
                                .ConfigureAwait(false);
      parentTaskIds.AddRange(res);
    }

    parentTaskIds.Add(parentTaskId);

    await payloadUploadTasks.WhenAll()
                            .ConfigureAwait(false);

    await taskTable_.CreateTasks(requests.Select(request => new TaskData(sessionId,
                                                                         request.Id,
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

    return (requests, options.Priority);
  }

  /// <inheritdoc />
  public async Task FinalizeTaskCreation(IEnumerable<Storage.TaskRequest> requests,
                                         int                              priority,
                                         string                           sessionId,
                                         string                           parentTaskId,
                                         CancellationToken                cancellationToken)
  {
    var taskIds = requests.Select(request => request.Id);

    await ChangeResultOwnership(sessionId,
                                parentTaskId,
                                requests,
                                cancellationToken);

    await lockedQueueStorage_.EnqueueMessagesAsync(taskIds,
                                                   priority,
                                                   cancellationToken)
                             .ConfigureAwait(false);

    await taskTable_.FinalizeTaskCreation(taskIds,
                                          cancellationToken)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<Count> CountTasks(TaskFilter        request,
                                      CancellationToken cancellationToken)

  {
    using var activity = activitySource_.StartActivity($"{nameof(CountTasks)}");

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var count = await taskTable_.CountTasksAsync(request,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    return new Count
           {
             Values =
             {
               count.Select(tuple => new StatusCount
                                     {
                                       Status = tuple.Status,
                                       Count  = tuple.Count,
                                     }),
             },
           };
  }

  /// <inheritdoc />
  public async Task<CreateSessionReply> CreateSession(string              sessionId,
                                                      IEnumerable<string> partitionIds,
                                                      TaskOptions         defaultTaskOptions,
                                                      CancellationToken   cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateSession)}");
    try
    {
      await sessionTable_.SetSessionDataAsync(sessionId,
                                              partitionIds,
                                              defaultTaskOptions,
                                              cancellationToken)
                         .ConfigureAwait(false);
      return new CreateSessionReply
             {
               Ok = new Empty(),
             };
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Error while creating Session");
      return new CreateSessionReply
             {
               Error = e.ToString(),
             };
    }
  }

  /// <inheritdoc />
  public async Task TryGetResult(ResultRequest                    request,
                                 IServerStreamWriter<ResultReply> responseStream,
                                 CancellationToken                cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(TryGetResult)}");
    var       storage  = ResultStorage(request.Session);

    var result = await resultTable_.GetResult(request.Session,
                                              request.Key,
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
        case TaskStatus.Failed:
        case TaskStatus.Timeout:
        case TaskStatus.Canceled:
        case TaskStatus.Canceling:
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

    await foreach (var chunk in storage.GetValuesAsync(request.Key,
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
          case TaskStatus.Failed:
            notCompleted += count;
            error        =  true;
            break;
          case TaskStatus.Timeout:
            notCompleted += count;
            break;
          case TaskStatus.Canceling:
            notCompleted += count;
            cancelled    =  true;
            break;
          case TaskStatus.Canceled:
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

      if (notCompleted == 0 || request.StopOnFirstTaskError && error || request.StopOnFirstTaskCancellation && cancelled)
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
  public async Task UpdateTaskStatusAsync(string            id,
                                          TaskStatus        status,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(UpdateTaskStatusAsync)}");
    await taskTable_.UpdateTaskStatusAsync(id,
                                           status,
                                           cancellationToken)
                    .ConfigureAwait(false);
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
                                   taskData.SessionId,
                                   taskData.TaskId,
                                   cancellationToken)
          .ConfigureAwait(false);
      }
      else
      {
        await resultTable_.AbortTaskResults(taskData.SessionId,
                                            taskData.TaskId,
                                            cancellationToken)
                          .ConfigureAwait(false);
      }
    }
  }

  /// <inheritdoc />
  public async Task<Output> TryGetTaskOutputAsync(ResultRequest     request,
                                                  CancellationToken contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(TryGetTaskOutputAsync)}");
    var output = await taskTable_.GetTaskOutput(request.Key,
                                                contextCancellationToken)
                                 .ConfigureAwait(false);
    return new Output(output);
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
                                                request.Key,
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
  public async Task<GetTaskStatusReply> GetTaskStatusAsync(GetTaskStatusRequest request,
                                                           CancellationToken    contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskStatusAsync)}");
    return new GetTaskStatusReply
           {
             IdStatuses =
             {
               await taskTable_.GetTaskStatus(request.TaskIds.ToList(),
                                              contextCancellationToken)
                               .ConfigureAwait(false),
             },
           };
  }

  /// <inheritdoc />
  public async Task<GetResultStatusReply> GetResultStatusAsync(GetResultStatusRequest request,
                                                               CancellationToken      contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResultStatusAsync)}");
    return new GetResultStatusReply
           {
             IdStatuses =
             {
               await resultTable_.GetResultStatus(request.ResultIds.ToList(),
                                                  request.SessionId,
                                                  contextCancellationToken)
                                 .ConfigureAwait(false),
             },
           };
  }

  /// <inheritdoc />
  public async Task<TaskIdList> ListTasksAsync(TaskFilter        request,
                                               CancellationToken contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       idList   = new TaskIdList();
    idList.TaskIds.AddRange(await taskTable_.ListTasksAsync(request,
                                                            contextCancellationToken)
                                            .ToListAsync(contextCancellationToken)
                                            .ConfigureAwait(false));
    return idList;
  }

  /// <inheritdoc />
  public async Task<SessionIdList> ListSessionsAsync(SessionFilter     request,
                                                     CancellationToken contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       idList   = new SessionIdList();
    idList.SessionIds.AddRange(await sessionTable_.ListSessionsAsync(request,
                                                                     contextCancellationToken)
                                                  .ToListAsync(contextCancellationToken)
                                                  .ConfigureAwait(false));
    return idList;
  }

  private IObjectStorage ResultStorage(string session)
    => objectStorageFactory_.CreateResultStorage(session);

  private IObjectStorage PayloadStorage(string session)
    => objectStorageFactory_.CreatePayloadStorage(session);


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

  public async Task SetResult(string                                 sessionId,
                              string                                 ownerTaskId,
                              string                                 key,
                              IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                              CancellationToken                      cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");
    var       storage  = ResultStorage(sessionId);

    await storage.AddOrUpdateAsync(key,
                                   chunks,
                                   cancellationToken)
                 .ConfigureAwait(false);

    await resultTable_.SetResult(sessionId,
                                 ownerTaskId,
                                 key,
                                 cancellationToken)
                      .ConfigureAwait(false);
  }
}
