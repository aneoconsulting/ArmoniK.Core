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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = ArmoniK.Core.Common.Exceptions.KeyNotFoundException;
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

    try
    {
      var sessionCancelTask = sessionTable_.CancelSessionAsync(sessionId,
                                                               cancellationToken);

      await taskTable_.CancelSessionAsync(sessionId,
                                          cancellationToken)
                      .ConfigureAwait(false);

      await sessionCancelTask.ConfigureAwait(false);
    }
    catch (KeyNotFoundException e)
    {
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        e.Message));
    }
    catch (Exception e)
    {
      throw new RpcException(new Status(StatusCode.Unknown,
                                        e.Message));
    }
  }

  /// <inheritdoc />
  public async Task CancelDispatchSessionAsync(string            rootSessionId,
                                               string            dispatchId,
                                               CancellationToken cancellationToken)
  {
    using var _        = logger_.LogFunction(dispatchId);
    using var activity = activitySource_.StartActivity($"{nameof(CancelDispatchSessionAsync)}");
    var sessionCancelTask = sessionTable_.CancelDispatchAsync(rootSessionId,
                                                              dispatchId,
                                                              cancellationToken);

    await taskTable_.CancelDispatchAsync(rootSessionId,
                                         dispatchId,
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

    try
    {
      await taskTable_.CancelTasks(request,
                                   cancellationToken)
                      .ConfigureAwait(false);
    }
    catch (KeyNotFoundException e)
    {
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        e.Message));
    }
    catch (Exception e)
    {
      throw new RpcException(new Status(StatusCode.Unknown,
                                        e.Message));
    }
  }

  /// <inheritdoc />
  public async Task<CreateTaskReply> CreateTasks(string                        sessionId,
                                                 string                        parentId,
                                                 string                        dispatchId,
                                                 TaskOptions                   options,
                                                 IAsyncEnumerable<TaskRequest> taskRequests,
                                                 CancellationToken             cancellationToken)
  {
    using var logFunction = logger_.LogFunction(dispatchId);
    using var activity    = activitySource_.StartActivity($"{nameof(CreateTasks)}");
    using var sessionScope = logger_.BeginPropertyScope(("Session", sessionId),
                                                        ("TaskId", parentId),
                                                        ("Dispatch", dispatchId));

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
      var payloadChunksList = await taskRequest.PayloadChunks.ToListAsync(cancellationToken)
                                               .ConfigureAwait(false);

      if (payloadChunksList.Count == 1)
      {
        requests.Add(new Storage.TaskRequest(taskRequest.Id,
                                             taskRequest.ExpectedOutputKeys,
                                             taskRequest.DataDependencies,
                                             payloadChunksList.Single(),
                                             true));
      }
      else
      {
        requests.Add(new Storage.TaskRequest(taskRequest.Id,
                                             taskRequest.ExpectedOutputKeys,
                                             taskRequest.DataDependencies,
                                             Array.Empty<byte>(),
                                             false));
        payloadUploadTasks.Add(PayloadStorage(sessionId)
                                 .AddOrUpdateAsync(taskRequest.Id,
                                                   payloadChunksList.ToAsyncEnumerable(),
                                                   cancellationToken));
      }
    }

    await InitializeTaskCreationAsync(sessionId,
                                      parentId,
                                      dispatchId,
                                      options,
                                      requests,
                                      cancellationToken)
      .ConfigureAwait(false);


    var finalizationFilter = new TaskFilter
                             {
                               Task = new TaskFilter.Types.IdsRequest
                                      {
                                        Ids =
                                        {
                                          requests.Select(taskRequest => taskRequest.Id),
                                        },
                                      },
                             };

    await Task.WhenAll(payloadUploadTasks)
              .ConfigureAwait(false);

    await lockedQueueStorage_.EnqueueMessagesAsync(requests.Select(taskRequest => taskRequest.Id),
                                                   options.Priority,
                                                   cancellationToken)
                             .ConfigureAwait(false);

    await using var finalizer = AsyncDisposable.Create(async () => await taskTable_.FinalizeTaskCreation(finalizationFilter,
                                                                                                         cancellationToken)
                                                                                   .ConfigureAwait(false));

    return new CreateTaskReply
           {
             Successfull = new Empty(),
           };
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
  public async Task<CreateSessionReply> CreateSession(string            sessionId,
                                                      TaskOptions       defaultTaskOptions,
                                                      CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateSession)}");
    try
    {
      await sessionTable_.CreateSessionAsync(sessionId,
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
  public async Task CompleteTaskAsync(string            id,
                                      Output            output,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CompleteTaskAsync)}");

    Storage.Output cOutput = output;

    if (cOutput.Success)
    {
      await taskTable_.SetTaskSuccessAsync(id,
                                           cancellationToken)
                      .ConfigureAwait(false);
    }
    else
    {
      await taskTable_.SetTaskErrorAsync(id,
                                         cOutput.Error,
                                         cancellationToken)
                      .ConfigureAwait(false);
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

    var result = await resultTable_.GetResult(request.Session,
                                              request.Key,
                                              contextCancellationToken)
                                   .ConfigureAwait(false);

    logger_.LogDebug("OwnerTaskId {OwnerTaskId}",
                     result.OwnerTaskId);

    var continueWaiting = true;

    while (continueWaiting)
    {
      var ownerId = result.OwnerTaskId;
      var completion = await WaitForCompletion(new WaitRequest
                                               {
                                                 Filter = new TaskFilter
                                                          {
                                                            Task = new TaskFilter.Types.IdsRequest
                                                                   {
                                                                     Ids =
                                                                     {
                                                                       ownerId,
                                                                     },
                                                                   },
                                                          },
                                                 StopOnFirstTaskCancellation = true,
                                                 StopOnFirstTaskError        = true,
                                               },
                                               contextCancellationToken)
                         .ConfigureAwait(false);
      if (completion.Values.Any(count => count.Status is TaskStatus.Failed or TaskStatus.Error))
      {
        return new AvailabilityReply
               {
                 Error = new TaskError
                         {
                           TaskId = ownerId,
                         },
               };
      }

      if (completion.Values.Any(count => count.Status is TaskStatus.Canceled or TaskStatus.Canceling))
      {
        return new AvailabilityReply
               {
                 NotCompletedTask = ownerId,
               };
      }

      result = await resultTable_.GetResult(request.Session,
                                            request.Key,
                                            contextCancellationToken)
                                 .ConfigureAwait(false);
      logger_.LogDebug("OwnerTaskId {OwnerTaskId}",
                       result.OwnerTaskId);
      if (ownerId != result.OwnerTaskId)
      {
        continueWaiting = !result.IsResultAvailable;
        if (continueWaiting)
        {
          await Task.Delay(150,
                           contextCancellationToken)
                    .ConfigureAwait(false);
        }
      }
      else
      {
        continueWaiting = false;
      }
    }

    var availabilityReply = new AvailabilityReply
                            {
                              Ok = new Empty(),
                            };
    return availabilityReply;
  }

  /// <inheritdoc />
  public async Task<GetStatusReply> GetStatusAsync(GetStatusrequest  request,
                                                   CancellationToken contextCancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetStatusAsync)}");
    return new GetStatusReply
           {
             Status = await taskTable_.GetTaskStatus(request.TaskId,
                                                     contextCancellationToken)
                                      .ConfigureAwait(false),
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
  public async Task FinalizeDispatch(string            taskId,
                                     Dispatch          dispatch,
                                     CancellationToken cancellationToken)
  {
    using var activity      = activitySource_.StartActivity($"{nameof(FinalizeDispatch)}");
    var       oldDispatchId = dispatch.Id;
    var targetDispatchId = await taskTable_.GetTaskDispatchId(taskId,
                                                              cancellationToken)
                                           .ConfigureAwait(false);
    while (oldDispatchId != targetDispatchId)
    {
      await taskTable_.ChangeTaskDispatch(oldDispatchId,
                                          targetDispatchId,
                                          cancellationToken)
                      .ConfigureAwait(false);

      // to be done after awaiting previous call to ensure proper modification sequencing
      await resultTable_.ChangeResultDispatch(dispatch.SessionId,
                                              oldDispatchId,
                                              targetDispatchId,
                                              cancellationToken)
                        .ConfigureAwait(false);

      oldDispatchId = targetDispatchId;
      targetDispatchId = await taskTable_.GetTaskDispatchId(taskId,
                                                            cancellationToken)
                                         .ConfigureAwait(false);
    }
  }

  private IObjectStorage ResultStorage(string session)
    => objectStorageFactory_.CreateResultStorage(session);

  private IObjectStorage PayloadStorage(string session)
    => objectStorageFactory_.CreatePayloadStorage(session);

  /*
   * TODO :
   * Pour bien faire, il faudrait couper en deux :
   * Initialisation
   * Mise en queue + finalisation
   * Comme ça, depuis le request processor, on peut :
   * initialiser
   * attendre la fin de l'exec de la tâche
   * mettre à jour les ownership
   * Mise en queue + finalisation
   * + chuncking
   */

  public async Task InitializeTaskCreationAsync(string                           session,
                                                string                           parentTaskId,
                                                string                           dispatchId,
                                                TaskOptions                      options,
                                                IEnumerable<Storage.TaskRequest> requests,
                                                CancellationToken                cancellationToken = default)
  {
    using var _        = logger_.LogFunction($"{session}.{parentTaskId}.{dispatchId}");
    using var activity = activitySource_.StartActivity($"{nameof(InitializeTaskCreationAsync)}");
    activity?.AddTag("sessionId",
                     session);
    activity?.AddTag("parentTaskId",
                     parentTaskId);
    activity?.AddTag("dispatchId",
                     dispatchId);
    activity?.AddTag("taskIds",
                     string.Join(",",
                                 requests.Select(request => request.Id)));

    var ancestors = new List<string>();

    if (!parentTaskId.Equals(session))
    {
      var res = await taskTable_.GetTaskAncestorDispatchIds(parentTaskId,
                                                            cancellationToken)
                                .ConfigureAwait(false);
      ancestors.AddRange(res);
    }

    ancestors.Add(dispatchId);

    var taskDataModels = requests.Select(async request =>
                                         {
                                           var tdm = new TaskData(session,
                                                                  parentTaskId,
                                                                  dispatchId,
                                                                  request.Id,
                                                                  request.DataDependencies.ToList(),
                                                                  request.ExpectedOutputKeys.ToList(),
                                                                  request.HasPayload,
                                                                  request.PayloadChunk.ToArray(),
                                                                  TaskStatus.Creating,
                                                                  options,
                                                                  ancestors,
                                                                  new Storage.Output(false,
                                                                                     ""));

                                           var parentExpectedOutputKeys = new List<string>();

                                           // if there is no parent task, we do not need to get parent task expected output keys
                                           if (!parentTaskId.Equals(session))
                                           {
                                             parentExpectedOutputKeys.AddRange(await taskTable_.GetTaskExpectedOutputKeys(parentTaskId,
                                                                                                                          cancellationToken)
                                                                                               .ConfigureAwait(false));
                                           }


                                           var intersect = parentExpectedOutputKeys.Intersect(request.ExpectedOutputKeys)
                                                                                   .ToList();

                                           if (intersect.Any())
                                           {
                                             await resultTable_.ChangeResultOwnership(session,
                                                                                      intersect,
                                                                                      parentTaskId,
                                                                                      request.Id,
                                                                                      cancellationToken)
                                                               .ConfigureAwait(false);
                                           }
                                           else
                                           {
                                             logger_.LogTrace("intersect empty, no " + nameof(resultTable_.ChangeResultOwnership));
                                           }


                                           var resultModel = request.ExpectedOutputKeys.Except(intersect)
                                                                    .Select(key => new Result(session,
                                                                                              key,
                                                                                              request.Id,
                                                                                              dispatchId,
                                                                                              false,
                                                                                              DateTime.UtcNow,
                                                                                              Array.Empty<byte>()));
                                           return (TaskDataModel: tdm, ResultModel: resultModel);
                                         })
                                 .ToList();

    await taskTable_.CreateTasks(taskDataModels.Select(tuple => tuple.Result.TaskDataModel),
                                 cancellationToken)
                    .ConfigureAwait(false);

    var resultCreations = taskDataModels.SelectMany(tuple => tuple.Result.ResultModel);
    if (resultCreations.Any())
    {
      await resultTable_.Create(resultCreations,
                                cancellationToken)
                        .ConfigureAwait(false);
    }
  }
}
