// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

public class Submitter : ISubmitter
{
  private readonly IQueueStorage                    lockedQueueStorage_;
  private readonly ILogger<Submitter> logger_;
  private readonly IObjectStorageFactory            objectStorageFactory_;


  private readonly ISessionTable  sessionTable_;
  private readonly ITaskTable     taskTable_;
  private readonly IResultTable   resultTable_;

  [UsedImplicitly]
  public Submitter(IQueueStorage         lockedQueueStorage,
                   IObjectStorageFactory objectStorageFactory,
                   ILogger<Submitter>    logger,
                   ISessionTable         sessionTable,
                   ITaskTable            taskTable,
                   IResultTable          resultTable)
  {
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    sessionTable_         = sessionTable;
    taskTable_            = taskTable;
    resultTable_          = resultTable;
    lockedQueueStorage_   = lockedQueueStorage;
  }

  private IObjectStorage ResultStorage(string  session) => objectStorageFactory_.CreateResultStorage(session);
  private IObjectStorage PayloadStorage(string session) => objectStorageFactory_.CreateResultStorage(session);

  /// <inheritdoc />
  public  Task<Configuration> GetServiceConfiguration(Empty request, CancellationToken cancellationToken)
    => Task.FromResult(new Configuration()
                       {
                         DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                       });

  /// <inheritdoc />
  public  async Task CancelSession(string sessionId, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      var sessionCancelTask = sessionTable_.CancelSessionAsync(sessionId,
                                                               cancellationToken);

      await taskTable_.CancelSessionAsync(sessionId,
                                          cancellationToken);

      await sessionCancelTask;
    }
    catch (KeyNotFoundException e)
    {
      throw new RpcException(new(StatusCode.FailedPrecondition,
                                 e.Message));
    }
    catch (Exception e)
    {
      throw new RpcException(new(StatusCode.Unknown,
                                 e.Message));
    }
  }

  /// <inheritdoc />
  public async Task CancelDispatchSessionAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction(dispatchId);
    var sessionCancelTask = sessionTable_.CancelDispatchAsync(rootSessionId,
                                                              dispatchId,
                                                              cancellationToken);

    await taskTable_.CancelDispatchAsync(rootSessionId,
                                         dispatchId,
                                         cancellationToken);

    await sessionCancelTask;
  }

  /// <inheritdoc />
  public async Task CancelTasks(TaskFilter request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      await taskTable_.CancelTasks(request,
                                   cancellationToken);
    }
    catch (KeyNotFoundException e)
    {
      throw new RpcException(new(StatusCode.FailedPrecondition,
                                 e.Message));
    }
    catch (Exception e)
    {
      throw new RpcException(new(StatusCode.Unknown,
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
    using var sessionScope = logger_.BeginPropertyScope(("Session", sessionId),
                                                        ("TaskId", parentId),
                                                        ("Dispatch", dispatchId));

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    options ??= await sessionTable_.GetDefaultTaskOptionAsync(sessionId,
                                                              cancellationToken);

    if (options.Priority >= lockedQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           $"Max priority is {lockedQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }




    var requests           = new List<Storage.TaskRequest>();
    var payloadUploadTasks = new List<Task>();

    await foreach (var taskRequest in taskRequests.WithCancellation(cancellationToken))
    {
      if (await taskRequest.PayloadChunks.CountAsync(cancellationToken) == 1)
      {
        requests.Add(new(taskRequest.Id,
                         taskRequest.ExpectedOutputKeys,
                         taskRequest.DataDependencies,
                         await taskRequest.PayloadChunks.SingleAsync(cancellationToken)));
      }
      else
      {
        requests.Add(new(taskRequest.Id,
                         taskRequest.ExpectedOutputKeys,
                         taskRequest.DataDependencies,
                         null));
        payloadUploadTasks.Add(PayloadStorage(sessionId).AddOrUpdateAsync(taskRequest.Id,
                                                                          taskRequest.PayloadChunks,
                                                                          cancellationToken));
      }
    }

    await InitializeTaskCreationAsync(sessionId,
                                                    parentId,
                                                    dispatchId,
                                                    options,
                                                    requests,
                                                    cancellationToken);


    var finalizationFilter = new TaskFilter
                             {
                               Task = new()
                                      {
                                        Ids =
                                        {
                                          requests.Select(taskRequest => taskRequest.Id),
                                        },
                                      },
                             };
    await using var finalizer = AsyncDisposable.Create(async () => await taskTable_.FinalizeTaskCreation(finalizationFilter,
                                                                                                            cancellationToken));


    var enqueueTask = lockedQueueStorage_.EnqueueMessagesAsync(requests.Select(taskRequest => taskRequest.Id),
                                                               options.Priority,
                                                               cancellationToken);

    await Task.WhenAll(enqueueTask,
                       Task.WhenAll(payloadUploadTasks));



    return new()
           {
             Successfull = new(),
           };
  }

  
  public async Task InitializeTaskCreationAsync(string                           session,
                                                string                           parentTaskId,
                                                string                           dispatchId,
                                                TaskOptions                      options,
                                                IEnumerable<Storage.TaskRequest> requests,
                                                CancellationToken                cancellationToken = default)
  {
    using var _                = logger_.LogFunction($"{session}.{parentTaskId}.{dispatchId}");


    async Task LoadOptions()
    {
      options = await sessionTable_.GetDefaultTaskOptionAsync(session,
                                                              cancellationToken);
    }

    IList<string> ancestors = null;

    async Task LoadAncestorDispatchIds()
    {
      ancestors = await taskTable_.GetTaskAncestorDispatchIds(parentTaskId,
                                                              cancellationToken);

      ancestors.Add(dispatchId);
    }


    var preload = new List<Task>();
    if (options is null)
    {

      preload.Add(LoadOptions());
    }

    preload.Add(LoadAncestorDispatchIds());

    await Task.WhenAll(preload);


    var taskDataModels = requests.Select(request =>
                                         {
                                           var tdm = new TaskData(session,
                                                                  parentTaskId,
                                                                  dispatchId,
                                                                  request.Id,
                                                                  request.DataDependencies.ToList(),
                                                                  request.ExpectedOutputKeys.ToList(),
                                                                  request.PayloadChunk is not null,
                                                                  request.PayloadChunk?.ToArray(),
                                                                  TaskStatus.Creating,
                                                                  options,
                                                                  ancestors);

                                           var resultModel = request.ExpectedOutputKeys
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

    await taskTable_.CreateTasks(taskDataModels.Select(tuple => tuple.TaskDataModel),
                                 cancellationToken);

    await resultTable_.Create(taskDataModels.SelectMany(tuple => tuple.ResultModel),
                              cancellationToken);
  }


  /// <inheritdoc />
  public  async Task<Count> CountTasks(TaskFilter request, CancellationToken cancellationToken)

  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var count = await taskTable_.CountTasksAsync(request,
                                                    cancellationToken);
    return new()
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
  public async Task<CreateSessionReply> CreateSession(string sessionId, TaskOptions defaultTaskOptions, CancellationToken cancellationToken)
  {
    try
    {
      await sessionTable_.CreateSessionAsync(sessionId,
                                             defaultTaskOptions,
                                             cancellationToken);
      return new()
             {
               Ok = new(),
             };
    }
    catch (Exception e)
    {
      Console.WriteLine(e);
      return new()
             {
               Error = e.ToString(),
             };
    }
  }

  /// <inheritdoc />
  public  async Task TryGetResult(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, CancellationToken cancellationToken)
  {
    var storage = ResultStorage(request.Session);
    await foreach (var chunk in storage.TryGetValuesAsync(request.Key, cancellationToken))
    {
      await responseStream.WriteAsync(new()
                                      {
                                        Result = new()
                                                 {
                                                   Data = UnsafeByteOperations.UnsafeWrap(new(chunk)),
                                                 },
                                      });
    }

    await responseStream.WriteAsync(new()
                                    {
                                      Result = new()
                                               {
                                                 DataComplete = true,
                                               },
                                    });
  }

  public  async Task<Count> WaitForCompletion(WaitRequest request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    Task<IEnumerable<TaskStatusCount>> CountUpdateFunc()
      => taskTable_.CountTasksAsync(request.Filter,
                                       cancellationToken);

    var output          = new Count();
    var countUpdateFunc = CountUpdateFunc;
    while (true)
    {
      var counts       = await countUpdateFunc();
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


      await Task.Delay(taskTable_.PollingDelay,
                       cancellationToken);
    }

    return output;
  }

  /// <inheritdoc />
  public async Task UpdateTaskStatusAsync(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction();
    await taskTable_.UpdateTaskStatusAsync(id,status, cancellationToken);
  }

  /// <inheritdoc />
  public async Task FinalizeDispatch(string taskId, IDispatch dispatch, Output output, CancellationToken cancellationToken)
  {
    var oldDispatchId = dispatch.Id;
    var targetDispatchId = await taskTable_.GetTaskDispatchId(taskId,
                                                              cancellationToken);
    while (oldDispatchId != targetDispatchId)
    {
      await taskTable_.ChangeTaskDispatch(oldDispatchId,
                                             targetDispatchId,
                                             cancellationToken);

      // to be done after awaiting previous call to ensure proper modification sequencing
      await resultTable_.ChangeResultDispatch(dispatch.SessionId,
                                              oldDispatchId,
                                              targetDispatchId,
                                              cancellationToken);

      oldDispatchId = targetDispatchId;
      targetDispatchId = await taskTable_.GetTaskDispatchId(taskId,
                                                            cancellationToken);
    }
  }
}
