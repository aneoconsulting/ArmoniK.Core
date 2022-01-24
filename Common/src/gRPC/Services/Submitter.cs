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
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = ArmoniK.Core.Common.Exceptions.KeyNotFoundException;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

public class Submitter : ISubmitter
{
  private readonly IQueueStorage                    lockedQueueStorage_;
  private readonly ILogger<Submitter> logger_;
  private readonly ITableStorage                    tableStorage_;
  private readonly IObjectStorageFactory            objectStorageFactory_;

  public Submitter(ITableStorage         tableStorage,
                   IQueueStorage         lockedQueueStorage,
                   IObjectStorageFactory objectStorageFactory,
                   ILogger<Submitter>    logger)
  {
    tableStorage_         = tableStorage;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    lockedQueueStorage_   = lockedQueueStorage;
  }

  private IObjectStorage ResultStorage(string  session) => objectStorageFactory_.CreateResultStorage(session);
  private IObjectStorage PayloadStorage(string session) => objectStorageFactory_.CreateResultStorage(session);

  /// <inheritdoc />
  public  Task<ConfigurationReply> GetServiceConfiguration(Empty request, CancellationToken cancellationToken)
    => Task.FromResult(new ConfigurationReply()
                       {
                         DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                       });

  public  async Task<Empty> CancelSession(SessionId request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      await tableStorage_.CancelSessionAsync(request,
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

    return new();
  }

  public  async Task<Empty> CancelTask(TaskFilter request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      await tableStorage_.CancelTask(request,
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

    return new();
  }

  /// <inheritdoc />
  public  Task<CreateSessionReply> CreateSession(CreateSessionRequest request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    return tableStorage_.CreateSessionAsync(request.Id,
                                            request.DefaultTaskOption,
                                            cancellationToken);
  }

  public  async Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    var options = request.TaskOptions ??
                  await tableStorage_.GetDefaultTaskOptionAsync(request.SessionId,
                                                           request.ParentTaskId,
                                                           cancellationToken);

    if (options.Priority >= lockedQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           $"Max priority is {lockedQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }

    foreach (var taskRequest in request.TaskRequests.Where(taskRequest => taskRequest.Payload.Length > PayloadConfiguration.MaxChunkSize))
    {
      throw new RpcException(new(StatusCode.InvalidArgument,
                                 $"Too big payload for task {taskRequest.Id}. Please use {nameof(CreateLargeTasks)} instead."));
    }

    await tableStorage_.InitializeTaskCreationAsync(request.SessionId,
                                               request.ParentTaskId,
                                               TODO,
                                               options,
                                               request.TaskRequests,
                                               cancellationToken);

    var finalizationFilter = new TaskFilter
                             {
                               Known = new()
                                       {
                                         TaskIds =
                                         {
                                           request.TaskRequests.Select(taskRequest => taskRequest.Id),
                                         },
                                       },
                             };
    await using var finalizer = AsyncDisposable.Create(async () => await tableStorage_.FinalizeTaskCreation(finalizationFilter,
                                                                                                            cancellationToken)
                                                      );



    await lockedQueueStorage_.EnqueueMessagesAsync(request.TaskRequests.Select(taskRequest => taskRequest.Id),
                                                   options.Priority,
                                                   cancellationToken);

    return new()
           {
             Successfull = new(),
           };
  }

  /// <inheritdoc />
  public  async Task<CreateTaskReply> CreateLargeTasks(IAsyncEnumerable<CreateLargeTaskRequest> requestStream, CancellationToken cancellationToken)
  {
    using var logFunction = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var requestEnumerator = requestStream.GetAsyncEnumerator(cancellationToken);

    await requestEnumerator.ForceMoveNext("Stream finished with no element",
                                      logger_);

    var initRequest = requestEnumerator.Current.GetInitRequest(logger_,
                                                           "first message should initiate the request");
    using var sessionScope = logger_.BeginPropertyScope(("Session", initRequest.SessionId));

    var options = initRequest.TaskOptions ??
                  await tableStorage_.GetDefaultTaskOptionAsync(initRequest.SessionId,
                                                           initRequest.ParentTaskId,
                                                           cancellationToken);

    if (options.Priority >= lockedQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           $"Max priority is {lockedQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }

    var taskRequests       = new List<TaskRequest>();
    var payloadUploadTasks = new List<Task>();

    while (await requestEnumerator.MoveNextAsync())
    {
      var initTaskRequest = requestEnumerator.Current.GetInitTask(logger_,
                                                              "first message after initialization of end of payload must be a task initialization");

      taskRequests.Add(new()
                       {
                         Id = initTaskRequest.Id,
                         DataDependencies =
                         {
                           initTaskRequest.DataDependencies,
                         },
                         ExpectedOutputKeys =
                         {
                           initTaskRequest.ExpectedOutputKeys,
                         },
                         Payload = initTaskRequest.PayloadChunk.DataComplete ? initTaskRequest.PayloadChunk.Data : null,
                       });



      if (!initTaskRequest.PayloadChunk.DataComplete)
      {
        var pipe = Channel.CreateUnbounded<ReadOnlyMemory<byte>>();

        payloadUploadTasks.Add(PayloadStorage(initRequest.SessionId).AddOrUpdateAsync(initTaskRequest.Id,
                                                                                      pipe.Reader.ReadAllAsync(cancellationToken),
                                                                                      cancellationToken));

        await pipe.Writer.WriteAsync(initTaskRequest.PayloadChunk.Data.Memory,
                                     cancellationToken);

        var continuePayload = true;

        while (continuePayload)
        {

          await requestEnumerator.ForceMoveNext("Previous message had incomplete PayloadChunk. Need a payload message.",
                                            logger_);

          var payload = requestEnumerator.Current.GetTaskPayload(logger_,
                                                             "payload from previous message was not complete, need a payload message.");

          await pipe.Writer.WriteAsync(payload.Data.Memory,
                                       cancellationToken);

          continuePayload = !payload.DataComplete;
        }

        pipe.Writer.Complete();

        logger_.LogDebug($"Payload is complete");
      }
    }

    await tableStorage_.InitializeTaskCreationAsync(initRequest.SessionId,
                                               initRequest.ParentTaskId,
                                               TODO,
                                               options,
                                               taskRequests,
                                               cancellationToken);


    var finalizationFilter = new TaskFilter
                             {
                               Known = new()
                                       {
                                         TaskIds =
                                         {
                                           taskRequests.Select(taskRequest => taskRequest.Id),
                                         },
                                       },
                             };
    await using var finalizer = AsyncDisposable.Create(async () => await tableStorage_.FinalizeTaskCreation(finalizationFilter,
                                                                                                            cancellationToken));


    var enqueueTask = lockedQueueStorage_.EnqueueMessagesAsync(taskRequests.Select(taskRequest => taskRequest.Id),
                                                               options.Priority,
                                                               cancellationToken);

    await Task.WhenAll(enqueueTask,
                       Task.WhenAll(payloadUploadTasks));



    return new()
           {
             Successfull = new(),
           };
  }

  /// <inheritdoc />
  public  async Task<Count> CountTasks(TaskFilter request, CancellationToken cancellationToken)

  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var count = await tableStorage_.CountTasksAsync(request,
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
  public  async Task TryGetResult(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, CancellationToken cancellationToken)
  {
    var storage = ResultStorage(request.Session);
    await foreach (var chunk in storage.TryGetValuesAsync(request.Key, cancellationToken))
    {
      await responseStream.WriteAsync(new()
                                      {
                                        Result = UnsafeByteOperations.UnsafeWrap(new(chunk)),
                                      });
    }
  }

  public  async Task<Count> WaitForCompletion(WaitRequest request, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    Task<IEnumerable<(TaskStatus Status, int Count)>> CountUpdateFunc()
      => tableStorage_.CountTasksAsync(request.Filter,
                                       cancellationToken);

    return await WaitForCompletionCore(request,
                                       CountUpdateFunc);
  }

  private async Task<Count> WaitForCompletionCore(WaitRequest request,
                                                  Func<Task<IEnumerable<(TaskStatus Status, int Count)>>>
                                                    countUpdateFunc)
  {
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
          default:
            throw new ArmoniKException($"Unknown TaskStatus {status}");
        }
      }

      if (notCompleted == 0 || (request.StopOnFirstTaskError && error) || (request.StopOnFirstTaskCancellation && cancelled))
      {
        var output = new Count();
        // ReSharper disable once PossibleMultipleEnumeration
        output.Values.AddRange(counts.Select(tuple => new StatusCount
                                                      {
                                                        Count  = tuple.Count,
                                                        Status = tuple.Status,
                                                      }));
        logger_.LogDebug("All sub tasks have completed. Returning count={count}",
                         output);
        return output;
      }


      await Task.Delay(tableStorage_.PollingDelay);
    }
  }
}
