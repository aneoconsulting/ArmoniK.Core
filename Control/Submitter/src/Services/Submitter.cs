// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.DataModel;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = ArmoniK.Core.Common.Exceptions.KeyNotFoundException;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Control.Submitter.Services;

public class Submitter : Api.gRPC.V1.Submitter.SubmitterBase
{
  private const int MaxDataChunkSize = PayloadConfiguration.MaxChunkSize;

  private readonly IQueueStorage         lockedQueueStorage_;
  private readonly ILogger<Submitter>    logger_;
  private readonly ITableStorage         tableStorage_;
  private readonly IObjectStorageFactory objectStorageFactory_;

  public Submitter(ITableStorage                            tableStorage,
                   IQueueStorage                            lockedQueueStorage,
                   IObjectStorageFactory objectStorageFactory,
                   ILogger<Submitter>                       logger)
  {
    tableStorage_         = tableStorage;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    lockedQueueStorage_   = lockedQueueStorage;
  }

  private IObjectStorage ResultStorage(string session) => objectStorageFactory_.CreateResultStorage(session);
  private IObjectStorage PayloadStorage(string session) => objectStorageFactory_.CreateResultStorage(session);

  /// <inheritdoc />
  public override Task<ConfigurationReply> GetServiceConfiguration(Empty request, ServerCallContext context)
    => Task.FromResult(new ConfigurationReply()
                       {
                         DataChunkMaxSize = MaxDataChunkSize,
                       });

  public override async Task<Empty> CancelSession(SessionId request, ServerCallContext context)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      await tableStorage_.CancelSessionAsync(request,
                                             context.CancellationToken);
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

  public override async Task<Empty> CancelTask(TaskFilter request, ServerCallContext context)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    try
    {
      await tableStorage_.CancelTask(request,
                                     context.CancellationToken);
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
  public override Task<CreateSessionReply> CreateSession(CreateSessionRequest request, ServerCallContext context) 
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    return tableStorage_.CreateSessionAsync(request,
                                            context.CancellationToken);
  }

  public override async Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request, ServerCallContext context)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    var options = request.TaskOptions ??
                  await tableStorage_.GetDefaultTaskOption(request.SessionId, 
                                                           request.ParentTaskId,
                                                           context
                                                            .CancellationToken);

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

    await tableStorage_.InitializeTaskCreation(request.SessionId,
                                               request.ParentTaskId,
                                               options,
                                               request.TaskRequests,
                                               context.CancellationToken);

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
                                                                                                            context.CancellationToken)
                                                      );



    await lockedQueueStorage_.EnqueueMessagesAsync(request.TaskRequests.Select(taskRequest => taskRequest.Id),
                                                               options.Priority,
                                                               context.CancellationToken);

    return new()
           {
             Successfull = new(),
           };
  }

  /// <inheritdoc />
  public override async Task<CreateTaskReply> CreateLargeTasks(IAsyncStreamReader<CreateLargeTaskRequest> requestStream, ServerCallContext context)
  {
    using var logFunction = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    await requestStream.ForceMoveNext("Stream finished with no element",
                                      logger_);

    var initRequest = requestStream.Current.GetInitRequest(logger_,
                                                           "first message should initiate the request");
    using var sessionScope = logger_.BeginPropertyScope(("Session", initRequest.SessionId));

    var options = initRequest.TaskOptions ??
                  await tableStorage_.GetDefaultTaskOption(initRequest.SessionId,
                                                           initRequest.ParentTaskId,
                                                           context
                                                            .CancellationToken);

    if (options.Priority >= lockedQueueStorage_.MaxPriority)
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           $"Max priority is {lockedQueueStorage_.MaxPriority}"));
      logger_.LogError(exception,
                       "Invalid Argument");
      throw exception;
    }

    var taskRequests       = new List<CreateSmallTaskRequest.Types.TaskRequest>();
    var payloadUploadTasks = new List<Task>();

    while (await requestStream.MoveNext())
    {
      var initTaskRequest = requestStream.Current.GetInitTask(logger_,
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
                         Payload = initTaskRequest.PayloadComplete ? initTaskRequest.PayloadChunk : null,
                       });



      if (!initTaskRequest.PayloadComplete)
      {
        var pipe = Channel.CreateUnbounded<ReadOnlyMemory<byte>>();

        payloadUploadTasks.Add(PayloadStorage(initRequest.SessionId).AddOrUpdateAsync(initTaskRequest.Id,
                                                                          pipe.Reader.ReadAllAsync()));

        await pipe.Writer.WriteAsync(initTaskRequest.PayloadChunk.Memory);

        var continuePayload = true;

        while (continuePayload)
        {

          await requestStream.ForceMoveNext("Previous message had incomplete PayloadChunk. Need a payload message.",
                                            logger_);

          var payload = requestStream.Current.GetTaskPayload(logger_,
                                                             "payload from previous message was not complete, need a payload message.");

          await pipe.Writer.WriteAsync(payload.PayloadChunk.Memory);

          continuePayload = !payload.PayloadComplete;
        }

        pipe.Writer.Complete();

        logger_.LogDebug($"Payload is complete");
      }
    }

    await tableStorage_.InitializeTaskCreation(initRequest.SessionId,
                                               initRequest.ParentTaskId,
                                               options,
                                               taskRequests,
                                               context.CancellationToken);


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
                                                                                                            context.CancellationToken));


    var enqueueTask = lockedQueueStorage_.EnqueueMessagesAsync(taskRequests.Select(taskRequest => taskRequest.Id),
                                                               options.Priority,
                                                               context.CancellationToken);

    await Task.WhenAll(enqueueTask,
                       Task.WhenAll(payloadUploadTasks));



    return new()
           {
             Successfull = new(),
           };
  }
  
  /// <inheritdoc />
  public override async Task<Count> CountTasks(TaskFilter request, ServerCallContext context)

  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var count = await tableStorage_.CountTasksAsync(request,
                                                    context.CancellationToken);
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
  public override async Task TryGetResult(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, ServerCallContext context)
  {
    var storage = ResultStorage(request.Session);
    await foreach(var chunk in storage.TryGetValuesAsync(request.Key, context.CancellationToken))
    {
      await responseStream.WriteAsync(new()
                                      {
                                        Result = UnsafeByteOperations.UnsafeWrap(new(chunk)),
                                      });
    }
  }
  
  public override async Task<Count> WaitForCompletion(WaitRequest request, ServerCallContext context)
  {
    using var _ = logger_.LogFunction();

    if (logger_.IsEnabled(LogLevel.Trace))
    {
      context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }


    Task<IEnumerable<(TaskStatus Status, int Count)>> CountUpdateFunc()
      => tableStorage_.CountTasksAsync(request.Filter,
                                       context.CancellationToken);

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