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
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = ArmoniK.Core.Exceptions.KeyNotFoundException;
using TaskCanceledException = ArmoniK.Core.Exceptions.TaskCanceledException;
using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Control.Services
{
  public class ClientService : Core.gRPC.V1.ClientService.ClientServiceBase
  {
    private readonly IQueueStorage                         lockedQueueStorage_;
    private readonly ILogger<ClientService>                logger_;
    private readonly ITableStorage                         tableStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;

    public ClientService(ITableStorage                         tableStorage,
                         IQueueStorage                         lockedQueueStorage,
                         KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                         KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                         ILogger<ClientService>                logger)
    {
      tableStorage_       = tableStorage;
      taskResultStorage_  = taskResultStorage;
      taskPayloadStorage_ = taskPayloadStorage;
      logger_             = logger;
      lockedQueueStorage_ = lockedQueueStorage;
    }

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

    public override async Task<Empty> CloseSession(SessionId request, ServerCallContext context)
    {
      using var _ = logger_.LogFunction();


      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      try
      {
        await tableStorage_.CloseSessionAsync(request,
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

    public override Task<SessionId> CreateSession(SessionOptions request, ServerCallContext context)
    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      return tableStorage_.CreateSessionAsync(request,
                                              context.CancellationToken);
    }

    public override async Task<CreateTaskReply> CreateTask(CreateTaskRequest request, ServerCallContext context)
    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }


      var options = request.TaskOptions ??
                    await tableStorage_.GetDefaultTaskOption(request.SessionId,
                                                             context
                                                              .CancellationToken);

      if (options.Priority >= lockedQueueStorage_.MaxPriority)
      {
        context.Status = new (StatusCode.InvalidArgument,
                                    $"Max priority is {lockedQueueStorage_.MaxPriority}");
        return null;
      }


      var inits = await tableStorage_.InitializeTaskCreation(request.SessionId,
                                                             options,
                                                             request.TaskRequests,
                                                             context.CancellationToken)
                                     .ToListAsync();

      await using var finalizer = AsyncDisposable.Create(async () => await tableStorage_.FinalizeTaskCreation(new TaskFilter
                                                                                                              {
                                                                                                                SessionId = request.SessionId.Session,
                                                                                                                SubSessionId = request.SessionId
                                                                                                                                      .SubSession,
                                                                                                                IncludedTaskIds =
                                                                                                                {
                                                                                                                  inits.Select(tuple
                                                                                                                                 => tuple
                                                                                                                                   .id
                                                                                                                                   .Task),
                                                                                                                },
                                                                                                              },
                                                                                                              context.CancellationToken));

      var payloadsUpdateTask = inits.Where(tuple => !tuple.HasPayload)
                                    .Select(tuple => taskPayloadStorage_.AddOrUpdateAsync(tuple.id,
                                                                                          new()
                                                                                          {
                                                                                            Data = ByteString.CopyFrom(tuple.Payload),
                                                                                          },
                                                                                          context.CancellationToken))
                                    .WhenAll();


      var enqueueTask = lockedQueueStorage_.EnqueueMessagesAsync(inits.Select(tuple => tuple.id),
                                                                 options.Priority,
                                                                 context.CancellationToken);

      await Task.WhenAll(enqueueTask,
                         payloadsUpdateTask);

      CreateTaskReply reply = new();
      reply.TaskIds.Add(inits.Select(tuple => tuple.id));
      return reply;
    }

    public override async Task<Count> GetTasksCount(TaskFilter request, ServerCallContext context)
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

    public override async Task<TaskIdList> ListTask(TaskFilter        request,
                                                    ServerCallContext context)
    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      var list = await tableStorage_.ListTasksAsync(request,
                                                    context.CancellationToken).ToListAsync(context.CancellationToken);

      var output = new TaskIdList();
      output.TaskIds.Add(list);
      return output;
    }

    /// <inheritdoc />
    public override async Task<TaskIdList> ListSubTasks(TaskFilter request, ServerCallContext context)
    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      TaskIdList wholeList = new();

      var listAsync = tableStorage_.ListTasksAsync(request,
                                                   context.CancellationToken);

      await foreach (var tid in listAsync)
      {
        var localFilter = new TaskFilter(request)
                          {
                            SubSessionId = tid.Task,
                          };
        localFilter.IncludedTaskIds.Clear();
        var localList = await ListSubTasks(localFilter,
                                           context);
        wholeList.TaskIds.Add(tid);
        wholeList.TaskIds.Add(localList.TaskIds);
      }

      return wholeList;
    }

    /// <inheritdoc />
    public override async Task<Count> GetSubTasksCount(TaskFilter request, ServerCallContext context)

    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      var count = await tableStorage_.CountSubTasksAsync(request,
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

    public override async Task<MultiplePayloadReply> TryGetResult(TaskFilter        request,
                                                                  ServerCallContext context)
    {
      using var            _                    = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      MultiplePayloadReply multiplePayloadReply = new();
      await foreach (var taskId in tableStorage_.ListTasksAsync(request,
                                                                context.CancellationToken)
                                                .WithCancellation(context.CancellationToken))
      {
        var result = await taskResultStorage_.TryGetValuesAsync(taskId,
                                                                context.CancellationToken);
        var reply = new SinglePayloadReply
                    {
                      TaskId = taskId,
                      Data = new()
                             {
                               Data = result.Result,
                             },
                    };
        multiplePayloadReply.Payloads.Add(reply);
      }

      return multiplePayloadReply;
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
              if (request.ThrowOnTaskCancellation && count > 0) throw new TaskCanceledException("A task was cancelled during execution");
              break;
            case TaskStatus.Timeout:
              notCompleted += count;
              break;
            case TaskStatus.Canceling:
              notCompleted += count;
              break;
            case TaskStatus.Canceled:
              notCompleted += count;
              if (request.ThrowOnTaskCancellation && count > 0) throw new TaskCanceledException("A task was cancelled during execution");
              break;
            case TaskStatus.Processing:
              notCompleted += count;
              break;
            case TaskStatus.WaitingForChildren:
              notCompleted += count;
              break;
            case TaskStatus.Error:
              notCompleted += count;
              break;
            default:
              throw new ArmoniKException($"Unknown TaskStatus {status}");
          }
        }

        if (notCompleted == 0)
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

    /// <inheritdoc />
    public override async Task<Count> WaitForSubTasksCompletion(WaitRequest request, ServerCallContext context)
    {
      using var _ = logger_.LogFunction();

      if (logger_.IsEnabled(LogLevel.Trace))
      {
        context.CancellationToken.Register(() => logger_.LogTrace("CancellationToken from ServerCallContext has been triggered"));
      }

      await WaitForCompletion(request,
                              context);

      Task<IEnumerable<(TaskStatus Status, int Count)>> CountUpdateFunc()
        => tableStorage_.CountSubTasksAsync(request.Filter,
                                            context.CancellationToken);

      return await WaitForCompletionCore(request,
                                         CountUpdateFunc);
    }
  }
}
