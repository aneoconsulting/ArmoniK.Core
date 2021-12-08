using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Grpc.Core;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.Utils;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;
using Microsoft.Extensions.Options;

namespace ArmoniK.Control.Services
{
  public class ClientService : Core.gRPC.V1.ClientService.ClientServiceBase
  {
    private readonly ITableStorage                         tableStorage_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly ILogger<ClientService>                logger_;
    private readonly IQueueStorage                         lockedQueueStorage_;

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
      lockedQueueStorage_       = lockedQueueStorage;
    }

    public override async Task<Empty> CancelSession(SessionId request, ServerCallContext context)
    {
      logger_.LogFunction();
      try
      {
        await tableStorage_.CancelSessionAsync(request, context.CancellationToken);
      }
      catch (KeyNotFoundException e)
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition, e.Message));
      }
      catch (Exception e)
      {
        throw new RpcException(new Status(StatusCode.Unknown, e.Message));
      }

      return new Empty();
    }

    public override async Task<Empty> CancelTask(TaskFilter request, ServerCallContext context)
    {
      logger_.LogFunction();
      try
      {
        await tableStorage_.CancelTask(request, context.CancellationToken);
      }
      catch (KeyNotFoundException e)
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition, e.Message));
      }
      catch (Exception e)
      {
        throw new RpcException(new Status(StatusCode.Unknown, e.Message));
      }

      return new();
    }

    public override async Task<Empty> CloseSession(SessionId request, ServerCallContext context)
    {
      logger_.LogFunction();
      try
      {
        await tableStorage_.CloseSessionAsync(request, context.CancellationToken);
      }
      catch (KeyNotFoundException e)
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition, e.Message));
      }
      catch (Exception e)
      {
        throw new RpcException(new Status(StatusCode.Unknown, e.Message));
      }

      return new();
    }

    public override Task<SessionId> CreateSession(SessionOptions request, ServerCallContext context)
    {
      logger_.LogFunction();
      return tableStorage_.CreateSessionAsync(request, context.CancellationToken);
    }

    public override async Task<CreateTaskReply> CreateTask(CreateTaskRequest request, ServerCallContext context)
    {
      logger_.LogFunction();

      var options = request.TaskOptions ??
                    await tableStorage_.GetDefaultTaskOption(request.SessionId,
                                                             context
                                                              .CancellationToken);

      var inits = await tableStorage_.InitializeTaskCreation(request.SessionId,
                                                           options,
                                                           request.TaskRequests.Select(taskRequest => taskRequest.Payload),
                                                           context.CancellationToken)
                                     .ToListAsync();

      await using var finalizer = AsyncDisposable.Create(async () => await tableStorage_.FinalizeTaskCreation(new TaskFilter
                                                                                                              {
                                                                                                                SessionId    = request.SessionId.Session,
                                                                                                                SubSessionId = request.SessionId.SubSession,
                                                                                                                IncludedTaskIds =
                                                                                                                {
                                                                                                                  inits.Select(tuple => tuple.id.Task),
                                                                                                                },
                                                                                                              },
                                                                                                              context.CancellationToken));

      var payloadsUpdateTask = inits.Where(tuple => !tuple.HasPayload)
                                    .Select(tuple => taskPayloadStorage_.AddOrUpdateAsync(tuple.Item1,
                                                                                          new Payload{Data = ByteString.CopyFrom(tuple.Payload)},
                                                                                          context.CancellationToken))
                                    .WhenAll();


      var enqueueTask = lockedQueueStorage_.EnqueueMessagesAsync(inits.Select(tuple => tuple.id),
                                                                 options.Priority,
                                                                 context.CancellationToken);

      await Task.WhenAll(enqueueTask, payloadsUpdateTask);

      CreateTaskReply reply = new();
      reply.TaskIds.Add(inits.Select(tuple => tuple.id));
      return reply;
    }

    public override async Task<Count> GetTasksCount(TaskFilter request, ServerCallContext context)
    {
      logger_.LogFunction();
      var count = await tableStorage_.CountTasksAsync(request, context.CancellationToken);
      return new Count { Value = count };
    }

    public override async Task ListTask(TaskFilter                  request,
                                        IServerStreamWriter<TaskId> responseStream,
                                        ServerCallContext           context)
    {
      logger_.LogFunction();
      await foreach (var taskId in tableStorage_.ListTasksAsync(request, context.CancellationToken)
                                                .WithCancellation(context.CancellationToken))
      {
        await responseStream.WriteAsync(taskId);
      }
    }

    public override async Task TryGetResult(TaskFilter                              request,
                                            IServerStreamWriter<SinglePayloadReply> responseStream,
                                            ServerCallContext                       context)
    {
      logger_.LogFunction();
      await foreach (var taskId in tableStorage_.ListTasksAsync(request, context.CancellationToken)
                                                .WithCancellation(context.CancellationToken))
      {
        var result = await taskResultStorage_.TryGetValuesAsync(taskId, context.CancellationToken);
        var reply  = new SinglePayloadReply { TaskId = taskId, Data = new Payload { Data = result.Result } };
        await responseStream.WriteAsync(reply);
      }
    }

    public override async Task<Empty> WaitForCompletion(TaskFilter request, ServerCallContext context)
    {
      logger_.LogFunction();
      if (!request.ExcludedTaskIds.Any() &&
          !request.IncludedTaskIds.Any() &&
          string.IsNullOrEmpty(request.SubSessionId) &&
          !await tableStorage_.IsSessionClosedAsync(new SessionId
                                                    {
                                                      Session    = request.SessionId,
                                                      SubSession = request.SubSessionId
                                                    },
                                                    context.CancellationToken))
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                          "Session must be closed before witing for its completion"));
      }

      // TODO: optimize by filtering based on the task statuses
      // TODO: optimize by filtering based on the number of retries
      var taskIds = tableStorage_.ListTasksAsync(request, context.CancellationToken);
      await foreach (var taskId in taskIds)
      {
        bool completed;
        do
        {
          var tdata = await tableStorage_.ReadTaskAsync(taskId, context.CancellationToken);
          logger_.LogInformation("Task {id} has status {status}, retry : {retry}, max {max}",
                                 taskId,
                                 tdata.Status,
                                 tdata.Retries,
                                 tdata.Options.MaxRetries);
          completed = tdata.Status == TaskStatus.Completed ||
                      tdata.Status == TaskStatus.Canceled;
          if (!completed)
          {
            logger_.LogInformation("Task {id} is not completed. Will wait", taskId);
            await Task.Delay(tableStorage_.PollingDelay);
          }
        } while (!completed);

        logger_.LogInformation("Task {id} has been completed", taskId);
      }

      return new Empty();
      ;
    }
  }
}