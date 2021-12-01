using ArmoniK.Compute.gRPC.V1;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Grpc.Core;

using System;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Control.Services
{
  public class ClientService : Core.gRPC.V1.ClientService.ClientServiceBase
  {
    private readonly ITableStorage                         tableStorage_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly ILogger<ClientService>                logger_;
    private readonly IQueueStorage                         queueStorage_;

    public ClientService(ITableStorage                         tableStorage,
                         IQueueStorage                         queueStorage,
                         KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                         KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                         ILogger<ClientService> logger)
    {
      tableStorage_       = tableStorage;
      taskResultStorage_  = taskResultStorage;
      taskPayloadStorage_ = taskPayloadStorage;
      logger_        = logger;
      queueStorage_       = queueStorage;
    }

    public override async Task<Empty> CancelSession(SessionId request, ServerCallContext context)
    {
      logger_.LogFunction();
      try
      {
        await tableStorage_.CancelSessionAsync(request, context.CancellationToken);
      }
      catch(KeyNotFoundException e)
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition, e.Message));
      }
      catch(Exception e)
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
      var tidsTask = request.TaskRequests.Select(async taskRequest =>
      {
        var options = taskRequest.TaskOptions;
        if (options == null)
        {
          options = await tableStorage_.GetDefaultTaskOption(taskRequest.SessionId, context.CancellationToken);
        }

        var (tid, isPayloadStored) = await tableStorage_.InitializeTaskCreation(taskRequest.SessionId,
                                                             options,
                                                             taskRequest.Payload,
                                                             context.CancellationToken);

        var payloadTask = isPayloadStored
                            ? ValueTask.CompletedTask
                            : new ValueTask(taskPayloadStorage_.AddOrUpdateAsync(tid, taskRequest.Payload, context.CancellationToken));

        var message = new QueueMessage("", tid);
        await queueStorage_.EnqueueAsync(message, options.Priority, context.CancellationToken); //TODO: use IEnumerable version

        await payloadTask;

          await tableStorage_.FinalizeTaskCreation(tid, context.CancellationToken);

        return tid;
      }).ToList();

      var tids = await Task.WhenAll(tidsTask);

      CreateTaskReply reply = new();
      reply.TaskIds.Add(tids);
      return reply;
    }

    public override async Task<Count> GetTasksCount(TaskFilter request, ServerCallContext context)
    {
      logger_.LogFunction();
      var count = await tableStorage_.CountTasksAsync(request, context.CancellationToken);
      return new Count { Value = count };
    }

    public override async Task ListTask(TaskFilter request, IServerStreamWriter<TaskId> responseStream, ServerCallContext context)
    {
      logger_.LogFunction();
      await foreach(var taskId in tableStorage_.ListTasksAsync(request, context.CancellationToken).WithCancellation(context.CancellationToken))
      {
        await responseStream.WriteAsync(taskId);
      }
    }

    public override async Task TryGetResult(TaskFilter request, IServerStreamWriter<SinglePayloadReply> responseStream, ServerCallContext context)
    {
      logger_.LogFunction();
      await foreach(var taskId in tableStorage_.ListTasksAsync(request, context.CancellationToken).WithCancellation(context.CancellationToken))
      {
        var result = await taskResultStorage_.TryGetValuesAsync(taskId, context.CancellationToken);
        var reply = new SinglePayloadReply { TaskId = taskId, Data = new Payload { Data = result.Result } };
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
            Session=request.SessionId, 
            SubSession=request.SubSessionId
          },context.CancellationToken ))
      {
        throw new RpcException(new Status(StatusCode.FailedPrecondition, "Session must be closed before witing for its completion"));
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
          completed = tdata.Status == TaskStatus.Completed ||
            tdata.Status == TaskStatus.Canceled ||
            tdata.Retries >= tdata.Options.MaxRetries;

          if (!completed)
          {
            await Task.Delay(tableStorage_.PollingDelay);
          }
        } while (!completed);

      }
      return new Empty(); ;
    }
  }
}
