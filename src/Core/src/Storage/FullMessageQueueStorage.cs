// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Utils;

namespace ArmoniK.Core.Storage
{
  public class FullMessageQueueStorage : IQueueStorage
  {
    private readonly IQueueStorage                                        queueStorage_;
    private readonly IKeyValueStorage<TaskId, TaskOptions>                taskOptionsStorage_;
    private readonly IKeyValueStorage<TaskId, Payload>                    taskPayloadStorage_;
    private readonly Func<QueueMessage, QueueMessage>                     queueMessageOptimizer_;
    private readonly ConcurrentDictionary<string, TaskId[]>               idsMappingTaskIds_      = new();

    public FullMessageQueueStorage(IQueueStorage                         queueStorage,
                                   IKeyValueStorage<TaskId, TaskOptions> taskOptionsStorage,
                                   IKeyValueStorage<TaskId, Payload>     taskPayloadStorage,
                                   Func<QueueMessage, QueueMessage>      queueMessageOptimizer)
    {
      queueStorage_          = queueStorage;
      taskOptionsStorage_    = taskOptionsStorage;
      taskPayloadStorage_    = taskPayloadStorage;
      queueMessageOptimizer_ = queueMessageOptimizer;
    }

    private async Task<TaskOptions> GetTaskOptionsAsync(TaskId taskId, CancellationToken cancellationToken)
      => (await (await taskOptionsStorage_.TryGetValuesAsync(new[] { taskId }, cancellationToken)).FirstAsync(cancellationToken)).Item2;

    private async Task<Payload> GetTaskPayloadAsync(TaskId taskId, CancellationToken cancellationToken)
      => (await (await taskPayloadStorage_.TryGetValuesAsync(new[] { taskId }, cancellationToken)).FirstAsync(cancellationToken)).Item2;

    private async Task<QueueMessage> GetCompleteMessageAsync(QueueMessage message, CancellationToken cancellationToken)
    {
      if (message.HasTaskOptions && message.HasTaskPayload)
        return message;

      Task<TaskOptions> optionTask = null;
      if (!message.HasTaskOptions)
      {
        optionTask = GetTaskOptionsAsync(message.TaskId, cancellationToken);
      }

      Task<Payload> payloadTask = null;
      if (!message.HasTaskPayload)
      {
        payloadTask = GetTaskPayloadAsync(message.TaskId, cancellationToken);
      }

      var options = optionTask is null ? message.TaskOptions : await optionTask;
      var payload = payloadTask is null ? message.Payload : await payloadTask;

      return new QueueMessage(message.MessageId,
                              message.TaskId,
                              true,
                              options,
                              true,
                              payload);
    }

    /// <inheritdoc />
    public async Task<QueueMessage> PullAsync(DateTime deadline, CancellationToken cancellationToken = default)
    {
      var message = await queueStorage_.PullAsync(deadline, cancellationToken);
      return await GetCompleteMessageAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteAsync(string id, CancellationToken cancellationToken = default)
      => Task.WhenAll(taskOptionsStorage_.TryDeleteAsync(idsMappingTaskIds_[id]),
                      taskPayloadStorage_.TryDeleteAsync(idsMappingTaskIds_[id]),
                      queueStorage_.DeleteAsync(id, cancellationToken));
  

    /// <inheritdoc />
    public Task ModifyDeadlineAsync(string id, DateTime deadline, CancellationToken cancellationToken = default)
      => queueStorage_.ModifyDeadlineAsync(id, deadline, cancellationToken);

    /// <inheritdoc />
    public async Task<string> EnqueueAsync(QueueMessage message, CancellationToken cancellationToken = default)
    {
      var queueMessage  = queueMessageOptimizer_(message);
      var queueSendTask = queueStorage_.EnqueueAsync(queueMessage, cancellationToken);

      var optionTask = queueMessage.HasTaskOptions
                         ? Task.CompletedTask
                         : taskOptionsStorage_.AddOrUpdateAsync(new[] { (message.TaskId, message.TaskOptions) });

      var payloadTask = queueMessage.HasTaskPayload
                          ? Task.CompletedTask
                          : taskPayloadStorage_.AddOrUpdateAsync(new[] { (message.TaskId, message.Payload) });

      var messageId = await queueSendTask;
      idsMappingTaskIds_[messageId] = new[] { message.TaskId };

      await Task.WhenAll(optionTask, payloadTask);
      return messageId;
    }
  }
}
