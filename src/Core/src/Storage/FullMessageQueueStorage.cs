// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

namespace ArmoniK.Core.Storage
{
  public class FullMessageQueueStorage : IQueueStorage
  {
    private readonly IQueueStorage                          queueStorage_;
    private readonly KeyValueStorage<TaskId, TaskOptions>   taskOptionsStorage_;
    private readonly KeyValueStorage<TaskId, Payload>       taskPayloadStorage_;
    private readonly Func<QueueMessage, QueueMessage>       queueMessageOptimizer_;
    private readonly ConcurrentDictionary<string, TaskId> idsMappingTaskIds_ = new();

    public FullMessageQueueStorage(IQueueStorage                        queueStorage,
                                   KeyValueStorage<TaskId, TaskOptions> taskOptionsStorage,
                                   KeyValueStorage<TaskId, Payload>     taskPayloadStorage,
                                   Func<QueueMessage, QueueMessage>     queueMessageOptimizer)
    {
      queueStorage_          = queueStorage;
      taskOptionsStorage_    = taskOptionsStorage;
      taskPayloadStorage_    = taskPayloadStorage;
      queueMessageOptimizer_ = queueMessageOptimizer;
    }

    private Task<TaskOptions> GetTaskOptionsAsync(TaskId taskId, CancellationToken cancellationToken)
      => taskOptionsStorage_.TryGetValuesAsync(taskId, cancellationToken);

    private Task<Payload> GetTaskPayloadAsync(TaskId taskId, CancellationToken cancellationToken)
      => taskPayloadStorage_.TryGetValuesAsync(taskId, cancellationToken);

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
    public async Task<QueueMessage> ReadAsync(string id, CancellationToken cancellationToken = default)
    {
      var message = await queueStorage_.ReadAsync(id, cancellationToken);
      return await GetCompleteMessageAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
      if (!idsMappingTaskIds_.TryGetValue(id, out var taskId))
      {
        var message = await queueStorage_.ReadAsync(id, cancellationToken);
        taskId = message.TaskId;
      }

      await Task.WhenAll(taskOptionsStorage_.TryDeleteAsync(taskId, cancellationToken),
                         taskPayloadStorage_.TryDeleteAsync(taskId, cancellationToken),
                         queueStorage_.DeleteAsync(id, cancellationToken));
    }


    /// <inheritdoc />
    public Task<bool> ModifyVisibilityAsync(string id, DateTime deadline, CancellationToken cancellationToken = default)
      => queueStorage_.ModifyVisibilityAsync(id, deadline, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<string> EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
                                                         CancellationToken         cancellationToken = default)
    {
      var optimizedMessages = messages.Select(message => (Origin: message, Optimized: queueMessageOptimizer_(message)))
                                      .ToList();

      var enqueueTask = queueStorage_.EnqueueMessagesAsync(optimizedMessages.Select(tuple => tuple.Optimized), cancellationToken);

      var storeTasks = optimizedMessages.Select(async tuple =>
                                                {
                                                  var optionTask = tuple.Optimized.HasTaskOptions
                                                                     ? Task.CompletedTask
                                                                     : taskOptionsStorage_.AddOrUpdateAsync(tuple.Optimized.TaskId,
                                                                                                            tuple.Origin.TaskOptions,
                                                                                                            cancellationToken);
                                                  var payloadTask = tuple.Optimized.HasTaskPayload
                                                                      ? Task.CompletedTask
                                                                      : taskPayloadStorage_.AddOrUpdateAsync(tuple.Optimized.TaskId,
                                                                                                             tuple.Origin.Payload,
                                                                                                             cancellationToken);
                                                  await Task.WhenAll(optionTask, payloadTask);
                                                  return tuple.Optimized.TaskId;
                                                })
                                        .ToList()
                                        .ToAsyncEnumerable();




      return enqueueTask.ZipAwait(storeTasks, async (s, task) =>
                                              {
                                                var taskId = await task;
                                                idsMappingTaskIds_.AddOrUpdate(s, taskId, (_, _) => taskId);
                                                return s;
                                              });
    }
  }
}
