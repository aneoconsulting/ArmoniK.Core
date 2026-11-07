// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory queue, implementing both <see cref="IPushQueueStorage" /> and <see cref="IPullQueueStorage" />.
/// </summary>
/// <remarks>
///   <para>
///     There is one queue per partition. Messages are pulled by decreasing priority, then in the order they were pushed.
///   </para>
///   <para>
///     A pulled message is invisible until its handler is disposed. Depending on
///     <see cref="IQueueMessageHandler.Status" />
///     when it is disposed, it is:
///     <list type="bullet">
///       <item>
///         <see cref="QueueMessageStatus.Waiting" />, <see cref="QueueMessageStatus.Running" />,
///         <see cref="QueueMessageStatus.Failed" />: put back at its place at the front of the queue;
///       </item>
///       <item><see cref="QueueMessageStatus.Postponed" />: put back at the end of the queue;</item>
///       <item>
///         <see cref="QueueMessageStatus.Processed" />, <see cref="QueueMessageStatus.Cancelled" />,
///         <see cref="QueueMessageStatus.Poisonous" />: removed.
///       </item>
///     </list>
///   </para>
///   <para>
///     As with other queues, the same task can be pushed several times: each push is a distinct message.
///   </para>
/// </remarks>
public class QueueStorage : IPullQueueStorage, IPushQueueStorage
{
  private readonly Lock lock_ = new();

  private readonly Dictionary<string, PriorityQueue<QueuedMessage, (int priority, long order)>> partitions_ = new();

  private long order_;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public int MaxPriority
    => 100;

  /// <inheritdoc />
  public IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string            partitionId,
                                                                  int               nbMessages,
                                                                  CancellationToken cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();

    var messages = new List<IQueueMessageHandler>();

    lock (lock_)
    {
      if (partitions_.TryGetValue(partitionId,
                                  out var queue))
      {
        while (messages.Count < nbMessages && queue.TryDequeue(out var message,
                                                               out _))
        {
          messages.Add(new MessageHandler(this,
                                          message)
                       {
                         ReceptionDateTime = DateTime.UtcNow,
                       });
        }
      }
    }

    return messages.ToAsyncEnumerable();
  }

  /// <inheritdoc />
  public Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                string                   partitionId,
                                CancellationToken        cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();

    var newMessages = messages.ToList();

    lock (lock_)
    {
      foreach (var message in newMessages)
      {
        Enqueue(new QueuedMessage(partitionId,
                                  message.TaskId,
                                  message.Options.Priority,
                                  ++order_));
      }
    }

    return Task.CompletedTask;
  }

  /// <summary>
  ///   Put a message in the queue of its partition. Must be called under the lock.
  /// </summary>
  /// <param name="message">Message to enqueue</param>
  private void Enqueue(QueuedMessage message)
  {
    if (!partitions_.TryGetValue(message.PartitionId,
                                 out var queue))
    {
      queue = new PriorityQueue<QueuedMessage, (int priority, long order)>();
      partitions_.Add(message.PartitionId,
                      queue);
    }

    // Lower elements are dequeued first: negate the priority so that the highest priority comes first,
    // then the lowest order, so that messages of the same priority come out in the order they were pushed.
    queue.Enqueue(message,
                  (-message.Priority, message.Order));
  }

  /// <summary>
  ///   Apply the status of a pulled message whose handler is disposed
  /// </summary>
  /// <param name="message">Message pulled from the queue</param>
  /// <param name="status">Status of the handler when it is disposed</param>
  private void Release(QueuedMessage      message,
                       QueueMessageStatus status)
  {
    lock (lock_)
    {
      switch (status)
      {
        case QueueMessageStatus.Waiting:
        case QueueMessageStatus.Running:
        case QueueMessageStatus.Failed:
          // Back at its place, at the front of the queue
          Enqueue(message);
          break;
        case QueueMessageStatus.Postponed:
          // Back at the end of the queue
          Enqueue(message with
                  {
                    Order = ++order_,
                  });
          break;
        case QueueMessageStatus.Processed:
        case QueueMessageStatus.Cancelled:
        case QueueMessageStatus.Poisonous:
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(status),
                                                status,
                                                $"Unknown {nameof(QueueMessageStatus)}");
      }
    }
  }

  /// <summary>
  ///   Message stored in the queue
  /// </summary>
  /// <param name="PartitionId">Partition of the message</param>
  /// <param name="TaskId">Task of the message</param>
  /// <param name="Priority">Priority of the message, higher is pulled first</param>
  /// <param name="Order">Order of the message among the messages of the same priority, lower is pulled first</param>
  private sealed record QueuedMessage(string PartitionId,
                                      string TaskId,
                                      int    Priority,
                                      long   Order);

  /// <summary>
  ///   Handler of a pulled message
  /// </summary>
  private sealed class MessageHandler : IQueueMessageHandler
  {
    private readonly QueuedMessage message_;
    private readonly QueueStorage  queue_;
    private          int           disposed_;

    public MessageHandler(QueueStorage  queue,
                          QueuedMessage message)
    {
      queue_   = queue;
      message_ = message;
    }

    /// <inheritdoc />
    [Obsolete("ArmoniK now manages loss of link with the queue")]
    public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

    /// <inheritdoc />
    public string MessageId
      => $"Message#{message_.Order}";

    /// <inheritdoc />
    public string TaskId
      => message_.TaskId;

    /// <inheritdoc />
    public QueueMessageStatus Status { get; set; } = QueueMessageStatus.Waiting;

    /// <inheritdoc />
    public DateTime ReceptionDateTime { get; init; }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
      // A handler releases its message only once
      if (Interlocked.Exchange(ref disposed_,
                               1) == 0)
      {
        queue_.Release(message_,
                       Status);
      }

      return ValueTask.CompletedTask;
    }
  }
}
