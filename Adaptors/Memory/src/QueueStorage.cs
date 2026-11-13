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
using System.Runtime.CompilerServices;
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
  /// <summary>
  ///   Default time a pull waits for a message when the queue is empty
  /// </summary>
  public static readonly TimeSpan DefaultPullWaitTime = TimeSpan.FromSeconds(1);

  private readonly Lock lock_ = new();

  private readonly Dictionary<string, PriorityQueue<QueuedMessage, (int priority, long order)>> partitions_ = new();

  private readonly TimeSpan pullWaitTime_;

  /// <summary>
  ///   Completed when messages are put in the queue, then replaced. Used to wake up the pulls waiting on an empty queue.
  /// </summary>
  private TaskCompletionSource enqueued_ = new(TaskCreationOptions.RunContinuationsAsynchronously);

  private long order_;

  /// <summary>
  ///   Creates an empty queue
  /// </summary>
  public QueueStorage()
    : this(DefaultPullWaitTime)
  {
  }

  /// <summary>
  ///   Creates an empty queue
  /// </summary>
  /// <param name="pullWaitTime">Time a pull waits for a message when the queue is empty</param>
  internal QueueStorage(TimeSpan pullWaitTime)
    => pullWaitTime_ = pullWaitTime;

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
  /// <remarks>
  ///   When the queue of the partition is empty, the pull waits until messages are pushed or released, for at most the
  ///   pull wait time, as other queues do: pollers calling it in a loop do not spin on an empty queue.
  ///   Messages pulled but not enumerated, when the enumeration stops early, are put back in the queue.
  /// </remarks>
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();

    var messages = TryDequeue(partitionId,
                              nbMessages,
                              out var enqueued);

    if (messages.Count == 0)
    {
      try
      {
        await enqueued.WaitAsync(pullWaitTime_,
                                 cancellationToken)
                      .ConfigureAwait(false);
      }
      catch (TimeoutException)
      {
        // No message pushed in time: the pull returns nothing
      }
      catch (OperationCanceledException)
      {
        yield break;
      }

      messages = TryDequeue(partitionId,
                            nbMessages,
                            out _);
    }

    var handedOut = 0;
    try
    {
      foreach (var message in messages)
      {
        handedOut++;
        yield return message;
      }
    }
    finally
    {
      // Put back the messages that were not handed out, when the enumeration stops early
      for (var i = handedOut; i < messages.Count; i++)
      {
        await messages[i]
              .DisposeAsync()
              .ConfigureAwait(false);
      }
    }
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
        var order = ++order_;
        Enqueue(new QueuedMessage($"Message#{order}",
                                  partitionId,
                                  message.TaskId,
                                  message.Options.Priority,
                                  order));
      }

      SignalEnqueued();
    }

    return Task.CompletedTask;
  }

  /// <summary>
  ///   Take the available messages of a partition
  /// </summary>
  /// <param name="partitionId">Partition of the messages</param>
  /// <param name="nbMessages">Maximal number of messages to take</param>
  /// <param name="enqueued">Task completed when messages are put in the queue after this call</param>
  /// <returns>
  ///   The handlers of the messages taken, possibly none
  /// </returns>
  private List<MessageHandler> TryDequeue(string   partitionId,
                                          int      nbMessages,
                                          out Task enqueued)
  {
    var messages = new List<MessageHandler>();

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

      enqueued = enqueued_.Task;
    }

    return messages;
  }

  /// <summary>
  ///   Wake up the pulls waiting for messages. Must be called under the lock.
  /// </summary>
  private void SignalEnqueued()
  {
    var enqueued = enqueued_;
    enqueued_ = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    enqueued.TrySetResult();
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
          SignalEnqueued();
          break;
        case QueueMessageStatus.Postponed:
          // Back at the end of the queue
          Enqueue(message with
                  {
                    Order = ++order_,
                  });
          SignalEnqueued();
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
  /// <param name="Id">
  ///   Identifier of the message, kept when the message is put back in the queue, as AMQP and NATS do: the pollster
  ///   compares it with the messages being processed to recognise a redelivered message
  /// </param>
  /// <param name="PartitionId">Partition of the message</param>
  /// <param name="TaskId">Task of the message</param>
  /// <param name="Priority">Priority of the message, higher is pulled first</param>
  /// <param name="Order">Order of the message among the messages of the same priority, lower is pulled first</param>
  private sealed record QueuedMessage(string Id,
                                      string PartitionId,
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
      => message_.Id;

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
