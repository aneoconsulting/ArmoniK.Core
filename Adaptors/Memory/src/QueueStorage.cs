// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

public class QueueStorage : IPullQueueStorage, IPushQueueStorage
{
  private static readonly MessageHandler DefaultMessage = new();

  private static readonly KeyValuePair<MessageHandler, MessageHandler> DefaultPair = new(DefaultMessage,
                                                                                         DefaultMessage);

  private readonly ConcurrentDictionary<string, MessageHandler> id2Handlers_ = new();

  private readonly SortedList<MessageHandler, MessageHandler> queues_ = new(MessageComparer.Instance);

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
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    while (nbMessages > 0 && queues_.Any())
    {
      cancellationToken.ThrowIfCancellationRequested();
      var (message, _) = queues_.FirstOrDefault(pair => pair.Key.IsVisible = true,
                                                DefaultPair);
      if (ReferenceEquals(message,
                          DefaultMessage))
      {
        continue;
      }

      try
      {
        await message.Semaphore.WaitAsync(CancellationToken.None)
                     .ConfigureAwait(false);
        if (message.IsVisible)
        {
          message.IsVisible = false;
          yield return message;
        }
        else
        {
          continue;
        }
      }
      finally
      {
        message.Semaphore.Release();
      }

      nbMessages--;
    }
  }

  /// <inheritdoc />
  public Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                string                   _,
                                CancellationToken        cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();

    var messageHandlers = messages.Select(message => new MessageHandler
                                                     {
                                                       IsVisible         = true,
                                                       Priority          = message.Options.Priority,
                                                       TaskId            = message.TaskId,
                                                       CancellationToken = CancellationToken.None,
                                                       Status            = QueueMessageStatus.Waiting,
                                                       Queues            = queues_,
                                                       Handlers          = id2Handlers_,
                                                     });
    foreach (var messageHandler in messageHandlers)
    {
      queues_.Add(messageHandler,
                  messageHandler);
      if (!id2Handlers_.TryAdd(messageHandler.TaskId,
                               messageHandler))
      {
        throw new InvalidOperationException("Duplicate messageId found.");
      }
    }

    return Task.CompletedTask;
  }

  private class MessageHandler : IQueueMessageHandler
  {
    private static long _count;

    public int Priority { get; init; }

    public bool IsVisible { get; set; }

    public SemaphoreSlim Semaphore { get; } = new(1);

    public long Order { get; } = Interlocked.Increment(ref _count);

    public SortedList<MessageHandler, MessageHandler>   Queues   { get; init; } = new();
    public ConcurrentDictionary<string, MessageHandler> Handlers { get; init; } = new();

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
      switch (Status)
      {
        case QueueMessageStatus.Postponed:
          if (!Handlers.TryRemove(TaskId,
                                  out var handler))
          {
            throw new KeyNotFoundException();
          }

          if (handler.IsVisible)
          {
            throw new InvalidOperationException("Cannot change the status of a message that is visible.");
          }

          var newMessage = new MessageHandler
                           {
                             IsVisible         = true,
                             Priority          = handler.Priority,
                             TaskId            = handler.TaskId,
                             CancellationToken = CancellationToken.None,
                             Status            = QueueMessageStatus.Waiting,
                           };

          Queues.Add(newMessage,
                     newMessage);
          if (!Handlers.TryAdd(newMessage.TaskId,
                               newMessage))
          {
            throw new InvalidOperationException("Duplicate messageId found.");
          }

          if (!Queues.Remove(handler,
                             out _))
          {
            throw new KeyNotFoundException();
          }

          break;
        case QueueMessageStatus.Failed:
        case QueueMessageStatus.Running:
        case QueueMessageStatus.Waiting:
          if (!Handlers.TryRemove(TaskId,
                                  out var failedHandler))
          {
            throw new KeyNotFoundException();
          }

          failedHandler.IsVisible = true;
          break;
        case QueueMessageStatus.Processed:
        case QueueMessageStatus.Cancelled:
          if (!Handlers.TryRemove(TaskId,
                                  out var processedHandler))
          {
            throw new KeyNotFoundException();
          }

          if (processedHandler.IsVisible)
          {
            throw new InvalidOperationException("Cannot change the status of a message that is visible.");
          }

          if (!Queues.Remove(processedHandler,
                             out _))
          {
            throw new KeyNotFoundException();
          }

          break;
        case QueueMessageStatus.Poisonous:
          if (!Handlers.TryRemove(TaskId,
                                  out var rejectedHandler))
          {
            throw new KeyNotFoundException();
          }

          if (rejectedHandler.IsVisible)
          {
            throw new InvalidOperationException("Cannot change the status of a message that is visible.");
          }

          if (!Queues.Remove(rejectedHandler,
                             out _))
          {
            throw new KeyNotFoundException();
          }

          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(Status),
                                                Status,
                                                null);
      }

      // TODO: dispose semaphore ?
      // Semaphore.Dispose();
      return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

    /// <inheritdoc />
    public string MessageId
      => $"Message#{Order}";

    /// <inheritdoc />
    public string TaskId { get; init; } = "";

    /// <inheritdoc />
    public QueueMessageStatus Status { get; set; } = 0;

    /// <inheritdoc />
    public DateTime ReceptionDateTime { get; init; }
  }

  private class MessageComparer : IComparer<MessageHandler>
  {
    public static readonly IComparer<MessageHandler> Instance = new MessageComparer();

    public int Compare(MessageHandler? x,
                       MessageHandler? y)
    {
      if (ReferenceEquals(x,
                          y))
      {
        return 0;
      }

      if (y is null)
      {
        return 1;
      }

      if (x is null)
      {
        return -1;
      }

      var priorityComparison = x.Priority.CompareTo(y.Priority);
      return priorityComparison == 0
               ? x.Order.CompareTo(y.Order)
               : priorityComparison;
    }
  }
}
