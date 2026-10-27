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

using System.Collections.Generic;
using System.Linq;

using ArmoniK.Core.Base;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Tracks the handlers processing tasks on this agent, by queue message.
/// </summary>
/// <remarks>
///   A task can be received several times through different queue messages. Each handler is tracked under its own
///   message, so that disposing the handler of a duplicate message cannot hide a handler still processing the task.
/// </remarks>
public sealed class TaskProcessingRegistry
{
  private readonly object                                              lock_  = new();
  private readonly Dictionary<string, Dictionary<string, TaskHandler>> tasks_ = new();

  /// <summary>
  ///   Whether no handler is tracked.
  /// </summary>
  public bool IsEmpty
  {
    get
    {
      lock (lock_)
      {
        return tasks_.Count == 0;
      }
    }
  }

  /// <summary>
  ///   Track the handler processing the given message.
  /// </summary>
  /// <param name="message">Queue message processed by the handler</param>
  /// <param name="handler">Handler processing the message</param>
  /// <returns>
  ///   False if a handler is already tracked for this message
  /// </returns>
  public bool TryAdd(IQueueMessageHandler message,
                     TaskHandler          handler)
  {
    lock (lock_)
    {
      if (!tasks_.TryGetValue(message.TaskId,
                              out var messages))
      {
        messages               = new Dictionary<string, TaskHandler>();
        tasks_[message.TaskId] = messages;
      }

      return messages.TryAdd(message.MessageId,
                             handler);
    }
  }

  /// <summary>
  ///   Stop tracking the handler of the given message.
  /// </summary>
  /// <remarks>
  ///   Nothing is removed if another handler is tracked for this message, as is the case when the given handler was
  ///   rejected by <see cref="TryAdd" />.
  /// </remarks>
  /// <param name="message">Queue message processed by the handler</param>
  /// <param name="handler">Handler to stop tracking</param>
  public void Remove(IQueueMessageHandler message,
                     TaskHandler          handler)
  {
    lock (lock_)
    {
      if (!tasks_.TryGetValue(message.TaskId,
                              out var messages))
      {
        return;
      }

      // A handler rejected by TryAdd must not remove the handler tracked for the same message
      if (!messages.TryGetValue(message.MessageId,
                                out var tracked) || !ReferenceEquals(tracked,
                                                                     handler))
      {
        return;
      }

      messages.Remove(message.MessageId);

      if (messages.Count == 0)
      {
        tasks_.Remove(message.TaskId);
      }
    }
  }

  /// <summary>
  ///   Copy of the ids of the messages being processed, by task id.
  /// </summary>
  /// <returns>
  ///   The ids of the messages being processed, by task id
  /// </returns>
  public IReadOnlyDictionary<string, string[]> Snapshot()
  {
    lock (lock_)
    {
      return tasks_.ToDictionary(pair => pair.Key,
                                 pair => pair.Value.Keys.ToArray());
    }
  }

  /// <summary>
  ///   Copy of the tracked handlers.
  /// </summary>
  /// <returns>
  ///   The tracked handlers
  /// </returns>
  public TaskHandler[] Handlers()
  {
    lock (lock_)
    {
      return tasks_.Values.SelectMany(messages => messages.Values)
                   .ToArray();
    }
  }
}
