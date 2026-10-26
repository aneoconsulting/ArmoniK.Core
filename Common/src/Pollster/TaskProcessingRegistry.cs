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

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Tracks the handlers processing tasks on this agent, by task id and by queue message id.
/// </summary>
/// <remarks>
///   A task can be received several times through different queue messages. Each handler is tracked under its own
///   message, so that disposing the handler of a duplicate message cannot hide a handler still processing the task.
/// </remarks>
/// <typeparam name="THandler">Type of the handlers that are tracked</typeparam>
public sealed class TaskProcessingRegistry<THandler>
  where THandler : class
{
  private readonly object                                           lock_  = new();
  private readonly Dictionary<string, Dictionary<string, THandler>> tasks_ = new();

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
  ///   Track the handler processing the given message of the given task.
  /// </summary>
  /// <param name="taskId">Id of the task</param>
  /// <param name="messageId">Id of the queue message</param>
  /// <param name="handler">Handler processing the message</param>
  /// <returns>
  ///   False if a handler is already tracked for this task and this message
  /// </returns>
  public bool TryAdd(string   taskId,
                     string   messageId,
                     THandler handler)
  {
    lock (lock_)
    {
      if (!tasks_.TryGetValue(taskId,
                              out var messages))
      {
        messages       = new Dictionary<string, THandler>();
        tasks_[taskId] = messages;
      }

      return messages.TryAdd(messageId,
                             handler);
    }
  }

  /// <summary>
  ///   Stop tracking the handler of the given message of the given task.
  /// </summary>
  /// <remarks>
  ///   Nothing is removed if another handler is tracked for this message, as is the case when the given handler was
  ///   rejected by <see cref="TryAdd" />.
  /// </remarks>
  /// <param name="taskId">Id of the task</param>
  /// <param name="messageId">Id of the queue message</param>
  /// <param name="handler">Handler to stop tracking</param>
  public void Remove(string   taskId,
                     string   messageId,
                     THandler handler)
  {
    lock (lock_)
    {
      if (!tasks_.TryGetValue(taskId,
                              out var messages)                                           || !messages.TryGetValue(messageId,
                                                                                                                   out var tracked) || !ReferenceEquals(tracked,
                                                                                                                                                        handler))
      {
        return;
      }

      messages.Remove(messageId);

      if (messages.Count == 0)
      {
        tasks_.Remove(taskId);
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
  public THandler[] Handlers()
  {
    lock (lock_)
    {
      return tasks_.Values.SelectMany(messages => messages.Values)
                   .ToArray();
    }
  }
}
