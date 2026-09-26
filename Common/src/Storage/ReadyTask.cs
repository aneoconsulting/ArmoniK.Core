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
using System.Linq.Expressions;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Projection of a task that became ready to be enqueued: the queue message, plus what the queue
///   may need to place the task next to its data, read in the same query that finds the ready tasks.
/// </summary>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="SessionId">Session of the task</param>
/// <param name="Options">Task options</param>
/// <param name="PayloadId">Result holding the payload of the task</param>
/// <param name="DataDependencies">Results the task depends on, payload excluded</param>
public record ReadyTask(string        TaskId,
                        string        SessionId,
                        TaskOptions   Options,
                        string        PayloadId,
                        IList<string> DataDependencies)
{
  private static readonly IList<string> NoDependencies = Array.Empty<string>();

  /// <summary>
  ///   Selector translated by every task table implementation, reading the payload and the data dependencies.
  /// </summary>
  public static readonly Expression<Func<TaskData, ReadyTask>> Selector = data => new ReadyTask(data.TaskId,
                                                                                                   data.SessionId,
                                                                                                   data.Options,
                                                                                                   data.PayloadId,
                                                                                                   data.DataDependencies);

  /// <summary>
  ///   Selector translated by every task table implementation, reading only what the queue message needs:
  ///   <see cref="PayloadId" /> is empty and <see cref="DataDependencies" /> has no element.
  /// </summary>
  public static readonly Expression<Func<TaskData, ReadyTask>> MessageSelector = data => new ReadyTask(data.TaskId,
                                                                                                          data.SessionId,
                                                                                                          data.Options,
                                                                                                          "",
                                                                                                          NoDependencies);

  /// <summary>
  ///   The selector suited to the queue: the data dependencies are read only for a queue that uses them,
  ///   since they may be many for an aggregation task.
  /// </summary>
  /// <param name="pushQueueStorage">Queue in which the ready tasks are enqueued</param>
  /// <returns>
  ///   <see cref="Selector" /> if the queue uses the data dependencies, <see cref="MessageSelector" /> otherwise
  /// </returns>
  public static Expression<Func<TaskData, ReadyTask>> SelectorFor(IPushQueueStorage pushQueueStorage)
    => pushQueueStorage.UsesDataDependencies
         ? Selector
         : MessageSelector;

  /// <summary>
  ///   The queue message of the task.
  /// </summary>
  public MessageData ToMessage()
    => new(TaskId,
           SessionId,
           Options);
}
