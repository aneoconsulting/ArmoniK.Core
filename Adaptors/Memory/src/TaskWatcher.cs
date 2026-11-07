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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="ITaskWatcher" />, fed by the change events of <see cref="TaskTable" />.
/// </summary>
/// <remarks>
///   Events are produced in the order the writes were applied. A status update is produced only when the status actually
///   changes. Filters are evaluated on the new value of the task.
/// </remarks>
public class TaskWatcher : ITaskWatcher
{
  private readonly ILogger<TaskWatcher> logger_;
  private readonly TaskTable            taskTable_;

  private bool isInitialized_;

  /// <summary>
  ///   Creates a watcher of the given task table
  /// </summary>
  /// <param name="taskTable">Task table to watch</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public TaskWatcher(TaskTable            taskTable,
                     ILogger<TaskWatcher> logger)
  {
    taskTable_ = taskTable;
    logger_    = logger;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task<IAsyncEnumerable<NewTask>> GetNewTasks(Expression<Func<TaskData, bool>> filter,
                                                     CancellationToken                cancellationToken = default)
  {
    var compiled = filter.Compile();
    return Task.FromResult(Subscribe((previous,
                                      current) => previous is null && compiled(current)
                                                    ? new NewTask(current.SessionId,
                                                                  current.TaskId,
                                                                  current.InitialTaskId,
                                                                  current.PayloadId,
                                                                  current.ParentTaskIds.ToList(),
                                                                  current.ExpectedOutputIds.ToList(),
                                                                  current.DataDependencies.ToList(),
                                                                  current.RetryOfIds.ToList(),
                                                                  current.Status)
                                                    : null,
                                     cancellationToken));
  }

  /// <inheritdoc />
  public Task<IAsyncEnumerable<TaskStatusUpdate>> GetTaskStatusUpdates(Expression<Func<TaskData, bool>> filter,
                                                                       CancellationToken                cancellationToken = default)
  {
    var compiled = filter.Compile();
    return Task.FromResult(Subscribe((previous,
                                      current) => previous is not null && previous.Status != current.Status && compiled(current)
                                                    ? new TaskStatusUpdate(current.SessionId,
                                                                           current.TaskId,
                                                                           current.Status)
                                                    : null,
                                     cancellationToken));
  }

  private IAsyncEnumerable<TEvent> Subscribe<TEvent>(Func<TaskData?, TaskData, TEvent?> convert,
                                                     CancellationToken                  cancellationToken)
    where TEvent : class
    => EventSubscription.Subscribe(handler => taskTable_.TaskChanged += handler,
                                   handler => taskTable_.TaskChanged -= handler,
                                   convert,
                                   logger_,
                                   cancellationToken);
}
