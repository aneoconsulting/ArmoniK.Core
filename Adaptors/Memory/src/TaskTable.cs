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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="ITaskTable" />.
/// </summary>
/// <remarks>
///   Every public method runs under a single lock, so that each call is atomic, as a single document operation is in a
///   database. Stored values are never modified in place: an update replaces the stored value with a new one, so values
///   returned to callers are not affected by later writes.
/// </remarks>
public class TaskTable : ITaskTable
{
  /// <summary>
  ///   For each task, the id of the last call to <see cref="RemoveRemainingDataDependenciesAsync{T}" /> that removed one
  ///   of its dependencies. Used to report a ready task only to the call that removed its last dependency.
  /// </summary>
  private readonly Dictionary<string, Guid> lastDependencyUpdater_ = new();

  private readonly Lock                         lock_  = new();
  private readonly Dictionary<string, TaskData> tasks_ = new();

  private bool isInitialized_;

  /// <summary>
  ///   Creates an empty task table
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public TaskTable(ILogger<TaskTable> logger)
    => Logger = logger;

  /// <summary>
  ///   Number of handlers subscribed to <see cref="TaskChanged" />
  /// </summary>
  internal int TaskChangedHandlerCount
    => TaskChanged?.GetInvocationList()
                  .Length ?? 0;

  /// <inheritdoc />
  public TimeSpan PollingDelayMax { get; set; }

  /// <inheritdoc />
  public TimeSpan PollingDelayMin { get; set; }

  /// <inheritdoc />
  public Task CreateTasks(IEnumerable<TaskData> tasks,
                          CancellationToken     cancellationToken = default)
  {
    var newTasks = tasks.Select(Copy)
                        .ToList();

    lock (lock_)
    {
      // Validate the whole batch before inserting, so that a duplicate does not leave part of the batch inserted.
      var ids = new HashSet<string>();
      foreach (var task in newTasks)
      {
        if (tasks_.ContainsKey(task.TaskId) || !ids.Add(task.TaskId))
        {
          throw new TaskAlreadyExistsException($"Tasks '{task.TaskId}' already exists");
        }
      }

      foreach (var task in newTasks)
      {
        tasks_.Add(task.TaskId,
                   task);
        OnChanged(null,
                  task);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<T> ReadTaskAsync<T>(string                        taskId,
                                  Expression<Func<TaskData, T>> selector,
                                  CancellationToken             cancellationToken = default)
  {
    TaskData? taskData;
    lock (lock_)
    {
      tasks_.TryGetValue(taskId,
                         out taskData);
    }

    if (taskData is null)
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    return Task.FromResult(selector.Compile()
                                   .Invoke(taskData));
  }

  /// <inheritdoc />
  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      return Task.FromResult(tasks_.Values.Count(data => data.Status == status));
    }
  }

  /// <inheritdoc />
  public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                            CancellationToken                cancellationToken = default)
    => Task.FromResult<IEnumerable<TaskStatusCount>>(Snapshot(filter)
                                                     .GroupBy(taskData => taskData.Status)
                                                     .Select(grouping => new TaskStatusCount(grouping.Key,
                                                                                             grouping.Count()))
                                                     .ToList());

  /// <inheritdoc />
  public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
    => Task.FromResult<IEnumerable<PartitionTaskStatusCount>>(Snapshot(null)
                                                              .GroupBy(model => new
                                                                                {
                                                                                  model.Options.PartitionId,
                                                                                  model.Status,
                                                                                })
                                                              .Select(models => new PartitionTaskStatusCount(models.Key.PartitionId,
                                                                                                             models.Key.Status,
                                                                                                             models.Count()))
                                                              .ToList());

  /// <inheritdoc />
  public Task DeleteTaskAsync(string            id,
                              CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      if (!tasks_.Remove(id))
      {
        throw new TaskNotFoundException($"Key '{id}' not found");
      }

      lastDependencyUpdater_.Remove(id);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteTasksAsync(string            sessionId,
                               CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      var ids = tasks_.Values.Where(data => data.SessionId == sessionId)
                      .Select(data => data.TaskId)
                      .ToList();

      foreach (var id in ids)
      {
        tasks_.Remove(id);
        lastDependencyUpdater_.Remove(id);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteTasksAsync(ICollection<string> taskIds,
                               CancellationToken   cancellationToken = default)
  {
    lock (lock_)
    {
      foreach (var id in taskIds)
      {
        tasks_.Remove(id);
        lastDependencyUpdater_.Remove(id);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                         Expression<Func<TaskData, object?>> orderField,
                                                                         Expression<Func<TaskData, T>>       selector,
                                                                         bool                                ascOrder,
                                                                         int                                 page,
                                                                         int                                 pageSize,
                                                                         CancellationToken                   cancellationToken = default)
  {
    var matching = Snapshot(filter);
    var tasks = matching.OrderByKey(orderField,
                                    ascOrder)
                        .Skip(page * pageSize)
                        .Take(pageSize)
                        .Select(selector.Compile())
                        .ToList();

    return Task.FromResult<(IEnumerable<T> tasks, long totalCount)>((tasks, matching.Count));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                               Expression<Func<TaskData, T>>    selector,
                                               CancellationToken                cancellationToken = default)
    => Snapshot(filter)
       .Select(selector.Compile())
       .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<TaskData?> UpdateOneTask(string                            taskId,
                                       Expression<Func<TaskData, bool>>? filter,
                                       UpdateDefinition<TaskData>        updates,
                                       bool                              before,
                                       CancellationToken                 cancellationToken = default)
  {
    var compiledFilter = filter?.Compile();

    TaskData oldTaskData;
    TaskData newTaskData;

    lock (lock_)
    {
      if (!tasks_.TryGetValue(taskId,
                              out var current) || (compiledFilter is not null && !compiledFilter(current)))
      {
        return Task.FromResult<TaskData?>(null);
      }

      oldTaskData = current;
      newTaskData = Copy(new TaskData(current,
                                      updates));
      tasks_[taskId] = newTaskData;
      OnChanged(oldTaskData,
                newTaskData);
    }

    var result = before
                   ? oldTaskData
                   : newTaskData;

    return Task.FromResult<TaskData?>(result with
                                      {
                                        RemainingDataDependencies = new Dictionary<string, bool>(),
                                      });
  }

  /// <inheritdoc />
  public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                    UpdateDefinition<TaskData>       updates,
                                    CancellationToken                cancellationToken = default)
  {
    var compiledFilter = filter.Compile();

    lock (lock_)
    {
      // The filter is evaluated and the updates are applied under the same lock,
      // so that a task that no longer matches the filter is not updated.
      var matching = tasks_.Values.Where(compiledFilter)
                           .ToList();

      foreach (var current in matching)
      {
        var updated = Copy(new TaskData(current,
                                        updates));
        tasks_[current.TaskId] = updated;
        OnChanged(current,
                  updated);
      }

      return Task.FromResult<long>(matching.Count);
    }
  }

  /// <inheritdoc />
  public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                             ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                             bool ascOrder,
                                                                                             int page,
                                                                                             int pageSize,
                                                                                             CancellationToken cancellationToken = default)
  {
    var queryable = Snapshot(filter)
                    .AsQueryable()
                    .GroupBy(data => new Application(data.Options.ApplicationName,
                                                     data.Options.ApplicationNamespace,
                                                     data.Options.ApplicationVersion,
                                                     data.Options.ApplicationService))
                    .Select(group => group.Key);

    var ordered = orderFields.Count > 0
                    ? queryable.OrderByList(orderFields,
                                            ascOrder)
                    : queryable;

    return Task.FromResult<(IEnumerable<Application> applications, int totalCount)>((ordered.Skip(page * pageSize)
                                                                                            .Take(pageSize)
                                                                                            .ToList(), ordered.Count()));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>           taskIds,
                                                                     ICollection<string>           dependenciesToRemove,
                                                                     Expression<Func<TaskData, T>> selector,
                                                                     bool                          recovery          = false,
                                                                     CancellationToken             cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    if (dependenciesToRemove.Count == 0)
    {
      return AsyncEnumerable.Empty<T>();
    }

    var updater = Guid.NewGuid();
    var ready   = new List<TaskData>();

    lock (lock_)
    {
      foreach (var taskId in taskIds.Distinct())
      {
        if (!tasks_.TryGetValue(taskId,
                                out var current))
        {
          continue;
        }

        // Only tasks that still hold one of the dependencies are updated, so no call can update a task after its last
        // dependency has been removed: the call that removed it is the last updater of the task.
        if (dependenciesToRemove.Any(current.RemainingDataDependencies.ContainsKey))
        {
          var remaining = new Dictionary<string, bool>(current.RemainingDataDependencies);
          foreach (var dependency in dependenciesToRemove)
          {
            remaining.Remove(dependency);
          }

          var previous = current;
          current = current with
                    {
                      RemainingDataDependencies = remaining,
                    };
          tasks_[taskId]                 = current;
          lastDependencyUpdater_[taskId] = updater;
          OnChanged(previous,
                    current);
        }

        if (current.Status is not (TaskStatus.Creating or TaskStatus.Pending) || current.RemainingDataDependencies.Count != 0)
        {
          continue;
        }

        // Outside recovery, a ready task is reported only by the call that removed its last dependency.
        // Tasks created without remaining dependencies (eg: retries) have no updater and are reported by every call.
        // In recovery, the call that removed the last dependency may have failed before handling the task,
        // so every ready task is reported.
        if (recovery                                                             || !lastDependencyUpdater_.TryGetValue(taskId,
                                                                                                                        out var lastUpdater) || lastUpdater == updater)
        {
          ready.Add(current);
        }
      }
    }

    return ready.Select(selector.Compile())
                .ToAsyncEnumerable();
  }

  /// <inheritdoc />
  public ILogger Logger { get; set; }

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

  /// <summary>
  ///   Raised after each write, with the previous value of the task (null when it is created) and its new value.
  /// </summary>
  /// <remarks>
  ///   The event is raised under the table lock, so handlers see the writes in the order they were applied. Handlers must
  ///   not block nor call the table. An exception thrown by a handler is logged and does not reach the writer nor the other
  ///   handlers.
  /// </remarks>
  internal event Action<TaskData?, TaskData>? TaskChanged;

  /// <summary>
  ///   Raise <see cref="TaskChanged" />. Must be called under the lock.
  /// </summary>
  /// <param name="previous">Previous value of the task, null when it is created</param>
  /// <param name="current">New value of the task</param>
  private void OnChanged(TaskData? previous,
                         TaskData  current)
  {
    foreach (var handler in Delegate.EnumerateInvocationList(TaskChanged))
    {
      try
      {
        handler(previous,
                current);
      }
      catch (Exception e)
      {
        Logger.LogError(e,
                        "A handler of {Event} failed for task {TaskId}",
                        nameof(TaskChanged),
                        current.TaskId);
      }
    }
  }

  /// <summary>
  ///   Copy the tasks matching the filter, under the lock
  /// </summary>
  /// <param name="filter">Filter on the tasks, or null for all tasks</param>
  /// <returns>
  ///   The matching tasks, at the time of the call
  /// </returns>
  private List<TaskData> Snapshot(Expression<Func<TaskData, bool>>? filter)
  {
    var compiledFilter = filter?.Compile();
    lock (lock_)
    {
      return compiledFilter is null
               ? tasks_.Values.ToList()
               : tasks_.Values.Where(compiledFilter)
                       .ToList();
    }
  }

  /// <summary>
  ///   Copy the mutable members of a task, so that the stored value does not share them with the caller
  /// </summary>
  /// <param name="taskData">Task to copy</param>
  /// <returns>
  ///   The copy of the task
  /// </returns>
  private static TaskData Copy(TaskData taskData)
    => taskData with
       {
         RemainingDataDependencies = new Dictionary<string, bool>(taskData.RemainingDataDependencies),
       };
}
