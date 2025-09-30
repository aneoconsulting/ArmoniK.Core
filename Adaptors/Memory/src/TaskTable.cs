// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

public class TaskTable : ITaskTable
{
  private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> session2TaskIds_;
  private readonly ConcurrentDictionary<string, TaskData>                taskId2TaskData_;

  private bool isInitialized_;

  public TaskTable(ConcurrentDictionary<string, TaskData>                task2TaskData,
                   ConcurrentDictionary<string, ConcurrentQueue<string>> session2TaskId,
                   ILogger<TaskTable>                                    logger)
  {
    taskId2TaskData_ = task2TaskData;
    session2TaskIds_ = session2TaskId;
    Logger           = logger;
  }

  public TimeSpan PollingDelayMax { get; set; }
  public TimeSpan PollingDelayMin { get; set; }

  /// <inheritdoc />
  public Task CreateTasks(IEnumerable<TaskData> tasks,
                          CancellationToken     cancellationToken = default)
  {
    foreach (var taskData in tasks)
    {
      if (!taskId2TaskData_.TryAdd(taskData.TaskId,
                                   taskData))
      {
        throw new TaskAlreadyExistsException($"Tasks '{taskData.TaskId}' already exists");
      }

      var session = session2TaskIds_.GetOrAdd(taskData.SessionId,
                                              new ConcurrentQueue<string>());
      session.Enqueue(taskData.TaskId);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<T> ReadTaskAsync<T>(string                        taskId,
                                  Expression<Func<TaskData, T>> selector,
                                  CancellationToken             cancellationToken = default)
  {
    if (taskId2TaskData_.TryGetValue(taskId,
                                     out var value))
    {
      return Task.FromResult(selector.Compile()
                                     .Invoke(value));
    }

    throw new TaskNotFoundException($"Key '{taskId}' not found");
  }

  /// <inheritdoc />
  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.Count(pair => pair.Value.Status == status));

  /// <inheritdoc />
  public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                            CancellationToken                cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AsQueryable()
                                       .Select(pair => pair.Value)
                                       .Where(filter)
                                       .GroupBy(taskData => taskData.Status)
                                       .Select(grouping => new TaskStatusCount(grouping.Key,
                                                                               grouping.Count()))
                                       .AsEnumerable());

  /// <inheritdoc />
  public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
  {
    var res = taskId2TaskData_.Values.AsQueryable()
                              .GroupBy(model => new
                                                {
                                                  model.Options.PartitionId,
                                                  model.Status,
                                                })
                              .Select(models => new PartitionTaskStatusCount(models.Key.PartitionId,
                                                                             models.Key.Status,
                                                                             models.Count()));

    return Task.FromResult(res as IEnumerable<PartitionTaskStatusCount>);
  }

  /// <inheritdoc />
  public Task DeleteTaskAsync(string            id,
                              CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.ContainsKey(id))
    {
      throw new TaskNotFoundException($"Key '{id}' not found");
    }

    return Task.FromResult(taskId2TaskData_.Remove(id,
                                                   out _));
  }

  public Task DeleteTasksAsync(string            sessionId,
                               CancellationToken cancellationToken = default)
  {
    var ids = taskId2TaskData_.Values.Where(data => data.SessionId == sessionId)
                              .Select(data => data.TaskId);

    foreach (var id in ids)
    {
      taskId2TaskData_.TryRemove(id,
                                 out _);
    }

    return Task.CompletedTask;
  }

  public Task DeleteTasksAsync(ICollection<string> taskIds,
                               CancellationToken   cancellationToken = default)
  {
    foreach (var taskId in taskIds)
    {
      taskId2TaskData_.TryRemove(taskId,
                                 out _);
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
    var queryable = taskId2TaskData_.AsQueryable()
                                    .Select(pair => pair.Value)
                                    .Where(filter);

    var ordered = ascOrder
                    ? queryable.OrderBy(orderField)
                    : queryable.OrderByDescending(orderField);

    return Task.FromResult<(IEnumerable<T> tasks, long totalCount)>((ordered.Skip(page * pageSize)
                                                                            .Take(pageSize)
                                                                            .Select(selector), ordered.Count()));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                               Expression<Func<TaskData, T>>    selector,
                                               CancellationToken                cancellationToken = default)
    => taskId2TaskData_.AsQueryable()
                       .Select(pair => pair.Value)
                       .Where(filter)
                       .Select(selector)
                       .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<TaskData?> UpdateOneTask(string                            taskId,
                                       Expression<Func<TaskData, bool>>? filter,
                                       UpdateDefinition<TaskData>        updates,
                                       bool                              before,
                                       CancellationToken                 cancellationToken = default)
  {
    if (!taskId2TaskData_.TryGetValue(taskId,
                                      out var taskData))
    {
      return Task.FromResult<TaskData?>(null);
    }

    if (filter is not null)
    {
      if (!filter.Compile()
                 .Invoke(taskData))
      {
        return Task.FromResult<TaskData?>(null);
      }
    }

    var newTaskData = taskId2TaskData_[taskId] = new TaskData(taskData,
                                                              updates);
    return Task.FromResult<TaskData?>(before
                                        ? taskData
                                        : newTaskData);
  }

  /// <inheritdoc />
  public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                    UpdateDefinition<TaskData>       updates,
                                    CancellationToken                cancellationToken = default)
  {
    long i = 0;
    foreach (var id in taskId2TaskData_.Values.AsQueryable()
                                       .Where(filter)
                                       .Select(data => data.TaskId))
    {
      i++;
      taskId2TaskData_.AddOrUpdate(id,
                                   _ => throw new TaskNotFoundException("Task not found"),
                                   (_,
                                    data) => new TaskData(data,
                                                          updates));
    }

    return Task.FromResult(i);
  }

  /// <inheritdoc />
  public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                             ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                             bool ascOrder,
                                                                                             int page,
                                                                                             int pageSize,
                                                                                             CancellationToken cancellationToken = default)
  {
    var queryable = taskId2TaskData_.AsQueryable()
                                    .Select(pair => pair.Value)
                                    .Where(filter)
                                    .GroupBy(data => new Application(data.Options.ApplicationName,
                                                                     data.Options.ApplicationNamespace,
                                                                     data.Options.ApplicationVersion,
                                                                     data.Options.ApplicationService))
                                    .Select(group => group.Key);

    var ordered = queryable.OrderByList(orderFields,
                                        ascOrder);

    return Task.FromResult<(IEnumerable<Application> tasks, int totalCount)>((ordered.Skip(page * pageSize)
                                                                                     .Take(pageSize), ordered.Count()));
  }

  public IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>           taskIds,
                                                                     ICollection<string>           dependenciesToRemove,
                                                                     Expression<Func<TaskData, T>> selector,
                                                                     CancellationToken             cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    foreach (var taskId in taskIds)
    {
      taskId2TaskData_.AddOrUpdate(taskId,
                                   _ => throw new TaskNotFoundException("The task does not exist."),
                                   (_,
                                    data) =>
                                   {
                                     var remainingDep = data.RemainingDataDependencies;

                                     foreach (var dep in dependenciesToRemove)
                                     {
                                       remainingDep.Remove(dep);
                                     }

                                     return data with
                                            {
                                              RemainingDataDependencies = remainingDep,
                                            };
                                   });
    }

    return taskId2TaskData_.AsQueryable()
                           .Select(pair => pair.Value)
                           .Where(data => taskIds.Contains(data.TaskId) && (data.Status == TaskStatus.Creating || data.Status == TaskStatus.Pending) &&
                                          data.RemainingDataDependencies.Count == 0)
                           .Select(selector)
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
}
