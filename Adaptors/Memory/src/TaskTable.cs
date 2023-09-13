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
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

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
        throw new ArmoniKException($"Tasks '{taskData.TaskId}' already exists");
      }

      var session = session2TaskIds_.GetOrAdd(taskData.SessionId,
                                              new ConcurrentQueue<string>());
      session.Enqueue(taskData.TaskId);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<TaskData> ReadTaskAsync(string            taskId,
                                      CancellationToken cancellationToken = default)
  {
    if (taskId2TaskData_.TryGetValue(taskId,
                                     out var value))
    {
      return Task.FromResult(value);
    }

    throw new TaskNotFoundException($"Key '{taskId}' not found");
  }

  /// <inheritdoc />
  public Task<bool> IsTaskCancelledAsync(string            taskId,
                                         CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.TryGetValue(taskId,
                                      out var value))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    return Task.FromResult(value.Status is TaskStatus.Cancelling or TaskStatus.Cancelled);
  }

  /// <inheritdoc />
  public Task StartTask(TaskData          taskData,
                        CancellationToken cancellationToken = default)
  {
    taskId2TaskData_.AddOrUpdate(taskData.TaskId,
                                 _ => throw new TaskNotFoundException($"Key '{taskData.TaskId}' not found"),
                                 (_,
                                  data) =>
                                 {
                                   if (data.Status is TaskStatus.Error or TaskStatus.Completed or TaskStatus.Retried or TaskStatus.Cancelled)
                                   {
                                     throw new TaskAlreadyInFinalStateException($"{taskData.TaskId} is already in a final state : {data.Status}");
                                   }

                                   return data with
                                          {
                                            Status = TaskStatus.Processing,
                                            StartDate = taskData.StartDate,
                                            PodTtl = taskData.PodTtl,
                                          };
                                 });
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                                  CancellationToken cancellationToken = default)
    => await ListTasksAsync(filter,
                            cancellationToken)
             .Select(taskId => taskId2TaskData_[taskId]
                       .Status)
             .GroupBy(status => status)
             .SelectAwait(async grouping => new TaskStatusCount(grouping.Key,
                                                                await grouping.CountAsync(cancellationToken)
                                                                              .ConfigureAwait(false)))
             .ToListAsync(cancellationToken)
             .ConfigureAwait(false);

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
  public async Task<int> CountAllTasksAsync(TaskStatus        status,
                                            CancellationToken cancellationToken = default)
  {
    var count = 0;

    foreach (var session in session2TaskIds_.Keys)
    {
      var statusFilter = new TaskFilter
                         {
                           Included = new TaskFilter.Types.StatusesRequest
                                      {
                                        Statuses =
                                        {
                                          status,
                                        },
                                      },
                           Session = new TaskFilter.Types.IdsRequest
                                     {
                                       Ids =
                                       {
                                         session,
                                       },
                                     },
                         };

      count += await ListTasksAsync(statusFilter,
                                    cancellationToken)
                     .CountAsync(cancellationToken)
                     .ConfigureAwait(false);
    }

    return count;
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

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                                 CancellationToken cancellationToken = default)
  {
    IEnumerable<string> rawList = filter.IdsCase switch
                                  {
                                    TaskFilter.IdsOneofCase.None =>
                                      throw new ArgumentException("Filter is not properly initialized. Either the session or the tasks are required",
                                                                  nameof(filter)),
                                    TaskFilter.IdsOneofCase.Session => filter.Session.Ids.SelectMany(s => session2TaskIds_[s])
                                                                             .ToImmutableList(),
                                    TaskFilter.IdsOneofCase.Task => filter.Task.Ids,
                                    _                            => throw new ArgumentException("Filter is set to an unknown IdsCase."),
                                  };

    return rawList.Where(taskId => filter.StatusesCase switch
                                   {
                                     TaskFilter.StatusesOneofCase.None => true,
                                     TaskFilter.StatusesOneofCase.Included => filter.Included.Statuses.Contains(taskId2TaskData_[taskId]
                                                                                                                  .Status),
                                     TaskFilter.StatusesOneofCase.Excluded => !filter.Excluded.Statuses.Contains(taskId2TaskData_[taskId]
                                                                                                                   .Status),
                                     _ => throw new ArgumentException("Filter is set to an unknown StatusesCase."),
                                   })
                  .ToAsyncEnumerable();
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
  public Task<IEnumerable<T>> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                                Expression<Func<TaskData, T>>    selector,
                                                CancellationToken                cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AsQueryable()
                                       .Select(pair => pair.Value)
                                       .Where(filter)
                                       .Select(selector)
                                       .AsEnumerable());

  public Task<TaskData> UpdateOneTask(string                                                                        taskId,
                                      ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                      CancellationToken                                                             cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AddOrUpdate(taskId,
                                                    _ => throw new TaskNotFoundException($"Key '{taskId}' not found"),
                                                    (_,
                                                     data) => new TaskData(data,
                                                                           updates)));

  public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>                                              filter,
                                    ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                    CancellationToken                                                             cancellationToken = default)
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

  public Task RemoveRemainingDataDependenciesAsync(ICollection<string> taskIds,
                                                   ICollection<string> dependenciesToRemove,
                                                   CancellationToken   cancellationToken = default)
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

                                     foreach (var dep in dependenciesToRemove.Select(TaskData.EscapeKey))
                                     {
                                       remainingDep.Remove(dep);
                                     }

                                     return data with
                                            {
                                              RemainingDataDependencies = remainingDep,
                                            };
                                   });
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<Output> GetTaskOutput(string            taskId,
                                    CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    return Task.FromResult(taskId2TaskData_[taskId]
                             .Output);
  }

  /// <inheritdoc />
  public Task<TaskData> AcquireTask(TaskData          taskData,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AddOrUpdate(taskData.TaskId,
                                                    _ => throw new InvalidOperationException("The task does not exist."),
                                                    (_,
                                                     data) =>
                                                    {
                                                      if (data.OwnerPodId != "")
                                                      {
                                                        return data;
                                                      }

                                                      return data with
                                                             {
                                                               OwnerPodId = taskData.OwnerPodId,
                                                               OwnerPodName = taskData.OwnerPodName,
                                                               ReceptionDate = taskData.ReceptionDate,
                                                               AcquisitionDate = DateTime.UtcNow,
                                                             };
                                                    }));

  /// <inheritdoc />
  public Task<TaskData> ReleaseTask(TaskData          taskData,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AddOrUpdate(taskData.TaskId,
                                                    _ => throw new InvalidOperationException("The task does not exist."),
                                                    (_,
                                                     data) =>
                                                    {
                                                      if (data.OwnerPodId != taskData.OwnerPodId)
                                                      {
                                                        throw new
                                                          InvalidOperationException($"The task {taskData.TaskId} is acquired by {data.OwnerPodId}, but release is done by {taskData.OwnerPodId}.");
                                                      }

                                                      return data with
                                                             {
                                                               OwnerPodId = "",
                                                               OwnerPodName = "",
                                                               AcquisitionDate = null,
                                                               ReceptionDate = null,
                                                             };
                                                    }));

  /// <inheritdoc />
  public Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskIds,
                                                                            CancellationToken   cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.Where(tdm => taskIds.Contains(tdm.Key))
                                       .Select(model => new GetTaskStatusReply.Types.IdStatus
                                                        {
                                                          Status = model.Value.Status,
                                                          TaskId = model.Value.TaskId,
                                                        }));

  public IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                              CancellationToken   cancellationToken = default)
    => taskId2TaskData_.Where(pair => taskIds.Contains(pair.Key))
                       .Select(pair => (pair.Key, pair.Value.ExpectedOutputIds as IEnumerable<string>))
                       .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                    CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    return Task.FromResult(taskId2TaskData_[taskId]
                             .ParentTaskIds as IEnumerable<string>);
  }

  /// <inheritdoc />
  public Task<string> RetryTask(TaskData          taskData,
                                CancellationToken cancellationToken = default)
  {
    var newTaskId = taskData.InitialTaskId + $"###{taskData.RetryOfIds.Count + 1}";
    var newTaskRetryOfIds = new List<string>(taskData.RetryOfIds)
                            {
                              taskData.TaskId,
                            };
    var newTaskData = new TaskData(taskData.SessionId,
                                   newTaskId,
                                   "",
                                   "",
                                   taskData.PayloadId,
                                   taskData.ParentTaskIds,
                                   taskData.DataDependencies,
                                   taskData.RemainingDataDependencies,
                                   taskData.ExpectedOutputIds,
                                   taskData.InitialTaskId,
                                   newTaskRetryOfIds,
                                   TaskStatus.Creating,
                                   "",
                                   taskData.Options,
                                   DateTime.UtcNow,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   new Output(false,
                                              ""));

    if (!taskId2TaskData_.TryAdd(newTaskId,
                                 newTaskData))
    {
      throw new ArmoniKException($"Tasks '{newTaskId}' already exists");
    }

    var session = session2TaskIds_.GetOrAdd(newTaskData.SessionId,
                                            new ConcurrentQueue<string>());
    session.Enqueue(newTaskId);

    return Task.FromResult(newTaskId);
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
