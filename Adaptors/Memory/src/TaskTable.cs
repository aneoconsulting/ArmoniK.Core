// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Task = System.Threading.Tasks.Task;
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
    if (taskId2TaskData_.ContainsKey(taskId))
    {
      return Task.FromResult(taskId2TaskData_[taskId]);
    }

    throw new TaskNotFoundException($"Key '{taskId}' not found");
  }

  /// <inheritdoc />
  public Task UpdateTaskStatusAsync(string            id,
                                    TaskStatus        status,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(UpdateAndCheckTaskStatus(id,
                                                status));

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                                  TaskStatus        status,
                                                  CancellationToken cancellationToken = default)
  {
    if (filter.Included != null && (filter.Included.Statuses.Contains(TaskStatus.Completed) || filter.Included.Statuses.Contains(TaskStatus.Canceled)))
    {
      throw new ArmoniKException("The given TaskFilter contains a terminal state or isn't initialized properly");
    }

    var result = await ListTasksAsync(filter,
                                      cancellationToken)
                       .Select(taskId => UpdateAndCheckTaskStatus(taskId,
                                                                  status))
                       .CountAsync(checkTask => checkTask,
                                   cancellationToken)
                       .ConfigureAwait(false);

    return result;
  }

  /// <inheritdoc />
  public Task<bool> IsTaskCancelledAsync(string            taskId,
                                         CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    return Task.FromResult(taskId2TaskData_[taskId]
                             .Status is TaskStatus.Canceling or TaskStatus.Canceled);
  }

  /// <inheritdoc />
  public Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
  {
    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    taskId2TaskData_.AddOrUpdate(taskId,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) => data with
                                           {
                                             Status = TaskStatus.Processing,
                                             StartDate = DateTime.UtcNow,
                                             PodTtl = DateTime.UtcNow,
                                           });
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    if (!session2TaskIds_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found");
    }

    var sessionFilter = new TaskFilter
                        {
                          Session = new TaskFilter.Types.IdsRequest
                                    {
                                      Ids =
                                      {
                                        sessionId,
                                      },
                                    },
                        };

    await UpdateAllTaskStatusAsync(sessionFilter,
                                   TaskStatus.Canceling,
                                   cancellationToken)
      .ConfigureAwait(false);
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

  public Task<IEnumerable<TaskData>> ListTasksAsync(ListTasksRequest  request,
                                                    CancellationToken cancellationToken = default)
  {
    var queryable = taskId2TaskData_.AsQueryable()
                                    .Select(pair => pair.Value)
                                    .Where(request.Filter.ToTaskDataFilter());

    var ordered = request.Sort.Direction == ListTasksRequest.Types.OrderDirection.Asc
                    ? queryable.OrderBy(request.Sort.ToTaskDataField())
                    : queryable.OrderByDescending(request.Sort.ToTaskDataField());

    return Task.FromResult<IEnumerable<TaskData>>(ordered.Skip(request.Page * request.PageSize)
                                                         .Take(request.PageSize));
  }

  /// <inheritdoc />
  public Task SetTaskSuccessAsync(string            taskId,
                                  CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var taskOutput = new Output(Error: "",
                                Success: true);

    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                          taskId,
                          TaskStatus.Completed,
                          taskOutput);

    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    taskId2TaskData_.AddOrUpdate(taskId,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) =>
                                 {
                                   if (data.Status is TaskStatus.Canceled or TaskStatus.Completed)
                                   {
                                     throw new ArmoniKException("Task already in a final status");
                                   }

                                   return data with
                                          {
                                            Status = TaskStatus.Completed,
                                            Output = taskOutput,
                                          };
                                 });
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task SetTaskCanceledAsync(string            taskId,
                                   CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var taskOutput = new Output(Error: "",
                                Success: false);

    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                          taskId,
                          TaskStatus.Canceled,
                          taskOutput);

    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    taskId2TaskData_.AddOrUpdate(taskId,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) =>
                                 {
                                   if (data.Status is TaskStatus.Canceled or TaskStatus.Completed)
                                   {
                                     throw new ArmoniKException("Task already in a final status");
                                   }

                                   return data with
                                          {
                                            Status = TaskStatus.Canceled,
                                            Output = taskOutput,
                                          };
                                 });
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<bool> SetTaskErrorAsync(string            taskId,
                                      string            errorDetail,
                                      CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var taskOutput = new Output(Error: errorDetail,
                                Success: false);

    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                          taskId,
                          TaskStatus.Error,
                          taskOutput);

    if (!taskId2TaskData_.ContainsKey(taskId))
    {
      throw new TaskNotFoundException($"Key '{taskId}' not found");
    }

    var updated = true;
    taskId2TaskData_.AddOrUpdate(taskId,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) =>
                                 {
                                   if (data.Status is TaskStatus.Canceled or TaskStatus.Completed)
                                   {
                                     updated = false;
                                   }

                                   return data with
                                          {
                                            Status = TaskStatus.Error,
                                            Output = taskOutput,
                                          };
                                 });
    return Task.FromResult(updated);
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
  public Task<TaskData> AcquireTask(string            taskId,
                                    string            ownerPodId,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AddOrUpdate(taskId,
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
                                                               OwnerPodId = ownerPodId,
                                                             };
                                                    }));

  /// <inheritdoc />
  public Task<TaskData> ReleaseTask(string            taskId,
                                    string            ownerPodId,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.AddOrUpdate(taskId,
                                                    _ => throw new InvalidOperationException("The task does not exist."),
                                                    (_,
                                                     data) =>
                                                    {
                                                      if (data.OwnerPodId != ownerPodId)
                                                      {
                                                        throw new
                                                          InvalidOperationException($"The task {taskId} is acquired by {data.OwnerPodId}, but release is done by {ownerPodId}.");
                                                      }

                                                      return data with
                                                             {
                                                               OwnerPodId = "",
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
                                   taskData.PayloadId,
                                   taskData.ParentTaskIds,
                                   taskData.DataDependencies,
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

  public Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                        CancellationToken   cancellationToken = default)
  {
    var result = taskIds.Sum(taskId => UpdateTaskToSubmitted(taskId)
                                         ? 1
                                         : 0);
    return Task.FromResult(result);
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

  private bool UpdateAndCheckTaskStatus(string     id,
                                        TaskStatus status)
  {
    if (!taskId2TaskData_.ContainsKey(id))
    {
      throw new TaskNotFoundException($"Key '{id}' not found");
    }

    var updated = false;
    taskId2TaskData_.AddOrUpdate(id,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) =>
                                 {
                                   if (status is not TaskStatus.Canceling && data.Status is TaskStatus.Canceled or TaskStatus.Completed)
                                   {
                                     throw new ArmoniKException("the task is in a final state ant its status cannot change anymore");
                                   }

                                   if (data.Status == status || data.Status is TaskStatus.Canceled or TaskStatus.Completed)
                                   {
                                     return data;
                                   }

                                   updated = true;
                                   return data with
                                          {
                                            Status = status,
                                          };
                                 });
    return updated;
  }

  private bool UpdateTaskToSubmitted(string id)
  {
    var updated = false;
    taskId2TaskData_.AddOrUpdate(id,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_,
                                  data) =>
                                 {
                                   if (data.Status != TaskStatus.Creating)
                                   {
                                     return data;
                                   }

                                   updated = true;
                                   return data with
                                          {
                                            Status = TaskStatus.Submitted,
                                            SubmittedDate = DateTime.UtcNow,
                                          };
                                 });
    return updated;
  }
}
