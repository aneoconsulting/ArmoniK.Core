// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory;

public class TaskTable : ITaskTable
{
  private readonly ConcurrentDictionary<string, TaskData>                taskId2TaskData_  = new();
  private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> dispatch2TaskIds_ = new();
  private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> session2TaskIds_ = new();

  /// <inheritdoc />
  public TimeSpan PollingDelay { get; set; }

  /// <inheritdoc />
  public Task CreateTasks(IEnumerable<TaskData> tasks, CancellationToken cancellationToken = default)
  {
    foreach (var taskData in tasks)
    {
      if (!taskId2TaskData_.TryAdd(taskData.TaskId,
                                   taskData))
        throw new InvalidOperationException("Tasks already exists");

      foreach (var dispatchId in taskData.AncestorDispatchIds)
      {
        var dispatches = dispatch2TaskIds_.GetOrAdd(dispatchId,
                                                    new ConcurrentQueue<string>());
        dispatches.Enqueue(taskData.TaskId);
      }

      var session = session2TaskIds_.GetOrAdd(taskData.SessionId,
                                              new ConcurrentQueue<string>());
      session.Enqueue(taskData.TaskId);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<TaskData> ReadTaskAsync(string taskId, CancellationToken cancellationToken = default) 
    => Task.FromResult(taskId2TaskData_[taskId]);

  /// <inheritdoc />
  public Task<string> GetTaskDispatchId(string taskId, CancellationToken cancellationToken = default) 
    => Task.FromResult(taskId2TaskData_[taskId].DispatchId);

  /// <inheritdoc />
  public Task<IList<string>> GetTaskAncestorDispatchIds(string taskId, CancellationToken cancellationToken = default) 
    => Task.FromResult(taskId2TaskData_[taskId].AncestorDispatchIds);

  /// <inheritdoc />
  public Task ChangeTaskDispatch(string oldDispatchId, string newDispatchId, CancellationToken cancellationToken)
  {
    while (dispatch2TaskIds_[oldDispatchId].TryDequeue(out var taskId))
    {
      taskId2TaskData_.AddOrUpdate(taskId,
                                   _ => throw new InvalidOperationException("The task does not exist."),
                                   (_, data) => data.DispatchId == oldDispatchId
                                                  ? data with
                                                    {
                                                      DispatchId = newDispatchId,
                                                      AncestorDispatchIds = data.AncestorDispatchIds.Where(s => s != oldDispatchId).ToList(),
                                                    }
                                                  : data with
                                                    {
                                                      AncestorDispatchIds = data.AncestorDispatchIds.Where(s => s != oldDispatchId).ToList(),
                                                    });
      dispatch2TaskIds_[newDispatchId].Enqueue(taskId);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task UpdateTaskStatusAsync(string id, TaskStatus status, CancellationToken cancellationToken = default)
    => Task.FromResult(UpdateAndCheckTaskStatus(id,
                                                status));

  public bool UpdateAndCheckTaskStatus(string id, TaskStatus status)
  {
    var updated = false;
    taskId2TaskData_.AddOrUpdate(id,
                                 _ => throw new InvalidOperationException("The task does not exist."),
                                 (_, data) =>
                                 {
                                   if (data.Status == status)
                                     return data;

                                   if (data.Status is TaskStatus.Failed or TaskStatus.Canceled or TaskStatus.Completed)
                                     throw new InvalidOperationException("the task is in a final state ant its status cannot change anymore");

                                   updated = true;
                                   return data with
                                          {
                                            Status = status,
                                          };
                                 });
    return updated;
  }

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter filter, TaskStatus status, CancellationToken cancellationToken = default)
    => await ListTasksAsync(filter,
                            cancellationToken).Select(taskId => UpdateAndCheckTaskStatus(taskId,
                                                                                         status))
                                              .CountAsync(checkTask => checkTask,
                                                               cancellationToken);

  /// <inheritdoc />
  public Task<bool> IsTaskCancelledAsync(string taskId, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_[taskId].Status is TaskStatus.Canceling or TaskStatus.Canceled);

  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    => await UpdateAllTaskStatusAsync(new ()
                                {
                                  Session = new()
                                            {
                                              Ids =
                                              {
                                                sessionId,
                                              },
                                            },
                                },
                                TaskStatus.Canceling,
                                cancellationToken);

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
    => await UpdateAllTaskStatusAsync(new()
                                      {
                                        Dispatch = new()
                                                  {
                                                    Ids =
                                                    {
                                                      dispatchId,
                                                    },
                                                  },
                                      },
                                      TaskStatus.Canceling,
                                      cancellationToken);
  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
    => await ListTasksAsync(filter,
                            cancellationToken).Select(taskId => taskId2TaskData_[taskId].Status)
                                              .GroupBy(status => status)
                                              .SelectAwait(async grouping => new TaskStatusCount(grouping.Key,
                                                                                                 await grouping.CountAsync(cancellationToken)))
                                              .ToListAsync(cancellationToken);

  /// <inheritdoc />
  public Task<int> CountAllTasksAsync(TaskStatus status, CancellationToken cancellationToken = default)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken)
  {
    IEnumerable<string> rawList = filter.IdsCase switch
                                  {
                                    TaskFilter.IdsOneofCase.None =>
                                      throw new ArgumentException("Filter is not properly initialized. Either the session, the dispatch or the tasks are required",
                                                                  nameof(filter)),
                                    TaskFilter.IdsOneofCase.Session  => filter.Session.Ids.SelectMany(s => session2TaskIds_[s]).ToImmutableList(),
                                    TaskFilter.IdsOneofCase.Dispatch => filter.Dispatch.Ids.SelectMany(s => dispatch2TaskIds_[s]).ToImmutableList(),
                                    TaskFilter.IdsOneofCase.Task     => filter.Task.Ids,
                                    _                                => throw new ArgumentException("Filter is set to an unknown IdsCase."),
                                  };

    return rawList.Where(taskId => filter.StatusesCase switch
                            {
                              TaskFilter.StatusesOneofCase.None     => true,
                              TaskFilter.StatusesOneofCase.Included => filter.Included.Statuses.Contains(taskId2TaskData_[taskId].Status),
                              TaskFilter.StatusesOneofCase.Excluded => !filter.Excluded.Statuses.Contains(taskId2TaskData_[taskId].Status),
                              _                                     => throw new ArgumentException("Filter is set to an unknown StatusesCase."),
                            })
                  .ToAsyncEnumerable();
  }

  /// <inheritdoc />
  public Task SetTaskSuccessAsync(string taskId, CancellationToken cancellationToken)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public Task SetTaskErrorAsync(string   taskId, string            errorDetail, CancellationToken cancellationToken)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public Task<Output> GetTaskOutput(string taskId, CancellationToken cancellationToken = default)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public Task<TaskStatus> GetTaskStatus(string taskId, CancellationToken cancellationToken = default)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string taskId, CancellationToken cancellationToken = default)
  {
    throw new NotImplementedException();
  }

  /// <inheritdoc />
  public ILogger Logger { get; set; }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(true);
}
