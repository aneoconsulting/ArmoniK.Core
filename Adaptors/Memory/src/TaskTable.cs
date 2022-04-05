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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = System.Collections.Generic.KeyNotFoundException;
using Output = ArmoniK.Core.Common.Storage.Output;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory;

public class TaskTable : ITaskTable
{
  private readonly ConcurrentDictionary<string, TaskData>                taskId2TaskData_;
  private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> dispatch2TaskIds_;
  private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> session2TaskIds_;

  public TaskTable(ConcurrentDictionary<string, TaskData> task2TaskData,
                   ConcurrentDictionary<string, ConcurrentQueue<string>> dispatch2TaskIds,
                   ConcurrentDictionary<string, ConcurrentQueue<string>> session2TaskId,
                   ILogger<TaskTable> logger)
  {
    taskId2TaskData_ = task2TaskData;
    dispatch2TaskIds_ = dispatch2TaskIds;
    session2TaskIds_ = session2TaskId;
    Logger = logger;
  }

  public TimeSpan PollingDelayMax { get; set; }
  public TimeSpan PollingDelayMin { get; set; }

  /// <inheritdoc />
  public Task CreateTasks(IEnumerable<TaskData> tasks, CancellationToken cancellationToken = default)
  {
    foreach (var taskData in tasks)
    {
      if (!taskId2TaskData_.TryAdd(taskData.TaskId,
                                   taskData))
        throw new ArmoniKException($"Tasks '{taskData.TaskId}' already exists");

      var dispatch = dispatch2TaskIds_.GetOrAdd(taskData.DispatchId,
                                                new ConcurrentQueue<string>());
      dispatch.Enqueue(taskData.TaskId);

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
  {
    try
    {
      return Task.FromResult(taskId2TaskData_[taskId].DispatchId);
    }
    catch (KeyNotFoundException)
    {
      throw new ArmoniKException($"key {taskId} not found");
    }
  }

  /// <inheritdoc />
  public Task<IList<string>> GetTaskAncestorDispatchIds(string taskId, CancellationToken cancellationToken = default)
  {
    try
    {
      return Task.FromResult(taskId2TaskData_[taskId].AncestorDispatchIds);
    }
    catch (KeyNotFoundException)
    {
      throw new ArmoniKException($"key {taskId} not found");
    }
  }

  /// <inheritdoc />
  public Task ChangeTaskDispatch(string oldDispatchId, string newDispatchId, CancellationToken cancellationToken)
  {
    if (!dispatch2TaskIds_.ContainsKey(oldDispatchId))
      throw new ArmoniKException($"Key '{oldDispatchId}' not found");

    while ( dispatch2TaskIds_[oldDispatchId].TryDequeue(out var taskId) )
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
      var dispatch = dispatch2TaskIds_.GetOrAdd(newDispatchId,
                                                new ConcurrentQueue<string>());
      dispatch.Enqueue(taskId);
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
                                   if ((status is not TaskStatus.Canceling) && (data.Status is TaskStatus.Failed or TaskStatus.Canceled or TaskStatus.Completed))
                                     throw new ArmoniKException("the task is in a final state ant its status cannot change anymore");

                                   if ((data.Status == status) || (data.Status is TaskStatus.Failed or TaskStatus.Canceled or TaskStatus.Completed))
                                     return data;

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
  {
    if (filter.Included != null &&
        filter.Included.Statuses.Contains(TaskStatus.Completed) |
        filter.Included.Statuses.Contains(TaskStatus.Failed) |
        filter.Included.Statuses.Contains(TaskStatus.Canceled))
    {
      throw new ArmoniKException($"The given TaskFilter contains a terminal state or isn't initialized properly");
    }

    var result = await ListTasksAsync(filter,
                         cancellationToken).Select(taskId => UpdateAndCheckTaskStatus(taskId,
                                                                                      status))
                                           .CountAsync(checkTask => checkTask, cancellationToken);

    return result;
  }

  /// <inheritdoc />
  public Task<bool> IsTaskCancelledAsync(string taskId, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_[taskId].Status is TaskStatus.Canceling or TaskStatus.Canceled);

  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    if (!session2TaskIds_.ContainsKey(sessionId))
      throw new ArmoniKException($"Key '{sessionId}' not found");

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
                                   cancellationToken);
  }

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    if (!dispatch2TaskIds_.ContainsKey(dispatchId))
      throw new ArmoniKException($"Key '{dispatchId}' not found");

    var dispatchFilter = new TaskFilter
    {
      Session = new TaskFilter.Types.IdsRequest
      {
        Ids =
        {
          rootSessionId,
        },
      },
      Dispatch = new TaskFilter.Types.IdsRequest
      {
        Ids =
        {
          dispatchId,
        },
      },
    };

    await UpdateAllTaskStatusAsync(dispatchFilter,
                                   TaskStatus.Canceling,
                                   cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
    => await ListTasksAsync(filter,
                            cancellationToken).Select(taskId => taskId2TaskData_[taskId].Status)
                                              .GroupBy(status => status)
                                              .SelectAwait(async grouping => new TaskStatusCount(grouping.Key,
                                                                                                 await grouping.CountAsync(cancellationToken)))
                                              .ToListAsync(cancellationToken);

  /// <inheritdoc />
  public async Task<int> CountAllTasksAsync(TaskStatus status, CancellationToken cancellationToken = default)
  {
    var count       = 0;

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
                                  cancellationToken).CountAsync(cancellationToken);
    }

    return count;
  }

  /// <inheritdoc />
  public Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_.Remove(id,out _));

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
  public async Task SetTaskSuccessAsync(string taskId, CancellationToken cancellationToken)
    => await UpdateTaskStatusAsync(taskId,  TaskStatus.Completed, cancellationToken);

  /// <inheritdoc />
  public async Task SetTaskErrorAsync(string   taskId, string            errorDetail, CancellationToken cancellationToken)
  {
    using var _ = Logger.LogFunction();

    var taskOutput = new Output(Error: errorDetail,
                               Success: false);

    Logger.LogDebug("update task {taskId} to output {output}",
                    taskId,
                    taskOutput);
    /* A Task that errors is conceptually a  completed task,
     * the error is reported and detailed in its Output*/
    await UpdateTaskStatusAsync(taskId, TaskStatus.Completed, cancellationToken);
  }

  /// <inheritdoc />
  public Task<Output> GetTaskOutput(string taskId, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_[taskId].Output);

  /// <inheritdoc />
  public Task<TaskStatus> GetTaskStatus(string taskId, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_[taskId].Status);

  /// <inheritdoc />
  public Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string taskId, CancellationToken cancellationToken = default)
    => Task.FromResult(taskId2TaskData_[taskId].ExpectedOutput as IEnumerable<string>);

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
