// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory;

public class TableStorage : ITableStorage
{
  private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SessionData>> sessions_ = new();

  private readonly ConcurrentDictionary<string, TaskData> tasks_ = new();

  private readonly ConcurrentDictionary<string, Dispatch> dispatchesPerKey_ = new();

  private readonly ConcurrentDictionary<string, Result> results_ = new();

  private readonly SemaphoreSlim dispatchSemaphore_ = new(1,
                                                         1);

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(true);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken) => Task.CompletedTask;

  /// <inheritdoc />
  public TimeSpan PollingDelay { get; } = TimeSpan.FromMilliseconds(50);

  /// <inheritdoc />
  public TimeSpan DispatchTimeToLive { get; } = TimeSpan.FromHours(1);

  /// <inheritdoc />
  public Task<CreateSessionReply> CreateSessionAsync(CreateSessionRequest sessionRequest, CancellationToken cancellationToken = default)
  {
    SessionData sessionData = sessionRequest.SessionTypeCase switch
                              {
                                CreateSessionRequest.SessionTypeOneofCase.Root => new()
                                                                                  {
                                                                                    SessionId          = sessionRequest.Root.Id,
                                                                                    DefaultTaskOptions = sessionRequest.Root.DefaultTaskOption,
                                                                                    ParentTaskId       = sessionRequest.Root.Id,
                                                                                  },
                                CreateSessionRequest.SessionTypeOneofCase.SubSession => new()
                                                                                        {
                                                                                          SessionId    = sessionRequest.SubSession.RootId,
                                                                                          ParentTaskId = sessionRequest.SubSession.ParentTaskId,
                                                                                        },
                                _ => throw new RpcException(new(StatusCode.InvalidArgument,
                                                                "Ill-formed request"))
                              };

    var subSessions = sessions_.GetOrAdd(sessionData.SessionId,
                                         _ => new());
    
    if (subSessions.TryAdd(sessionData.ParentTaskId,
                           sessionData))
    {
      if (sessionData.ParentTaskId != sessionData.SessionId)
      {
        var parentSubSession = subSessions[sessionData.ParentTaskId];
        parentSubSession.ChildrenSessions.Add(sessionData.SessionId);
        sessionData.AncestorSessions.Add(parentSubSession.SessionId);
        foreach (var ancestor in parentSubSession.AncestorSessions)
        {
          var ancestorSubSession = subSessions[ancestor];
          ancestorSubSession.ChildrenSessions.Add(sessionData.SessionId);
          sessionData.AncestorSessions.Add(ancestorSubSession.SessionId);
        }
      }

      var parent  = sessionData.ParentTaskId;
      var current = sessionData.SessionId;
      while (parent != current)
      {
        var parentData = subSessions[parent];
        parentData.ChildrenSessions.Add(sessionData.SessionId);
        current = parent;
        parent  = parentData.ParentTaskId;
      }

      return Task.FromResult(new CreateSessionReply()
                             {
                               Ok = new(),
                             });
    }

    return Task.FromResult(new CreateSessionReply()
                           {
                             Error = "A session has already been created with this session/parentId configuration",
                           });
  }


  /// <inheritdoc />
  public Task CancelSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default)
  {
    sessions_[sessionId.Session][sessionId.ParentTaskId].IsCancelled = true;
    foreach (var child in sessions_[sessionId.Session][sessionId.ParentTaskId].ChildrenSessions)
    {
      sessions_[sessionId.Session][child].IsCancelled = true;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<bool> IsSessionCancelledAsync(SessionId sessionId, CancellationToken cancellationToken = default)
    => Task.FromResult(sessions_[sessionId.Session][sessionId.ParentTaskId].IsCancelled);

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListSessionsAsync(CancellationToken cancellationToken = default)
    => sessions_.Keys.ToAsyncEnumerable();

  /// <inheritdoc />
  public Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    sessions_.TryRemove(sessionId,
                        out _);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<TaskOptions> GetDefaultTaskOption(string sessionId, string parentId, CancellationToken cancellationToken = default)
    => Task.FromResult(sessions_[sessionId][parentId].DefaultTaskOptions);

  /// <inheritdoc />
  public Task InitializeTaskCreation(string                                                session,
                                     string                                                parentTaskId,
                                     TaskOptions                                           options,
                                     IEnumerable<CreateSmallTaskRequest.Types.TaskRequest> requests,
                                     CancellationToken                                     cancellationToken = default)
  {
    var errors = new List<string>();
    foreach (var taskRequest in requests)
    {
      TaskData data = new()
                      {
                        Status       = TaskStatus.Creating,
                        Options      = options,
                        Payload      = taskRequest.Payload.ToByteArray(),
                        ParentTaskId = parentTaskId,
                        SessionId    = session,
                        TaskId       = taskRequest.Id,
                      };
      data.DataDependencies.AddRange(taskRequest.DataDependencies);
      if (tasks_.TryAdd(taskRequest.Id,
                        data))
      {
        foreach (var key in taskRequest.ExpectedOutputKeys)
        {
          var result = results_.GetOrAdd(key,
                          (_)=>new ()
                          {
                            Key               = key,
                            IsResultAvailable = false,
                            SessionId         = session,
                          });
          result.Owner = taskRequest.Id;
        }
      }
      else
      {
        errors.Add(taskRequest.Id);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<ITaskData> ReadTaskAsync(string id, CancellationToken cancellationToken = default)
    => Task.FromResult(tasks_[id] as ITaskData);

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
    => tasks_.Values
             .Where(task => filter.IdsCase switch
                            {
                              TaskFilter.IdsOneofCase.Known => filter.Known.TaskIds.Contains(task.TaskId) ||
                                                               filter.Known.TaskIds.Contains(task.ParentTaskId) ||
                                                               sessions_[task.SessionId][task.ParentTaskId].AncestorSessions
                                                                                                           .Intersect(filter.Known.TaskIds)
                                                                                                           .Any(),
                              TaskFilter.IdsOneofCase.Unknown => filter.Unknown.SessionId == task.SessionId &&
                                                                 !filter.Unknown.ExcludedTaskIds.Contains(task.TaskId),
                              _ => throw new ArgumentOutOfRangeException(nameof(filter)),
                            })
             .Where(task => filter.StatusesCase switch
                            {
                              TaskFilter.StatusesOneofCase.Included => filter.Included.IncludedStatuses.Contains(task.Status),
                              TaskFilter.StatusesOneofCase.Excluded => !filter.Excluded.IncludedStatuses.Contains(task.Status),
                              TaskFilter.StatusesOneofCase.None     => true,
                              _                                     => throw new ArgumentOutOfRangeException(nameof(filter)),
                            })
             .Select(task => task.TaskId)
             .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task UpdateTaskStatusAsync(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    tasks_[id].Status = status;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter filter, TaskStatus status, CancellationToken cancellationToken = default)
  {
    var count = 0;
    await foreach (var task in ListTasksAsync(filter,
                                              cancellationToken)
                    .WithCancellation(cancellationToken))
    {
      tasks_[task].Status = status;
      count++;
    }

    return count;
  }

  /// <inheritdoc />
  public Task<IEnumerable<(TaskStatus Status, int Count)>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
    => Task.FromResult(tasks_.Values
                             .Where(task => filter.IdsCase switch
                                            {
                                              TaskFilter.IdsOneofCase.Known => filter.Known.TaskIds.Contains(task.TaskId),
                                              TaskFilter.IdsOneofCase.Unknown => filter.Unknown.SessionId == task.SessionId &&
                                                                                 !filter.Unknown.ExcludedTaskIds.Contains(task.TaskId),
                                              _ => throw new ArgumentOutOfRangeException(nameof(filter)),
                                            })
                             .Where(task => filter.StatusesCase switch
                                            {
                                              TaskFilter.StatusesOneofCase.Included => filter.Included.IncludedStatuses.Contains(task.Status),
                                              TaskFilter.StatusesOneofCase.Excluded => !filter.Excluded.IncludedStatuses.Contains(task.Status),
                                              TaskFilter.StatusesOneofCase.None     => true,
                                              _                                     => throw new ArgumentOutOfRangeException(nameof(filter)),
                                            })
                             .GroupBy(task => task.Status)
                             .Select(tasks => (
                                                Status: tasks.Key,
                                                Count: tasks.Count()
                                              )));

  /// <inheritdoc />
  public Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
  {
    tasks_.Remove(id,
                  out _);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task<bool> TryAcquireDispatchAsync(string            dispatchId,
                                            string            taskId,
                                            DateTime          ttl,
                                            string            podId             = "",
                                            string            nodeId            = "",
                                            CancellationToken cancellationToken = default)
  {
    if (tasks_[taskId].Dispatch is not null && tasks_[taskId].Dispatch.TimeToLive > DateTime.UtcNow)
      return false;

    var dispatch = new Dispatch()
                   {
                     Id         = dispatchId,
                     TaskId     = taskId,
                     TimeToLive = ttl,
                     Attempt = 1,
                   };

    if (dispatchesPerKey_.TryAdd(dispatchId,
                                 dispatch))
    {
      if (tasks_[taskId].Dispatch is null || tasks_[taskId].Dispatch.TimeToLive < DateTime.UtcNow)
      {
        await dispatchSemaphore_.WaitAsync(cancellationToken);
        using var _ = Disposable.Create(() => dispatchSemaphore_.Release());
        if (tasks_[taskId].Dispatch is null || tasks_[taskId].Dispatch.TimeToLive < DateTime.UtcNow)
        {
          if (tasks_[taskId].Dispatch is not null)
          {
            ((Dispatch)tasks_[taskId].Dispatch).TimeToLive = DateTime.MinValue;

            dispatch.Attempt = tasks_[taskId].Dispatch.Attempt + 1;
          }

          tasks_[taskId].Dispatch = dispatch;
          return true;
        }
      }


      if (!dispatchesPerKey_.TryRemove(new(dispatchId,
                                           dispatch)))
      {
        throw new KeyNotFoundException();
      }
    }

    return false;
  }

  /// <inheritdoc />
  public Task DeleteDispatch(string id, CancellationToken cancellationToken = default)
  {
    dispatchesPerKey_.Remove(id,
                             out var dispatch);
    {
      if (dispatch != null)
        tasks_[dispatch.TaskId].Dispatch = null;
    }
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task UpdateDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    if (tasks_[dispatchesPerKey_[id].TaskId].Dispatch.Id == id)
    {
      dispatchesPerKey_[id].Statuses.Add(new(status,
                                             DateTime.UtcNow,
                                             string.Empty));
      tasks_[dispatchesPerKey_[id].TaskId].Status = status;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task ExtendDispatchTtl(string id, DateTime newTtl, CancellationToken cancellationToken = default)
  {
    var dispatch = dispatchesPerKey_[id];

    await dispatchSemaphore_.WaitAsync(cancellationToken);
    using var _ = Disposable.Create(() => dispatchSemaphore_.Release());

    if (tasks_[dispatch.TaskId].Dispatch.Id == id)
    {
      dispatch.TimeToLive = newTtl;
    }
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListDispatchAsync(string taskId, CancellationToken cancellationToken = default) 
    => dispatchesPerKey_.Keys.ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<IDispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default)
    => Task.FromResult(dispatchesPerKey_[dispatchId] as IDispatch);

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListResultsAsync(string sessionId, CancellationToken cancellationToken = default) 
    => results_.Keys.ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<IResult> GetResult(string sessionId, string key, CancellationToken cancellationToken = default) 
    => Task.FromResult(results_[key] as IResult);

  /// <inheritdoc />
  public Task SetResult(string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default)
  {
    var result = results_[key];
    if (result.Owner == ownerTaskId && !result.IsResultAvailable)
    {
      result.Data = smallPayload;
      result.IsResultAvailable = true;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResult(string session, string key, CancellationToken cancellationToken = default)
  {
    results_.Remove(key,
                    out _);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResults(string sessionId, CancellationToken cancellationToken = default)
  {
    var toDelete = results_.Values
                           .Where(result => result.SessionId == sessionId)
                           .Select(result => result.Key)
                           .ToList();
    foreach (var result in toDelete)
    {
      results_.TryRemove(result, out _);
    }

    return Task.CompletedTask;
  }
}
