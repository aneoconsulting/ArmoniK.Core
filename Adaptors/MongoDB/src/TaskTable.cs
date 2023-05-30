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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <inheritdoc />
public class TaskTable : ITaskTable
{
  private readonly ActivitySource                                          activitySource_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider_;

  private bool isInitialized_;

  public TaskTable(SessionProvider                                         sessionProvider,
                   MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider,
                   ILogger<TaskTable>                                      logger,
                   ActivitySource                                          activitySource,
                   TableStorage                                            option)
  {
    sessionProvider_        = sessionProvider;
    taskCollectionProvider_ = taskCollectionProvider;
    Logger                  = logger;
    activitySource_         = activitySource;
    PollingDelayMin         = option.PollingDelayMin;
    PollingDelayMax         = option.PollingDelayMax;
  }

  public TimeSpan PollingDelayMin { get; set; }
  public TimeSpan PollingDelayMax { get; set; }

  /// <inheritdoc />
  public async Task CreateTasks(IEnumerable<TaskData> tasks,
                                CancellationToken     cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateTasks)}");

    var taskCollection = taskCollectionProvider_.Get();

    await taskCollection.InsertManyAsync(tasks.Select(taskData => taskData),
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<TaskData> ReadTaskAsync(string            taskId,
                                            CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ReadTaskAsync)}");
    activity?.SetTag("ReadTaskId",
                     taskId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.Find(tdm => tdm.TaskId == taskId)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task UpdateTaskStatusAsync(string            id,
                                          TaskStatus        status,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(UpdateTaskStatusAsync)}");
    activity?.SetTag($"{nameof(UpdateTaskStatusAsync)}_TaskId",
                     id);
    activity?.SetTag($"{nameof(UpdateTaskStatusAsync)}_Status",
                     status);
    var taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       status);
    Logger.LogInformation("update task {taskId} to status {status}",
                          id,
                          status);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == id && x.Status != TaskStatus.Completed && x.Status != TaskStatus.Cancelled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        var taskStatus = await GetTaskStatus(new[]
                                             {
                                               id,
                                             },
                                             cancellationToken)
                           .ConfigureAwait(false);
        throw new ArmoniKException($"Task not found or task already in a terminal state - {id} from {taskStatus.Single()} to {status}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                                  TaskStatus        status,
                                                  CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(UpdateAllTaskStatusAsync)}");
    var       taskCollection = taskCollectionProvider_.Get();

    if (filter.Included != null && (filter.Included.Statuses.Contains(TaskStatus.Completed) || filter.Included.Statuses.Contains(TaskStatus.Cancelled) ||
                                    filter.Included.Statuses.Contains(TaskStatus.Error)))
    {
      throw new ArmoniKException("The given TaskFilter contains a terminal state, update forbidden");
    }

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       status);
    Logger.LogInformation("update all tasks to statuses to status {status}",
                          status);
    var res = await taskCollection.UpdateManyAsync(filter.ToFilterExpression(),
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);
    return (int)res.MatchedCount;
  }

  /// <inheritdoc />
  public async Task<bool> IsTaskCancelledAsync(string            taskId,
                                               CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(IsTaskCancelledAsync)}");
    activity?.SetTag($"{nameof(IsTaskCancelledAsync)}_taskId",
                     taskId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.Find(model => model.TaskId == taskId)
                                 .Project(model => model.Status == TaskStatus.Cancelled || model.Status == TaskStatus.Cancelling)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.",
                                      e);
    }
  }

  /// <inheritdoc />
  public async Task StartTask(TaskData          taskData,
                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(StartTask)}");
    activity?.SetTag($"{nameof(StartTask)}_TaskId",
                     taskData.TaskId);
    var taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       TaskStatus.Processing)
                                                                  .Set(tdm => tdm.StartDate,
                                                                       taskData.StartDate)
                                                                  .Set(tdm => tdm.PodTtl,
                                                                       taskData.PodTtl);
    Logger.LogInformation("update task {taskId} to status {status}",
                          taskData.TaskId,
                          TaskStatus.Processing);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskData.TaskId && x.Status != TaskStatus.Completed && x.Status != TaskStatus.Cancelled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        var taskStatus = await GetTaskStatus(new[]
                                             {
                                               taskData.TaskId,
                                             },
                                             cancellationToken)
                           .ConfigureAwait(false);

        if (!taskStatus.Any())
        {
          throw new TaskNotFoundException($"Task {taskData.TaskId} not found");
        }

        throw new ArmoniKException($"Task already in a terminal state - {taskStatus.Single()} to {TaskStatus.Processing}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CancelSessionAsync)}");
    activity?.SetTag($"{nameof(CancelSessionAsync)}_sessionId",
                     sessionId);
    var taskCollection = taskCollectionProvider_.Get();

    var taskCount = await CountTasksAsync(new TaskFilter
                                          {
                                            Session = new TaskFilter.Types.IdsRequest
                                                      {
                                                        Ids =
                                                        {
                                                          sessionId,
                                                        },
                                                      },
                                          },
                                          cancellationToken)
                      .ConfigureAwait(false);

    if (!taskCount.Any())
    {
      throw new SessionNotFoundException($"Session '{sessionId}' not found");
    }

    await taskCollection.UpdateManyAsync(model => model.SessionId == sessionId && model.Status != TaskStatus.Completed && model.Status != TaskStatus.Cancelled,
                                         Builders<TaskData>.Update.Set(model => model.Status,
                                                                       TaskStatus.Cancelling),
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<IList<TaskData>> CancelTaskAsync(ICollection<string> taskIds,
                                                     CancellationToken   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(CancelTaskAsync)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(data => data.Status,
                                                                       TaskStatus.Cancelling)
                                                                  .Set(data => data.EndDate,
                                                                       DateTime.UtcNow);

    await taskCollection.UpdateManyAsync(data => taskIds.Contains(data.TaskId) &&
                                                 !(data.Status == TaskStatus.Cancelled || data.Status == TaskStatus.Cancelling || data.Status == TaskStatus.Error ||
                                                   data.Status == TaskStatus.Completed),
                                         updateDefinition,
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

    var tasks = await taskCollection.FindAsync(data => taskIds.Contains(data.TaskId),
                                               cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);

    return tasks.ToList();
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountTasksAsync)}");

    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();


    var res = await taskCollection.AsQueryable(sessionHandle)
                                  .FilterQuery(filter)
                                  .GroupBy(model => model.Status)
                                  .Select(models => new TaskStatusCount(models.Key,
                                                                        models.Count()))
                                  .ToListAsync(cancellationToken)
                                  .ConfigureAwait(false);

    return res;
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                                  CancellationToken                cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountTasksAsync)}");

    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    var res = await taskCollection.AsQueryable(sessionHandle)
                                  .Where(filter)
                                  .GroupBy(model => model.Status)
                                  .Select(models => new TaskStatusCount(models.Key,
                                                                        models.Count()))
                                  .ToListAsync(cancellationToken)
                                  .ConfigureAwait(false);

    return res;
  }

  /// <inheritdoc />
  public async Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountPartitionTasksAsync)}");

    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();


    var res = await taskCollection.AsQueryable(sessionHandle)
                                  .GroupBy(model => new
                                                    {
                                                      model.Options.PartitionId,
                                                      model.Status,
                                                    })
                                  .Select(models => new PartitionTaskStatusCount(models.Key.PartitionId,
                                                                                 models.Key.Status,
                                                                                 models.Count()))
                                  .ToListAsync(cancellationToken)
                                  .ConfigureAwait(false);

    return res;
  }

  /// <inheritdoc />
  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountAllTasksAsync)}");

    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    var res = taskCollection.AsQueryable(sessionHandle)
                            .Count(model => model.Status == status);

    return Task.FromResult(res);
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string            id,
                                    CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteTaskAsync)}");
    activity?.SetTag($"{nameof(DeleteTaskAsync)}_TaskId",
                     id);
    var taskCollection = taskCollectionProvider_.Get();

    var result = await taskCollection.DeleteOneAsync(tdm => tdm.TaskId == id,
                                                     cancellationToken)
                                     .ConfigureAwait(false);

    if (result.DeletedCount == 0)
    {
      throw new TaskNotFoundException($"Task '{id}' not found.");
    }

    if (result.DeletedCount > 1)
    {
      throw new ArmoniKException("Multiple tasks deleted");
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListTasksAsync(TaskFilter                                 filter,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    await foreach (var taskId in taskCollection.AsQueryable(sessionHandle)
                                               .FilterQuery(filter)
                                               .Select(model => model.TaskId)
                                               .ToAsyncEnumerable()
                                               .WithCancellation(cancellationToken)
                                               .ConfigureAwait(false))
    {
      yield return taskId;
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<TaskData> tasks, long totalCount)> ListTasksAsync(Expression<Func<TaskData, bool>>    filter,
                                                                                   Expression<Func<TaskData, object?>> orderField,
                                                                                   bool                                ascOrder,
                                                                                   int                                 page,
                                                                                   int                                 pageSize,
                                                                                   CancellationToken                   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    var findFluent = taskCollection.Find(sessionHandle,
                                         filter);

    var ordered = ascOrder
                    ? findFluent.SortBy(orderField)
                    : findFluent.SortByDescending(orderField);

    var taskResult = ordered.Skip(page * pageSize)
                            .Limit(pageSize)
                            .ToListAsync(cancellationToken);

    return (await taskResult.ConfigureAwait(false), await findFluent.CountDocumentsAsync(cancellationToken)
                                                                    .ConfigureAwait(false));
  }

  /// <inheritdoc />
  public async Task<IEnumerable<T>> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                                      Expression<Func<TaskData, T>>    selector,
                                                      CancellationToken                cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(filter)
                               .Select(selector)
                               .ToListAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                                   ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                                   bool ascOrder,
                                                                                                   int page,
                                                                                                   int pageSize,
                                                                                                   CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ListApplicationsAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    var queryable = taskCollection.AsQueryable(sessionHandle)
                                  .Where(filter)
                                  .GroupBy(data => new Application(data.Options.ApplicationName,
                                                                   data.Options.ApplicationNamespace,
                                                                   data.Options.ApplicationVersion,
                                                                   data.Options.ApplicationService))
                                  .Select(group => group.Key);

    var ordered = queryable.OrderByList(orderFields,
                                        ascOrder);

    var taskResult = ordered.Skip(page * pageSize)
                            .Take(pageSize)
                            .ToListAsync(cancellationToken);

    return (await taskResult.ConfigureAwait(false), await ordered.CountAsync(cancellationToken)
                                                                 .ConfigureAwait(false));
  }

  /// <inheritdoc />
  public async Task RemoveRemainingDataDependenciesAsync(ICollection<string> taskIds,
                                                         ICollection<string> dependenciesToRemove,
                                                         CancellationToken   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(RemoveRemainingDataDependenciesAsync)}");
    var       taskCollection = taskCollectionProvider_.Get();

    // MongoDB driver does not support to unset a list, so Unset should be called multiple times.
    // However, Unset on an UpdateDefinitionBuilder returns an UpdateDefinition.
    // We need to call Unset on the first element outside of the loop.
    using var deps = dependenciesToRemove.GetEnumerator();
    if (!deps.MoveNext())
    {
      return;
    }


    var key0   = TaskData.EscapeKey(deps.Current);
    var update = new UpdateDefinitionBuilder<TaskData>().Unset(data => data.RemainingDataDependencies[key0]);
    while (deps.MoveNext())
    {
      var key = TaskData.EscapeKey(deps.Current);
      update = update.Unset(data => data.RemainingDataDependencies[key]);
    }

    await taskCollection.UpdateManyAsync(data => taskIds.Contains(data.TaskId),
                                         update,
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<TaskData> UpdateOneTask(string                                                                        taskId,
                                            ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                            CancellationToken                                                             cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(UpdateOneTask)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Combine();

    foreach (var (selector, newValue) in updates)
    {
      updateDefinition = updateDefinition.Set(selector,
                                              newValue);
    }

    var filter = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskId);

    var task = await taskCollection.FindOneAndUpdateAsync(filter,
                                                          updateDefinition,
                                                          new FindOneAndUpdateOptions<TaskData>
                                                          {
                                                            ReturnDocument = ReturnDocument.Before,
                                                          },
                                                          cancellationToken)
                                   .ConfigureAwait(false);

    return task ?? throw new TaskNotFoundException($"Task not found {taskId}");
  }

  /// <inheritdoc />
  public async Task<Output> GetTaskOutput(string            taskId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskOutput)}");
    activity?.SetTag($"{nameof(GetTaskOutput)}_TaskId",
                     taskId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
                                 .Select(model => model.Output)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new TaskNotFoundException($"Task not found {taskId}",
                                      e);
    }
  }

  /// <inheritdoc />
  public async Task<TaskData> AcquireTask(TaskData          taskData,
                                          CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(AcquireTask)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                       taskData.OwnerPodId)
                                                                  .Set(tdm => tdm.OwnerPodName,
                                                                       taskData.OwnerPodName)
                                                                  .Set(tdm => tdm.ReceptionDate,
                                                                       taskData.ReceptionDate)
                                                                  .Set(tdm => tdm.AcquisitionDate,
                                                                       taskData.AcquisitionDate)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Dispatched);

    var filter = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskData.TaskId && x.OwnerPodId == "" && x.Status == TaskStatus.Submitted);

    Logger.LogDebug("Acquire task {task} on {podName}",
                    taskData.TaskId,
                    taskData.OwnerPodId);
    var res = await taskCollection.FindOneAndUpdateAsync(filter,
                                                         updateDefinition,
                                                         new FindOneAndUpdateOptions<TaskData>
                                                         {
                                                           ReturnDocument = ReturnDocument.After,
                                                         },
                                                         cancellationToken)
                                  .ConfigureAwait(false);

    return res ?? await ReadTaskAsync(taskData.TaskId,
                                      cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<TaskData> ReleaseTask(TaskData          taskData,
                                          CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ReleaseTask)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                       "")
                                                                  .Set(tdm => tdm.OwnerPodName,
                                                                       "")
                                                                  .Set(tdm => tdm.AcquisitionDate,
                                                                       null)
                                                                  .Set(tdm => tdm.ReceptionDate,
                                                                       null)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Submitted);

    var filter = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskData.TaskId && x.OwnerPodId == taskData.OwnerPodId);

    Logger.LogInformation("Release task {task} on {podName}",
                          taskData.TaskId,
                          taskData.OwnerPodId);
    var res = await taskCollection.FindOneAndUpdateAsync(filter,
                                                         updateDefinition,
                                                         new FindOneAndUpdateOptions<TaskData>
                                                         {
                                                           ReturnDocument = ReturnDocument.After,
                                                         },
                                                         cancellationToken)
                                  .ConfigureAwait(false);

    Logger.LogDebug("Released task {taskData}",
                    res);

    if (Logger.IsEnabled(LogLevel.Debug) && res is null)
    {
      Logger.LogDebug("Released task (old) {taskData}",
                      await ReadTaskAsync(taskData.TaskId,
                                          cancellationToken)
                        .ConfigureAwait(false));
    }

    return res ?? await ReadTaskAsync(taskData.TaskId,
                                      cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskIds,
                                                                                  CancellationToken   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetTaskStatus)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => taskIds.Contains(tdm.TaskId))
                               .Select(model => new GetTaskStatusReply.Types.IdStatus
                                                {
                                                  Status = model.Status,
                                                  TaskId = model.TaskId,
                                                })
                               .ToListAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  public IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                              CancellationToken   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetTasksExpectedOutputKeys)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    return taskCollection.AsQueryable(sessionHandle)
                         .Where(tdm => taskIds.Contains(tdm.TaskId))
                         .Select(model => new
                                          {
                                            model.TaskId,
                                            model.ExpectedOutputIds,
                                          })
                         .ToAsyncEnumerable(cancellationToken)
                         .Select(model => (model.TaskId, model.ExpectedOutputIds.AsEnumerable()));
  }

  public async Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskExpectedOutputKeys)}");
    activity?.SetTag($"{nameof(GetTaskExpectedOutputKeys)}_TaskId",
                     taskId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.Find(tdm => tdm.TaskId == taskId)
                                 .Project(model => model.ExpectedOutputIds)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  public async Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetParentTaskIds)}");
    activity?.SetTag($"{nameof(GetParentTaskIds)}_TaskId",
                     taskId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.Find(tdm => tdm.TaskId == taskId)
                                 .Project(model => model.ParentTaskIds)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  public async Task<string> RetryTask(TaskData          taskData,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(RetryTask)}");

    var taskCollection = taskCollectionProvider_.Get();

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

    await taskCollection.InsertOneAsync(newTaskData,
                                        cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
    return newTaskId;
  }

  public async Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                              CancellationToken   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(FinalizeTaskCreation)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       TaskStatus.Submitted)
                                                                  .Set(tdm => tdm.SubmittedDate,
                                                                       DateTime.UtcNow);
    Logger.LogInformation("update all tasks to status {status}",
                          TaskStatus.Submitted);
    var res = await taskCollection.UpdateManyAsync(tdm => taskIds.Contains(tdm.TaskId) && tdm.Status == TaskStatus.Creating,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);
    return (int)res.MatchedCount;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      await taskCollectionProvider_.Init(cancellationToken)
                                   .ConfigureAwait(false);
      taskCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }
}
