// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

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
  public async Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                               Expression<Func<TaskData, object?>> orderField,
                                                                               Expression<Func<TaskData, T>>       selector,
                                                                               bool                                ascOrder,
                                                                               int                                 page,
                                                                               int                                 pageSize,
                                                                               CancellationToken                   cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    var taskList = Task.FromResult(new List<T>());
    if (pageSize > 0)
    {
      var findFluent1 = taskCollection.Find(sessionHandle,
                                            filter);

      var ordered = ascOrder
                      ? findFluent1.SortBy(orderField)
                      : findFluent1.SortByDescending(orderField);

      taskList = ordered.Skip(page * pageSize)
                        .Limit(pageSize)
                        .Project(selector)
                        .ToListAsync(cancellationToken);
    }

    // Find needs to be duplicated, otherwise, the count is computed on a single page, and not the whole collection
    var taskCount = taskCollection.CountDocumentsAsync(sessionHandle,
                                                       filter,
                                                       cancellationToken: cancellationToken);

    return (await taskList.ConfigureAwait(false), await taskCount.ConfigureAwait(false));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                               Expression<Func<TaskData, T>>    selector,
                                               CancellationToken                cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(FindTasksAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = taskCollectionProvider_.Get();

    return taskCollection.Find(sessionHandle,
                               filter)
                         .Project(selector)
                         .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>                                              filter,
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

    var result = await taskCollection.UpdateManyAsync(filter,
                                                      updateDefinition,
                                                      cancellationToken: cancellationToken)
                                     .ConfigureAwait(false);

    return result.MatchedCount;
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
  public async Task<TaskData?> UpdateOneTask(string                                                                        taskId,
                                             Expression<Func<TaskData, bool>>?                                             filter,
                                             ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                             bool                                                                          before,
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

    var where = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskId);

    if (filter is not null)
    {
      where = new FilterDefinitionBuilder<TaskData>().And(where,
                                                          new FilterDefinitionBuilder<TaskData>().Where(filter));
    }

    var task = await taskCollection.FindOneAndUpdateAsync(where,
                                                          updateDefinition,
                                                          new FindOneAndUpdateOptions<TaskData>
                                                          {
                                                            ReturnDocument = before
                                                                               ? ReturnDocument.Before
                                                                               : ReturnDocument.After,
                                                          },
                                                          cancellationToken)
                                   .ConfigureAwait(false);

    Logger.LogInformation("Update {task} with {condition} and {updates}",
                          taskId,
                          filter,
                          updates);

    return task;
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
