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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <inheritdoc cref="ITaskTable" />
public class TaskTable : BaseTable<TaskData, TaskDataModelMapping>, ITaskTable
{
  /// <inheritdoc />
  public TaskTable(SessionProvider                                         sessionProvider,
                   MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider,
                   ActivitySource                                          activitySource,
                   ILogger<TaskTable>                                      logger)
    : base(sessionProvider,
           taskCollectionProvider,
           activitySource,
           logger)
  {
  }

  /// <inheritdoc />
  private TaskTable(TaskTable taskTable,
                    bool      readOnly)
    : base(taskTable,
           readOnly)
  {
  }

  /// <inheritdoc />
  public ITaskTable Secondary
    => new TaskTable(this,
                     true);

  /// <inheritdoc />
  public TimeSpan PollingDelayMin { get; set; }

  /// <inheritdoc />
  public TimeSpan PollingDelayMax { get; set; }

  /// <inheritdoc />
  public async Task CreateTasks(IEnumerable<TaskData> tasks,
                                CancellationToken     cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       taskCollection = GetCollection();

    try
    {
      await taskCollection.InsertManyAsync(tasks.Select(taskData => taskData),
                                           cancellationToken: cancellationToken)
                          .ConfigureAwait(false);
    }
    catch (MongoBulkWriteException<TaskData> e) when (e.WriteErrors.All(error => error.Category == ServerErrorCategory.DuplicateKey))
    {
      throw new TaskAlreadyExistsException("Task already exists",
                                           e);
    }
  }

  /// <inheritdoc />
  public async Task<T> ReadTaskAsync<T>(string                        taskId,
                                        Expression<Func<TaskData, T>> selector,
                                        CancellationToken             cancellationToken = default)
  {
    using var activity = StartActivity();
    activity?.SetTag("ReadTaskId",
                     taskId);
    var taskCollection = GetReadCollection();

    try
    {
      var task = await taskCollection.Find(tdm => tdm.TaskId == taskId)
                                     .Project(selector)
                                     .SingleAsync(cancellationToken)
                                     .ConfigureAwait(false);
      Logger.LogDebug("Read {id} matching {condition} returns {@task}",
                      taskId,
                      selector,
                      task);
      return task;
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                                  CancellationToken                cancellationToken = default)
  {
    using var activity = StartActivity();

    var sessionHandle  = GetSession();
    var taskCollection = GetReadCollection();

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
    using var activity = StartActivity();

    var sessionHandle  = GetSession();
    var taskCollection = GetReadCollection();


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
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeleteTaskAsync)}_TaskId",
                     id);
    var taskCollection = GetCollection();

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
  public async Task DeleteTasksAsync(string            sessionId,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeleteTaskAsync)}_SessionId",
                     sessionId);
    var taskCollection = GetCollection();

    var res = await taskCollection.DeleteManyAsync(tdm => tdm.SessionId == sessionId,
                                                   cancellationToken)
                                  .ConfigureAwait(false);

    if (res.DeletedCount > 0)
    {
      Logger.LogDebug("Deleted Tasks from {sessionId}",
                      sessionId);
    }
  }

  /// <inheritdoc />
  public async Task DeleteTasksAsync(ICollection<string> taskIds,
                                     CancellationToken   cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       taskCollection = GetCollection();

    await taskCollection.DeleteManyAsync(data => taskIds.Contains(data.TaskId),
                                         cancellationToken)
                        .ConfigureAwait(false);
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
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetReadCollection();

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
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetReadCollection();

    return taskCollection.Find(sessionHandle,
                               filter)
                         .Project(selector)
                         .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>               filter,
                                          Core.Common.Storage.UpdateDefinition<TaskData> updates,
                                          CancellationToken                              cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       taskCollection = GetCollection();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Combine();

    foreach (var (selector, newValue) in updates.Setters)
    {
      updateDefinition = updateDefinition.Set(selector,
                                              newValue);
    }

    var result = await taskCollection.UpdateManyAsync(filter,
                                                      updateDefinition,
                                                      cancellationToken: cancellationToken)
                                     .ConfigureAwait(false);

    Logger.LogDebug("Update tasks matching {condition} and {@updates}; {modifiedCount} tasks modified and {matchedCount} tasks matched",
                    filter,
                    updates,
                    result.ModifiedCount,
                    result.MatchedCount);

    return result.MatchedCount;
  }

  /// <inheritdoc />
  async Task<long> ITaskTable.BulkUpdateTasks(IEnumerable<(Expression<Func<TaskData, bool>> filter, Core.Common.Storage.UpdateDefinition<TaskData> updates)> bulkUpdates,
                                              CancellationToken cancellationToken)
  {
    using var activity       = StartActivity();
    var       taskCollection = GetCollection();

    var requests = bulkUpdates.Select(item =>
                                      {
                                        var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Combine();

                                        foreach (var (selector, newValue) in item.updates.Setters)
                                        {
                                          updateDefinition = updateDefinition.Set(selector,
                                                                                  newValue);
                                        }

                                        return new UpdateManyModel<TaskData>(Builders<TaskData>.Filter.Where(item.filter),
                                                                             updateDefinition);
                                      })
                              .AsICollection();

    var updateResult = await taskCollection.BulkWriteAsync(requests,
                                                           new BulkWriteOptions
                                                           {
                                                             IsOrdered = false,
                                                           },
                                                           cancellationToken)
                                           .ConfigureAwait(false);

    Logger.LogDebug("Bulk update tasks matching {@updates}",
                    bulkUpdates);

    return updateResult.MatchedCount;
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                                   ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                                   bool ascOrder,
                                                                                                   int page,
                                                                                                   int pageSize,
                                                                                                   CancellationToken cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetReadCollection();

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
  public async IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>                        taskIds,
                                                                           ICollection<string>                        dependenciesToRemove,
                                                                           Expression<Func<TaskData, T>>              selector,
                                                                           [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetCollection();

    // MongoDB driver does not support to unset a list, so Unset should be called multiple times.
    // However, Unset on an UpdateDefinitionBuilder returns an UpdateDefinition.
    // We need to call Unset on the first element outside of the loop.
    using var deps = dependenciesToRemove.GetEnumerator();
    if (!deps.MoveNext())
    {
      yield break;
    }


    var key0   = deps.Current;
    var update = new UpdateDefinitionBuilder<TaskData>().Unset(data => data.RemainingDataDependencies[key0]);
    while (deps.MoveNext())
    {
      var key = deps.Current;
      update = update.Unset(data => data.RemainingDataDependencies[key]);
    }

    Logger.LogDebug("Remove data dependencies {@ResultIds} for tasks {@TaskIds}",
                    dependenciesToRemove,
                    taskIds);

    await taskCollection.UpdateManyAsync(sessionHandle,
                                         data => taskIds.Contains(data.TaskId),
                                         update,
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

    var emptyDictionary = new Dictionary<string, bool>();

    var filter =   Builders<TaskData>.Filter.In(t => t.TaskId, taskIds)
                 & Builders<TaskData>.Filter.Or(Builders<TaskData>.Filter.Eq(t => t.Status, TaskStatus.Creating),
                                                          Builders<TaskData>.Filter.Eq(t => t.Status,TaskStatus.Pending))
                 & Builders<TaskData>.Filter.Eq(t => t.RemainingDataDependencies,
                                                emptyDictionary);

    var readyTasks = taskCollection.Find(filter)
                                   .Project(selector)
                                   .ToAsyncEnumerable(cancellationToken);

    await foreach (var task in readyTasks.ConfigureAwait(false))
    {
      yield return task;
    }
  }

  /// <inheritdoc />
  public async Task<TaskData?> UpdateOneTask(string                                         taskId,
                                             Expression<Func<TaskData, bool>>?              filter,
                                             Core.Common.Storage.UpdateDefinition<TaskData> updates,
                                             bool                                           before,
                                             CancellationToken                              cancellationToken = default)
  {
    using var activity       = StartActivity();
    var       taskCollection = GetCollection();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Combine();

    foreach (var (selector, newValue) in updates.Setters)
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

    Logger.LogDebug("Update {task} matching {condition} and {@updates}; got {data}",
                    taskId,
                    filter,
                    updates,
                    task);

    return task;
  }

  /// <inheritdoc />
  ILogger ITaskTable.Logger
    => base.Logger;

  /// <inheritdoc />
  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = StartActivity($"{nameof(CountAllTasksAsync)}");

    var sessionHandle  = GetSession();
    var taskCollection = GetReadCollection();

    var res = taskCollection.AsQueryable(sessionHandle)
                            .Count(model => model.Status == status);

    return Task.FromResult(res);
  }
}
