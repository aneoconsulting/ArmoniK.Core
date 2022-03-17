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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

public class TaskTable : ITaskTable
{
  private readonly SessionProvider                        sessionProvider_;
  private readonly MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider_;

  public TaskTable(SessionProvider sessionProvider, MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider, ILogger<TaskTable> logger)
  {
    sessionProvider_        = sessionProvider;
    taskCollectionProvider_ = taskCollectionProvider;
    Logger                  = logger;
  }

  /// <inheritdoc />
  public TimeSpan PollingDelay { get; set; } = TimeSpan.FromMilliseconds(150);

  /// <inheritdoc />
  public async Task CreateTasks(IEnumerable<Core.Common.Storage.TaskData> tasks, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.InsertManyAsync(tasks.Select(taskData => taskData),
                                         cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task<Core.Common.Storage.TaskData> ReadTaskAsync(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(taskId);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<string> GetTaskDispatchId(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(model => model.TaskId == taskId)
                               .Select(model => model.DispatchId)
                               .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IList<string>> GetTaskAncestorDispatchIds(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(taskId);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();


    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model=>model.AncestorDispatchIds)
                               .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task ChangeTaskDispatch(string oldDispatchId, string newDispatchId, CancellationToken cancellationToken)
  {
    using var _ = Logger.LogFunction();

    var taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.UpdateManyAsync(model => model.DispatchId == oldDispatchId,
                                         Builders<TaskData>.Update
                                                                .Set(model => model.DispatchId,
                                                                     newDispatchId),
                                         cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task UpdateTaskStatusAsync(string            id,
                                          TaskStatus        status,
                                          CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(id);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                            status);
    Logger.LogInformation("update task {taskId} to status {status}",
                    id,
                    status);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == id &&
                                                        x.Status != TaskStatus.Completed &&
                                                        x.Status != TaskStatus.Failed &&
                                                        x.Status != TaskStatus.Canceled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken);

    switch (res.MatchedCount)
    {
      case 0:
        var taskStatus = await GetTaskStatus(id,
                            cancellationToken);
        throw new ArmoniKException($"Task not found or task already in a terminal state - {id} from {taskStatus} to {status}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                                  TaskStatus        status,
                                                  CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    var       taskCollection = await taskCollectionProvider_.GetAsync();


    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                            status);

    var res = await taskCollection.UpdateManyAsync(filter.ToFilterExpression(),
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken);
    return (int)res.MatchedCount;
  }

  /// <inheritdoc />
  public async Task<bool> IsTaskCancelledAsync(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(model => model.TaskId == taskId)
                               .Select(model => model.Status == TaskStatus.Canceled || model.Status == TaskStatus.Canceling)
                               .FirstAsync(cancellationToken);

  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(sessionId);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.UpdateManyAsync(model => model.SessionId == sessionId,
                                         Builders<TaskData>.Update
                                                                .Set(model => model.Status,
                                                                     TaskStatus.Canceling),
                                         cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(dispatchId);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.UpdateManyAsync(model => model.DispatchId == dispatchId,
                                         Builders<TaskData>.Update
                                                                .Set(model => model.Status,
                                                                     TaskStatus.Canceling),
                                         cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var sessionHandle  = await sessionProvider_.GetAsync();
    var taskCollection = await taskCollectionProvider_.GetAsync();



    var res = await taskCollection.AsQueryable(sessionHandle)
                                  .FilterQuery(filter)
                                  .GroupBy(model => model.Status)
                                  .Select(models => new TaskStatusCount(models.Key,
                                                                        models.Count()))
                                  .ToListAsync(cancellationToken);

    return res.Select(tuple => new TaskStatusCount(tuple.Status, tuple.Count));
  }

  /// <inheritdoc />
  public async Task<int> CountAllTasksAsync(TaskStatus status, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var sessionHandle  = await sessionProvider_.GetAsync();
    var taskCollection = await taskCollectionProvider_.GetAsync();

    var res = taskCollection
              .AsQueryable(sessionHandle)
              .Count(model => model.Status == status);

    return res;
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(id);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.DeleteOneAsync(tdm => tdm.TaskId == id,
                                        cancellationToken);

  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListTasksAsync(TaskFilter filter, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var _              = Logger.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await foreach (var taskId in taskCollection.AsQueryable(sessionHandle)
                                               .FilterQuery(filter)
                                               .Select(model => model.TaskId)
                                               .AsAsyncEnumerable().WithCancellation(cancellationToken))
      yield return taskId;
  }

  public async Task SetTaskSuccessAsync(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    var taskOuput = new Output(Error: "",
                               Success: true);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOuput).Set(tdm => tdm.Status,
                                                                                      TaskStatus.Completed);
    Logger.LogDebug("update task {taskId} to output {output}",
                    taskId,
                    taskOuput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken);

    switch (res.MatchedCount)
    {
      case 0:
        throw new ArmoniKException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  public async Task SetTaskErrorAsync(string taskId, string errorDetail, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    var taskOuput = new Output(Error: errorDetail,
                               Success: false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOuput).Set(tdm => tdm.Status,
                                                                                      TaskStatus.Completed);
    Logger.LogDebug("update task {taskId} to output {output}",
                    taskId,
                    taskOuput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken);

    switch (res.MatchedCount)
    {
      case 0:
        throw new ArmoniKException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task<Output> GetTaskOutput(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(taskId);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.Output)
                               .FirstAsync(cancellationToken);
  }

  public async Task<TaskStatus> GetTaskStatus(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(taskId);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.Status)
                               .FirstAsync(cancellationToken);
  }

  public async Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction(taskId);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.ExpectedOutput)
                               .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public ILogger Logger { get; }
}
