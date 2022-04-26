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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
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

    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

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
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ArmoniKException($"Task '{taskId}' not found.");
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
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       status);
    Logger.LogInformation("update task {taskId} to status {status}",
                          id,
                          status);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == id && x.Status != TaskStatus.Completed && x.Status != TaskStatus.Failed &&
                                                        x.Status != TaskStatus.Canceled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        var taskStatus = await GetTaskStatus(id,
                                             cancellationToken)
                           .ConfigureAwait(false);
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
    using var activity = activitySource_.StartActivity($"{nameof(UpdateAllTaskStatusAsync)}");
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    if (filter.Included != null && filter.Included.Statuses.Contains(TaskStatus.Completed) | filter.Included.Statuses.Contains(TaskStatus.Failed) |
        filter.Included.Statuses.Contains(TaskStatus.Canceled))
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
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(model => model.TaskId == taskId)
                               .Select(model => model.Status == TaskStatus.Canceled || model.Status == TaskStatus.Canceling)
                               .FirstAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  public async Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(StartTask)}");
    activity?.SetTag($"{nameof(StartTask)}_TaskId",
                     taskId);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       TaskStatus.Processing)
                                                                  .Set(tdm => tdm.StartDate,
                                                                       DateTime.UtcNow)
                                                                  .Set(tdm => tdm.PodTtl,
                                                                       DateTime.UtcNow);
    Logger.LogInformation("update task {taskId} to status {status}",
                          taskId,
                          TaskStatus.Processing);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId && x.Status != TaskStatus.Completed && x.Status != TaskStatus.Failed &&
                                                        x.Status != TaskStatus.Canceled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        var taskStatus = await GetTaskStatus(taskId,
                                             cancellationToken)
                           .ConfigureAwait(false);
        throw new ArmoniKException($"Task not found or task already in a terminal state - {taskStatus} from {taskStatus} to {TaskStatus.Processing}");
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
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var result = await taskCollection.UpdateManyAsync(model => model.SessionId == sessionId,
                                                      Builders<TaskData>.Update.Set(model => model.Status,
                                                                                    TaskStatus.Canceling),
                                                      cancellationToken: cancellationToken)
                                     .ConfigureAwait(false);
    if (result.MatchedCount == 0)
    {
      throw new ArmoniKException($"Key '{sessionId}' not found");
    }
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountTasksAsync)}");

    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);


    var res = await taskCollection.AsQueryable(sessionHandle)
                                  .FilterQuery(filter)
                                  .GroupBy(model => model.Status)
                                  .Select(models => new TaskStatusCount(models.Key,
                                                                        models.Count()))
                                  .ToListAsync(cancellationToken)
                                  .ConfigureAwait(false);

    return res.Select(tuple => new TaskStatusCount(tuple.Status,
                                                   tuple.Count));
  }

  /// <inheritdoc />
  public async Task<int> CountAllTasksAsync(TaskStatus        status,
                                            CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountAllTasksAsync)}");

    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var res = taskCollection.AsQueryable(sessionHandle)
                            .Count(model => model.Status == status);

    return res;
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string            id,
                                    CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteTaskAsync)}");
    activity?.SetTag($"{nameof(DeleteTaskAsync)}_TaskId",
                     id);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    await taskCollection.DeleteOneAsync(tdm => tdm.TaskId == id,
                                        cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListTasksAsync(TaskFilter                                 filter,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    await foreach (var taskId in taskCollection.AsQueryable(sessionHandle)
                                               .FilterQuery(filter)
                                               .Select(model => model.TaskId)
                                               .AsAsyncEnumerable()
                                               .WithCancellation(cancellationToken)
                                               .ConfigureAwait(false))
    {
      yield return taskId;
    }
  }

  public async Task SetTaskSuccessAsync(string            taskId,
                                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetTaskSuccessAsync)}");
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var taskOutput = new Output(Error: "",
                                Success: true);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOutput)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Completed)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       DateTime.UtcNow);
    Logger.LogDebug("update task {taskId} to output {output}",
                    taskId,
                    taskOutput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        throw new ArmoniKException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  public async Task SetTaskErrorAsync(string            taskId,
                                      string            errorDetail,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetTaskErrorAsync)}");
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var taskOutput = new Output(Error: errorDetail,
                                Success: false);

    /* A Task that errors is conceptually a  completed task,
     * the error is reported and detailed in its Output*/
    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOutput)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Completed)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       DateTime.UtcNow);
    Logger.LogDebug("update task {taskId} to output {output}",
                    taskId,
                    taskOutput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        throw new ArmoniKException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task<Output> GetTaskOutput(string            taskId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskOutput)}");
    activity?.SetTag($"{nameof(GetTaskOutput)}_TaskId",
                     taskId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.Output)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  public async Task<bool> AcquireTask(string            taskId,
                                CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AcquireTask)}");
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var hostname = Dns.GetHostName();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                       hostname)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Dispatched);

    Logger.LogDebug("Acquire task {taskId} on {podName}",
                    taskId,
                    hostname);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId && x.OwnerPodId == "",
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        var sessionHandle = await sessionProvider_.GetAsync()
                                                  .ConfigureAwait(false);
        var ownerPodId = await taskCollection.AsQueryable(sessionHandle)
                            .Where(tdm => tdm.TaskId == taskId)
                            .Select(model => model.OwnerPodId)
                            .SingleAsync(cancellationToken)
                            .ConfigureAwait(false);
        Logger.LogInformation("Task {taskId} already acquired by {OtherOwnerPodId}", taskId, ownerPodId);
        return false;
      case 1:
        return true;
      default:
        throw new ArmoniKException($"Error during acquisition of task {taskId}");
    }
  }

  public async Task<TaskStatus> GetTaskStatus(string            taskId,
                                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskStatus)}");
    activity?.SetTag($"{nameof(GetTaskStatus)}_TaskId",
                     taskId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
                                 .Select(model => model.Status)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);

    }
    catch (InvalidOperationException)
    {
      throw new ArmoniKException($"Task '{taskId}' not found.");
    }
  }

  public async Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskExpectedOutputKeys)}");
    activity?.SetTag($"{nameof(GetTaskExpectedOutputKeys)}_TaskId",
                     taskId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.ExpectedOutputIds)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  public async Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                    CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetParentTaskIds)}");
    activity?.SetTag($"{nameof(GetParentTaskIds)}_TaskId",
                     taskId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(tdm => tdm.TaskId == taskId)
                               .Select(model => model.ParentTaskIds)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
  }

  public async Task<string> RetryTask(TaskData          taskData,
                           CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(RetryTask)}");

    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var newTaskId = taskData.TaskId + $"###{taskData.RetryOfIds.Count + 1}";

    var newTaskRetryOfIds = new List<string>(taskData.RetryOfIds)
                            {
                              taskData.TaskId,
                            };
    var newTaskData = new TaskData(taskData.SessionId,
                                   newTaskId,
                                   "",
                                   taskData.ParentTaskIds,
                                   taskData.DataDependencies,
                                   taskData.ExpectedOutputIds,
                                   newTaskRetryOfIds,
                                   TaskStatus.Creating,
                                   "",
                                   taskData.Options,
                                   DateTime.UtcNow,
                                   DateTime.MinValue,
                                   DateTime.MinValue,
                                   DateTime.MinValue,
                                   DateTime.MinValue,
                                   new Output(false,
                                              ""));

    await taskCollection.InsertOneAsync(newTaskData,
                                        cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
    return newTaskId;
  }

  public async Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(FinalizeTaskCreation)}");
    var taskCollection = await taskCollectionProvider_.GetAsync()
                                                      .ConfigureAwait(false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Status,
                                                                       TaskStatus.Submitted)
                                                                  .Set(tdm => tdm.SubmittedDate,
                                                                       DateTime.UtcNow);
    Logger.LogInformation("update all tasks to statuses to status {status}", TaskStatus.Submitted);
    var res = await taskCollection.UpdateManyAsync(tdm => taskIds.Contains(tdm.TaskId) && tdm.Status == TaskStatus.Creating,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);
    return (int)res.MatchedCount;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var session        = sessionProvider_.GetAsync();
      var taskCollection = taskCollectionProvider_.GetAsync();
      await session.ConfigureAwait(false);
      await taskCollection.ConfigureAwait(false);
      isInitialized_ = true;
    }
  }
}
