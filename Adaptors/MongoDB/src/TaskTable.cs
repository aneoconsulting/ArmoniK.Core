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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
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

using InvalidOperationException = System.InvalidOperationException;
using Output = ArmoniK.Core.Common.Storage.Output;
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
    var sessionHandle = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
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
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == id && x.Status != TaskStatus.Completed && x.Status != TaskStatus.Failed &&
                                                        x.Status != TaskStatus.Canceled,
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
    using var activity = activitySource_.StartActivity($"{nameof(UpdateAllTaskStatusAsync)}");
    var taskCollection = taskCollectionProvider_.Get();

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
    var sessionHandle = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(model => model.TaskId == taskId)
                                 .Select(model => model.Status == TaskStatus.Canceled || model.Status == TaskStatus.Canceling)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.", e);
    }
  }

  public async Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(StartTask)}");
    activity?.SetTag($"{nameof(StartTask)}_TaskId",
                     taskId);
    var taskCollection = taskCollectionProvider_.Get();

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
        var taskStatus = await GetTaskStatus(new[]
                                             {
                                               taskId,
                                             },
                                             cancellationToken)
                           .ConfigureAwait(false);

        if (!taskStatus.Any())
        {
          throw new TaskNotFoundException($"Task {taskId} not found");
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

    await taskCollection.UpdateManyAsync(model => model.SessionId == sessionId && model.Status != TaskStatus.Completed && model.Status != TaskStatus.Failed &&
                                                  model.Status    != TaskStatus.Canceled,
                                         Builders<TaskData>.Update.Set(model => model.Status,
                                                                       TaskStatus.Canceling),
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountTasksAsync)}");

    var sessionHandle = sessionProvider_.Get();
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
  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                            CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CountAllTasksAsync)}");

    var sessionHandle = sessionProvider_.Get();
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
                                                       [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListTasksAsync)}");
    var sessionHandle = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

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

  public async Task SetTaskSuccessAsync(string            taskId,
                                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetTaskSuccessAsync)}");
    var taskCollection = taskCollectionProvider_.Get();

    var taskOutput = new Output(Error: "",
                                Success: true);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOutput)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Completed)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       DateTime.UtcNow);
    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                    taskId,
                    TaskStatus.Completed,
                    taskOutput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        throw new TaskNotFoundException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  public async Task SetTaskCanceledAsync(string taskId,
                                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetTaskCanceledAsync)}");
    var taskCollection = taskCollectionProvider_.Get();

    var taskOutput = new Output(Error: "",
                                Success: false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOutput)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Canceled)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       DateTime.UtcNow);
    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                    taskId,
                    TaskStatus.Canceled,
                    taskOutput);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == taskId,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    switch (res.MatchedCount)
    {
      case 0:
        throw new TaskNotFoundException($"Task not found {taskId}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  public async Task<bool> SetTaskErrorAsync(string            taskId,
                                      string            errorDetail,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetTaskErrorAsync)}");
    var taskCollection = taskCollectionProvider_.Get();

    var taskOutput = new Output(Error: errorDetail,
                                Success: false);

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.Output,
                                                                       taskOutput)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Error)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       DateTime.UtcNow);

    var filter = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskId);

    Logger.LogInformation("update task {taskId} to status {status} with {output}",
                    taskId,
                    TaskStatus.Error,
                    taskOutput);
    var task = await taskCollection.FindOneAndUpdateAsync(filter,
                                                         updateDefinition,
                                                         new FindOneAndUpdateOptions<TaskData>
                                                         {
                                                           ReturnDocument = ReturnDocument.Before,
                                                         },
                                                         cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);

    if (task == null)
    {
      throw new TaskNotFoundException($"Task not found {taskId}");
    }

    return task.Status != TaskStatus.Error;
  }

  /// <inheritdoc />
  public async Task<Output> GetTaskOutput(string            taskId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskOutput)}");
    activity?.SetTag($"{nameof(GetTaskOutput)}_TaskId",
                     taskId);
    var sessionHandle = sessionProvider_.Get();
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
      throw new TaskNotFoundException($"Task not found {taskId}", e);
    }
  }

  /// <inheritdoc />
  public async Task<TaskData> AcquireTask(string            taskId,
                                          string            ownerPodId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(AcquireTask)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                       ownerPodId)
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Dispatched);

    var filter = new FilterDefinitionBuilder<TaskData>().Where(x => x.TaskId == taskId && x.OwnerPodId == "" && x.Status == TaskStatus.Submitted);

    Logger.LogDebug("Acquire task {task} on {podName}",
                    taskId,
                    ownerPodId);
    var res = await taskCollection.FindOneAndUpdateAsync(filter,
                                                         updateDefinition,
                                                         new FindOneAndUpdateOptions<TaskData>
                                                         {
                                                           ReturnDocument = ReturnDocument.After,
                                                         },
                                                         cancellationToken)
                                  .ConfigureAwait(false);

    return res ?? await ReadTaskAsync(taskId,
                                      cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<TaskData> ReleaseTask(string            taskId,
                                          string            ownerPodId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ReleaseTask)}");
    var       taskCollection = taskCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                       "")
                                                                  .Set(tdm => tdm.Status,
                                                                       TaskStatus.Submitted);

    var filter = new FilterDefinitionBuilder<TaskData>().And(new FilterDefinitionBuilder<TaskData>().Eq(x => x.OwnerPodId,
                                                                                                        ownerPodId),
                                                             new FilterDefinitionBuilder<TaskData>().Eq(x => x.TaskId,
                                                                                                        taskId),
                                                             new FilterDefinitionBuilder<TaskData>().In(x => x.Status,
                                                                                                        new[]
                                                                                                        {
                                                                                                          TaskStatus.Processing,
                                                                                                          TaskStatus.Dispatched,
                                                                                                        }));

    Logger.LogDebug("Release task {task} on {podName}",
                    taskId,
                    ownerPodId);
    var res = await taskCollection.FindOneAndUpdateAsync(filter,
                                                         updateDefinition,
                                                         new FindOneAndUpdateOptions<TaskData>
                                                         {
                                                           ReturnDocument = ReturnDocument.After,
                                                         },
                                                         cancellationToken)
                                  .ConfigureAwait(false);

    return res ?? await ReadTaskAsync(taskId,
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

  public async Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskExpectedOutputKeys)}");
    activity?.SetTag($"{nameof(GetTaskExpectedOutputKeys)}_TaskId",
                     taskId);
    var sessionHandle = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
                                 .Select(model => model.ExpectedOutputIds)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  public async Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                    CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetParentTaskIds)}");
    activity?.SetTag($"{nameof(GetParentTaskIds)}_TaskId",
                     taskId);
    var sessionHandle = sessionProvider_.Get();
    var taskCollection = taskCollectionProvider_.Get();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.TaskId == taskId)
                                 .Select(model => model.ParentTaskIds)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }
  }

  public async Task<string> RetryTask(TaskData          taskData,
                           CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(RetryTask)}");

    var taskCollection = taskCollectionProvider_.Get();

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
                                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(FinalizeTaskCreation)}");
    var taskCollection = taskCollectionProvider_.Get();

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
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      sessionProvider_.Get();
      taskCollectionProvider_.Get();
      isInitialized_ = true;
    }
    return Task.CompletedTask;
  }
}
