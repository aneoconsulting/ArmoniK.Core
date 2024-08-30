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
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

public static class TaskTableExtensions
{
  private static readonly TaskStatus[] FinalStatus =
  {
    TaskStatus.Completed,
    TaskStatus.Cancelled,
    TaskStatus.Error,
    TaskStatus.Retried,
    TaskStatus.Timeout,
  };

  private static readonly Expression<Func<TaskData, TaskData>> Identity = data => data;

  /// <summary>
  ///   Change the status of the task to canceled
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.EndDate" />: Date when the task ends
  ///   - <see cref="TaskData.CreationToEndDuration" />: Duration between the creation and the end of the task
  ///   - <see cref="TaskData.ProcessingToEndDuration" />: Duration between the start and the end of the task
  ///   - <see cref="TaskData.Output" />: Output of the task
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to tag as succeeded</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task SetTaskCanceledAsync(this ITaskTable   taskTable,
                                                TaskData          taskData,
                                                CancellationToken cancellationToken = default)
  {
    await taskTable.UpdateOneTask(taskData.TaskId,
                                  new UpdateDefinition<TaskData>().Set(data => data.Output,
                                                                       new Output(Error: "",
                                                                                  Status: OutputStatus.Error))
                                                                  .Set(data => data.Status,
                                                                       TaskStatus.Cancelled)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       taskData.EndDate)
                                                                  .Set(tdm => tdm.ProcessedDate,
                                                                       taskData.ProcessedDate)
                                                                  .Set(tdm => tdm.ReceivedToEndDuration,
                                                                       taskData.ReceivedToEndDuration)
                                                                  .Set(tdm => tdm.CreationToEndDuration,
                                                                       taskData.CreationToEndDuration)
                                                                  .Set(tdm => tdm.ProcessingToEndDuration,
                                                                       taskData.ProcessingToEndDuration),
                                  cancellationToken)
                   .ConfigureAwait(false);
    taskTable.Logger.LogDebug("Update {task} to {status}",
                              taskData.TaskId,
                              TaskStatus.Cancelled);
  }

  /// <summary>
  ///   Tag a task as errored and populate its output with an
  ///   error message
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.EndDate" />: Date when the task ends
  ///   - <see cref="TaskData.CreationToEndDuration" />: Duration between the creation and the end of the task
  ///   - <see cref="TaskData.ProcessingToEndDuration" />: Duration between the start and the end of the task
  ///   - <see cref="TaskData.Output" />: Output of the task
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to mark as errored</param>
  /// <param name="errorDetail">Error message to be inserted in task's output</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A boolean representing whether the status has been updated
  /// </returns>
  public static async Task<bool> SetTaskErrorAsync(this ITaskTable   taskTable,
                                                   TaskData          taskData,
                                                   string            errorDetail,
                                                   CancellationToken cancellationToken = default)
  {
    var task = await taskTable.UpdateOneTask(taskData.TaskId,
                                             new UpdateDefinition<TaskData>().Set(data => data.Output,
                                                                                  new Output(Error: errorDetail,
                                                                                             Status: OutputStatus.Error))
                                                                             .Set(data => data.Status,
                                                                                  TaskStatus.Error)
                                                                             .Set(tdm => tdm.EndDate,
                                                                                  taskData.EndDate)
                                                                             .Set(tdm => tdm.ProcessedDate,
                                                                                  taskData.ProcessedDate)
                                                                             .Set(tdm => tdm.ReceivedToEndDuration,
                                                                                  taskData.ReceivedToEndDuration)
                                                                             .Set(tdm => tdm.CreationToEndDuration,
                                                                                  taskData.CreationToEndDuration)
                                                                             .Set(tdm => tdm.ProcessingToEndDuration,
                                                                                  taskData.ProcessingToEndDuration),
                                             cancellationToken)
                              .ConfigureAwait(false);

    taskTable.Logger.LogDebug("Update {task} to {status}",
                              taskData.TaskId,
                              TaskStatus.Error);

    return task.Status != TaskStatus.Error;
  }

  /// <summary>
  ///   Change the status of the task to succeeded
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.EndDate" />: Date when the task ends
  ///   - <see cref="TaskData.CreationToEndDuration" />: Duration between the creation and the end of the task
  ///   - <see cref="TaskData.ProcessingToEndDuration" />: Duration between the start and the end of the task
  ///   - <see cref="TaskData.Output" />: Output of the task
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to tag as succeeded</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task SetTaskSuccessAsync(this ITaskTable   taskTable,
                                               TaskData          taskData,
                                               CancellationToken cancellationToken = default)
  {
    await taskTable.UpdateOneTask(taskData.TaskId,
                                  new UpdateDefinition<TaskData>().Set(data => data.Output,
                                                                       new Output(Error: "",
                                                                                  Status: OutputStatus.Success))
                                                                  .Set(data => data.Status,
                                                                       TaskStatus.Completed)
                                                                  .Set(tdm => tdm.EndDate,
                                                                       taskData.EndDate)
                                                                  .Set(tdm => tdm.ProcessedDate,
                                                                       taskData.ProcessedDate)
                                                                  .Set(tdm => tdm.ReceivedToEndDuration,
                                                                       taskData.ReceivedToEndDuration)
                                                                  .Set(tdm => tdm.CreationToEndDuration,
                                                                       taskData.CreationToEndDuration)
                                                                  .Set(tdm => tdm.ProcessingToEndDuration,
                                                                       taskData.ProcessingToEndDuration),
                                  cancellationToken)
                   .ConfigureAwait(false);
    taskTable.Logger.LogDebug("Update {task} to {status}",
                              taskData.TaskId,
                              TaskStatus.Completed);
  }


  /// <summary>
  ///   Change the status of the task to retry
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.EndDate" />: Date when the task ends
  ///   - <see cref="TaskData.CreationToEndDuration" />: Duration between the creation and the end of the task
  ///   - <see cref="TaskData.ProcessingToEndDuration" />: Duration between the start and the end of the task
  ///   - <see cref="TaskData.Output" />: Output of the task
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to tag as succeeded</param>
  /// <param name="errorDetail">Error message to be inserted in task's output</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A boolean representing whether the status has been updated
  /// </returns>
  public static async Task<bool> SetTaskRetryAsync(this ITaskTable   taskTable,
                                                   TaskData          taskData,
                                                   string            errorDetail,
                                                   CancellationToken cancellationToken = default)
  {
    var task = await taskTable.UpdateOneTask(taskData.TaskId,
                                             new UpdateDefinition<TaskData>().Set(data => data.Output,
                                                                                  new Output(Error: errorDetail,
                                                                                             Status: OutputStatus.Error))
                                                                             .Set(data => data.Status,
                                                                                  TaskStatus.Retried)
                                                                             .Set(tdm => tdm.EndDate,
                                                                                  taskData.EndDate)
                                                                             .Set(tdm => tdm.ProcessedDate,
                                                                                  taskData.ProcessedDate)
                                                                             .Set(tdm => tdm.ReceivedToEndDuration,
                                                                                  taskData.ReceivedToEndDuration)
                                                                             .Set(tdm => tdm.CreationToEndDuration,
                                                                                  taskData.CreationToEndDuration)
                                                                             .Set(tdm => tdm.ProcessingToEndDuration,
                                                                                  taskData.ProcessingToEndDuration),
                                             cancellationToken)
                              .ConfigureAwait(false);

    taskTable.Logger.LogDebug("Update {task} to {status}",
                              taskData.TaskId,
                              TaskStatus.Retried);

    return task.Status != TaskStatus.Retried;
  }

  /// <summary>
  ///   Change the status of the task to retry
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.EndDate" />: Date when the task ends
  ///   - <see cref="TaskData.CreationToEndDuration" />: Duration between the creation and the end of the task
  ///   - <see cref="TaskData.ProcessingToEndDuration" />: Duration between the start and the end of the task
  ///   - <see cref="TaskData.Output" />: Output of the task
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to tag as succeeded</param>
  /// <param name="output">Task Output</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A boolean representing whether the status has been updated
  /// </returns>
  public static async Task<bool> SetTaskTimeoutAsync(this ITaskTable   taskTable,
                                                     TaskData          taskData,
                                                     Output            output,
                                                     CancellationToken cancellationToken = default)
  {
    var task = await taskTable.UpdateOneTask(taskData.TaskId,
                                             new UpdateDefinition<TaskData>().Set(data => data.Output,
                                                                                  output)
                                                                             .Set(data => data.Status,
                                                                                  TaskStatus.Timeout)
                                                                             .Set(tdm => tdm.EndDate,
                                                                                  taskData.EndDate)
                                                                             .Set(tdm => tdm.ProcessedDate,
                                                                                  taskData.ProcessedDate)
                                                                             .Set(tdm => tdm.ReceivedToEndDuration,
                                                                                  taskData.ReceivedToEndDuration)
                                                                             .Set(tdm => tdm.CreationToEndDuration,
                                                                                  taskData.CreationToEndDuration)
                                                                             .Set(tdm => tdm.ProcessingToEndDuration,
                                                                                  taskData.ProcessingToEndDuration),
                                             cancellationToken)
                              .ConfigureAwait(false);

    taskTable.Logger.LogDebug("Update {task} to {status}",
                              taskData.TaskId,
                              TaskStatus.Timeout);

    return task.Status != TaskStatus.Timeout;
  }

  /// <summary>
  ///   Cancels all tasks in a given session
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="sessionId">Id of the target session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task CancelSessionAsync(this ITaskTable   taskTable,
                                              string            sessionId,
                                              CancellationToken cancellationToken = default)
  {
    await taskTable.UpdateManyTasks(data => data.SessionId == sessionId && !FinalStatus.Contains(data.Status),
                                    new UpdateDefinition<TaskData>().Set(tdm => tdm.Status,
                                                                         TaskStatus.Cancelling)
                                                                    .Set(tdm => tdm.EndDate,
                                                                         DateTime.UtcNow),
                                    cancellationToken)
                   .ConfigureAwait(false);

    taskTable.Logger.LogInformation("Cancel {session}",
                                    sessionId);
  }


  /// <summary>
  ///   Cancels all the given tasks that are not in a final status
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of task matched
  /// </returns>
  public static async Task<long> CancelTaskAsync(this ITaskTable     taskTable,
                                                 ICollection<string> taskIds,
                                                 CancellationToken   cancellationToken = default)
  {
    var res = await taskTable.UpdateManyTasks(data => taskIds.Contains(data.TaskId) &&
                                                      !(data.Status == TaskStatus.Cancelled || data.Status == TaskStatus.Cancelling || data.Status == TaskStatus.Error ||
                                                        data.Status == TaskStatus.Completed || data.Status == TaskStatus.Retried || data.Status == TaskStatus.Timeout),
                                              new UpdateDefinition<TaskData>().Set(tdm => tdm.Status,
                                                                                   TaskStatus.Cancelling)
                                                                              .Set(tdm => tdm.EndDate,
                                                                                   DateTime.UtcNow),
                                              cancellationToken)
                             .ConfigureAwait(false);

    taskTable.Logger.LogInformation("Cancel {tasks}",
                                    taskIds);

    return res;
  }

  /// <summary>
  ///   Tag a collection of tasks as submitted
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskIds">Task ids whose creation will be finalized</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tagged tasks by the function
  /// </returns>
  public static async Task<long> FinalizeTaskCreation(this ITaskTable     taskTable,
                                                      ICollection<string> taskIds,
                                                      CancellationToken   cancellationToken = default)
  {
    var res = await taskTable.UpdateManyTasks(tdm => taskIds.Contains(tdm.TaskId) && tdm.Status == TaskStatus.Creating,
                                              new UpdateDefinition<TaskData>().Set(tdm => tdm.Status,
                                                                                   TaskStatus.Submitted)
                                                                              .Set(tdm => tdm.SubmittedDate,
                                                                                   DateTime.UtcNow),
                                              cancellationToken)
                             .ConfigureAwait(false);

    taskTable.Logger.LogDebug("Update {tasks} to {status}",
                              taskIds,
                              TaskStatus.Submitted);

    return res;
  }

  /// <summary>
  ///   Retrieves a task from the data base
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskId">Id of the task to read</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task metadata of the retrieved task
  /// </returns>
  public static async Task<TaskData> ReadTaskAsync(this ITaskTable   taskTable,
                                                   string            taskId,
                                                   CancellationToken cancellationToken = default)
  {
    var task = await taskTable.ReadTaskAsync(taskId,
                                             Identity,
                                             cancellationToken)
                              .ConfigureAwait(false);
    taskTable.Logger.LogDebug("Read {task}",
                              task);
    return task;
  }


  /// <summary>
  ///   Retrieve a task's output
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task's output
  /// </returns>
  public static Task<Output> GetTaskOutput(this ITaskTable   taskTable,
                                           string            taskId,
                                           CancellationToken cancellationToken = default)
    => taskTable.ReadTaskAsync(taskId,
                               data => data.Output,
                               cancellationToken);

  /// <summary>
  ///   Get reply status metadata of a task given its id
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskIds">Ids of the target tasks</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply status metadata
  /// </returns>
  public static IAsyncEnumerable<TaskIdStatus> GetTaskStatus(this ITaskTable     taskTable,
                                                             IEnumerable<string> taskIds,
                                                             CancellationToken   cancellationToken = default)
    => taskTable.FindTasksAsync(data => taskIds.Contains(data.TaskId),
                                data => new TaskIdStatus(data.TaskId,
                                                         data.Status),
                                cancellationToken);

  /// <summary>
  ///   Get reply status metadata of a task given its id
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply status metadata
  /// </returns>
  public static Task<TaskStatus> GetTaskStatus(this ITaskTable   taskTable,
                                               string            taskId,
                                               CancellationToken cancellationToken = default)
    => taskTable.ReadTaskAsync(taskId,
                               data => data.Status,
                               cancellationToken);

  /// <summary>
  ///   Get expected output keys of tasks given their ids
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The expected output keys
  /// </returns>
  public static IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(this ITaskTable     taskTable,
                                                                                                                     IEnumerable<string> taskIds,
                                                                                                                     CancellationToken   cancellationToken = default)
    => taskTable.FindTasksAsync(data => taskIds.Contains(data.TaskId),
                                data => ValueTuple.Create(data.TaskId,
                                                          data.ExpectedOutputIds),
                                cancellationToken)
                .Select(tuple => (tuple.Item1, tuple.Item2.AsEnumerable()));

  /// <summary>
  ///   Get expected parent's ids of a task given its id
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The parent's ids
  /// </returns>
  public static async Task<IEnumerable<string>> GetParentTaskIds(this ITaskTable   taskTable,
                                                                 string            taskId,
                                                                 CancellationToken cancellationToken = default)
    => await taskTable.ReadTaskAsync(taskId,
                                     data => data.ParentTaskIds,
                                     cancellationToken)
                      .ConfigureAwait(false);

  /// <summary>
  ///   Retry a task identified by its meta data
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Task metadata of the task to retry</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The id of the freshly created task
  /// </returns>
  public static async Task<string> RetryTask(this ITaskTable   taskTable,
                                             TaskData          taskData,
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
                                   "",
                                   taskData.PayloadId,
                                   taskData.ParentTaskIds,
                                   taskData.DataDependencies,
                                   taskData.RemainingDataDependencies,
                                   taskData.ExpectedOutputIds,
                                   taskData.InitialTaskId,
                                   taskData.TaskId,
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
                                   null,
                                   null,
                                   null,
                                   new Output(OutputStatus.Error,
                                              ""));
    await taskTable.CreateTasks(new[]
                                {
                                  newTaskData,
                                },
                                cancellationToken)
                   .ConfigureAwait(false);

    return newTaskId;
  }

  /// <summary>
  ///   Update one task with the given new values
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskId">Id of the tasks to be updated</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The task metadata before the update
  /// </returns>
  /// <exception cref="TaskNotFoundException">task not found</exception>
  private static async Task<TaskData> UpdateOneTask(this ITaskTable            taskTable,
                                                    string                     taskId,
                                                    UpdateDefinition<TaskData> updates,
                                                    CancellationToken          cancellationToken = default)
    => await taskTable.UpdateOneTask(taskId,
                                     null,
                                     updates,
                                     true,
                                     cancellationToken)
                      .ConfigureAwait(false) ?? throw new TaskNotFoundException($"Task not found {taskId}");


  /// <summary>
  ///   Acquire the task to process it on the current agent
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.OwnerPodId" />: Identifier (Ip) that will be used to reach the pod if another pod tries to
  ///   acquire the task
  ///   - <see cref="TaskData.OwnerPodName" />: Hostname of the pollster
  ///   - <see cref="TaskData.ReceptionDate" />: Date when the message from the queue storage is received
  ///   - <see cref="TaskData.AcquisitionDate" />: Date when the task is acquired
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to acquire</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Metadata of the task we try to acquire
  /// </returns>
  public static async Task<TaskData> AcquireTask(this ITaskTable   taskTable,
                                                 TaskData          taskData,
                                                 CancellationToken cancellationToken = default)
  {
    var res = await taskTable.UpdateOneTask(taskData.TaskId,
                                            x => x.OwnerPodId == "" && x.Status == TaskStatus.Submitted,
                                            new UpdateDefinition<TaskData>().Set(tdm => tdm.OwnerPodId,
                                                                                 taskData.OwnerPodId)
                                                                            .Set(tdm => tdm.OwnerPodName,
                                                                                 taskData.OwnerPodName)
                                                                            .Set(tdm => tdm.ReceptionDate,
                                                                                 taskData.ReceptionDate)
                                                                            .Set(tdm => tdm.AcquisitionDate,
                                                                                 taskData.AcquisitionDate)
                                                                            .Set(tdm => tdm.Status,
                                                                                 TaskStatus.Dispatched),
                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    taskTable.Logger.LogInformation("Acquire task {task} on {podName} with {success}",
                                    taskData.TaskId,
                                    taskData.OwnerPodId,
                                    res is not null);

    return res ?? await taskTable.ReadTaskAsync(taskData.TaskId,
                                                cancellationToken)
                                 .ConfigureAwait(false);
  }

  /// <summary>
  ///   Release the task from the current agent
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.OwnerPodId" />: Identifier (Ip) that will be used to reach the pod if another pod tries to
  ///   acquire the task
  ///   - <see cref="TaskData.OwnerPodName" />: Hostname of the pollster
  ///   - <see cref="TaskData.ReceptionDate" />: Date when the message from the queue storage is received
  ///   - <see cref="TaskData.AcquisitionDate" />: Date when the task is acquired
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to release</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Metadata of the task we try to release
  /// </returns>
  public static async Task<TaskData> ReleaseTask(this ITaskTable   taskTable,
                                                 TaskData          taskData,
                                                 CancellationToken cancellationToken = default)
  {
    var res = await taskTable.UpdateOneTask(taskData.TaskId,
                                            data => data.OwnerPodId == taskData.OwnerPodId,
                                            new UpdateDefinition<TaskData>().Set(data => data.OwnerPodId,
                                                                                 "")
                                                                            .Set(data => data.OwnerPodName,
                                                                                 "")
                                                                            .Set(data => data.ReceptionDate,
                                                                                 null)
                                                                            .Set(data => data.AcquisitionDate,
                                                                                 null)
                                                                            .Set(data => data.Status,
                                                                                 TaskStatus.Submitted),
                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    taskTable.Logger.LogInformation("Released task {task} on {podName}",
                                    taskData.TaskId,
                                    taskData.OwnerPodId);

    taskTable.Logger.LogDebug("Released task {taskData}",
                              res);

    if (taskTable.Logger.IsEnabled(LogLevel.Debug) && res is null)
    {
      taskTable.Logger.LogDebug("Released task (old) {taskData}",
                                await taskTable.ReadTaskAsync(taskData.TaskId,
                                                              cancellationToken)
                                               .ConfigureAwait(false));
    }

    return res ?? await taskTable.ReadTaskAsync(taskData.TaskId,
                                                cancellationToken)
                                 .ConfigureAwait(false);
  }

  /// <summary>
  ///   Updates in bulk tasks
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="bulkUpdates">Enumeration of updates with the taskId they apply on</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of task matched
  /// </returns>
  public static Task<long> BulkUpdateTasks(this ITaskTable                                                  taskTable,
                                           IEnumerable<(string taskId, UpdateDefinition<TaskData> updates)> bulkUpdates,
                                           CancellationToken                                                cancellationToken)
    => taskTable.BulkUpdateTasks(bulkUpdates.Select(item => ((Expression<Func<TaskData, bool>>)(task => task.TaskId == item.taskId), item.updates)),
                                 cancellationToken);

  /// <summary>
  ///   Update a task status to TaskStatus.Processing
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.StartDate" />: Date when the task starts
  ///   - <see cref="TaskData.PodTtl" />: Date TTL on the pod
  /// </remarks>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="taskData">Metadata of the task to start</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task StartTask(this ITaskTable   taskTable,
                                     TaskData          taskData,
                                     CancellationToken cancellationToken = default)
  {
    var res = await taskTable.UpdateOneTask(taskData.TaskId,
                                            data => data.Status == TaskStatus.Dispatched,
                                            new UpdateDefinition<TaskData>().Set(data => data.PodTtl,
                                                                                 taskData.PodTtl)
                                                                            .Set(data => data.StartDate,
                                                                                 taskData.StartDate)
                                                                            .Set(data => data.FetchedDate,
                                                                                 taskData.FetchedDate)
                                                                            .Set(tdm => tdm.Status,
                                                                                 TaskStatus.Processing),
                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    taskTable.Logger.LogInformation("Start task {taskId} and update to status {status}",
                                    taskData.TaskId,
                                    TaskStatus.Processing);

    if (res is null)
    {
      var taskStatus = await taskTable.ReadTaskAsync(taskData.TaskId,
                                                     data => data.Status,
                                                     cancellationToken)
                                      .ConfigureAwait(false);

      throw new ArmoniKException($"Fail to start task because task was not acquired - {taskStatus} to {TaskStatus.Processing}");
    }
  }
}
