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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
/// Interface to manage tasks and their life cycle
/// in the data base
/// </summary>
public interface ITaskTable : IInitializable
{
  TimeSpan PollingDelayMin { get; }
  TimeSpan PollingDelayMax { get; }

  /// <summary>
  /// Logger for this class
  /// </summary>
  public ILogger Logger { get; }

  /// <summary>
  /// Inserts a collection of tasks in the data base
  /// </summary>
  /// <param name="tasks">Collection of tasks to be inserted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task CreateTasks(IEnumerable<TaskData> tasks,
                   CancellationToken     cancellationToken = default);

  /// <summary>
  /// Retrieves a task from the data base
  /// </summary>
  /// <param name="taskId">Id of the task to read</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task metadata of the retrieved task
  /// </returns>
  Task<TaskData> ReadTaskAsync(string            taskId,
                               CancellationToken cancellationToken = default);

  /// <summary>
  /// Update the task status in the data base
  /// </summary>
  /// <param name="id">Id of the task whose status should be updated</param>
  /// <param name="status">The new task status</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task UpdateTaskStatusAsync(string            id,
                             TaskStatus        status,
                             CancellationToken cancellationToken = default);

  /// <summary>
  /// Update the statuses of all tasks matching a given filter
  /// </summary>
  /// <param name="filter">Task Filter describing the tasks whose status should be updated</param>
  /// <param name="status">The new task status</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The number of updated tasks
  /// </returns>
  Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                     TaskStatus        status,
                                     CancellationToken cancellationToken = default);
  /// <summary>
  ///  Query a task status to check for cancellation
  /// </summary>
  /// <param name="taskId">Id of the task to check</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Boolean representing the cancellation status of the task
  /// </returns>
  Task<bool> IsTaskCancelledAsync(string            taskId,
                                  CancellationToken cancellationToken = default);

  /// <summary>
  ///  Update a task status to TaskStatus.Processing
  /// </summary>
  /// <param name="taskId">Id of the task to start</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task StartTask(string            taskId,
                 CancellationToken cancellationToken = default);

  /// <summary>
  ///  Cancel all tasks in a given session
  /// </summary>
  /// <param name="sessionId">Id of the target session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task CancelSessionAsync(string            sessionId,
                          CancellationToken cancellationToken = default);

  /// <summary>
  /// Count tasks matching a given filter
  /// </summary>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The number of tasks that matched the filter
  /// </returns>
  Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                     CancellationToken cancellationToken = default);
  /// <summary>
  /// Count tasks matching a given status
  /// </summary>
  /// <param name="status">Status of the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The number of tasks that matched the status
  /// </returns>
  Task<int> CountAllTasksAsync(TaskStatus        status,
                               CancellationToken cancellationToken = default);

  /// <summary>
  /// Remove a task from the data base given its id
  /// </summary>
  /// <param name="id">Id of the tasks to be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteTaskAsync(string            id,
                       CancellationToken cancellationToken = default);

  /// <summary>
  /// List all tasks matching a given filter
  /// </summary>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// List of tasks that matched the filter
  /// </returns>
  IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                          CancellationToken cancellationToken);

  /// <summary>
  ///  Tag a task as succeded
  /// </summary>
  /// <param name="taskId">Id of the task to tag as succeeded</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task SetTaskSuccessAsync(string            taskId,
                           CancellationToken cancellationToken);

  /// <summary>
  ///  Tag a task as errored and populate its output with an
  ///  error message
  /// </summary>
  /// <param name="taskId">Id of the task to mark as errored</param>
  /// <param name="errorDetail">Error message to be inserted in task's output</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task SetTaskErrorAsync(string            taskId,
                         string            errorDetail,
                         CancellationToken cancellationToken);

  /// <summary>
  ///  Retrieve a task's output
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task's output
  /// </returns>
  Task<Output> GetTaskOutput(string            taskId,
                             CancellationToken cancellationToken = default);

  /// <summary>
  /// Query acquired status of a task
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Boolean representing the acquired status of the task
  /// </returns>
  Task<bool> AcquireTask(string            taskId,
                         CancellationToken cancellationToken = default);

  /// <summary>
  /// Get reply status metadata of a task given its id
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Reply status metadata
  /// </returns>
  Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskId,
                                                                     CancellationToken   cancellationToken = default);
  /// <summary>
  /// Get expected output keys of a task given its id
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The expected output keys
  /// </returns>
  Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                      CancellationToken cancellationToken = default);

  /// <summary>
  /// Get expected parent's ids of a task given its id
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The parent's ids
  /// </returns>
  Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                             CancellationToken cancellationToken);

  /// <summary>
  /// Retry a task identified by its meta data
  /// </summary>
  /// <param name="taskData">Task metadata of the task to retry</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The id of the freshly created task
  /// </returns>
  Task<string> RetryTask(TaskData          taskData,
                         CancellationToken cancellationToken);

  /// <summary>
  /// Tag a collection of tasks as created
  /// </summary>
  /// <param name="taskIds">Task ids whose creation will be finalised</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// The number of tagged tasks by the function
  /// </returns>
  Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                 CancellationToken   cancellationToken = default);

}
