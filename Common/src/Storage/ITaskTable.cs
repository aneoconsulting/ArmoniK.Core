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
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Interface to manage tasks and their life cycle
///   in the data base
/// </summary>
public interface ITaskTable : IInitializable
{
  /// <summary>
  ///   Lower bound delay between two data base accesses
  ///   when active polling is employed to wait for a task
  ///   status to change
  /// </summary>
  TimeSpan PollingDelayMin { get; }

  /// <summary>
  ///   Upper bound delay between two data base accesses
  ///   when active polling is employed to wait for a task
  ///   status to change
  /// </summary>
  TimeSpan PollingDelayMax { get; }

  /// <summary>
  ///   Logger for class ITaskTable
  /// </summary>
  public ILogger Logger { get; }

  /// <summary>
  ///   Inserts a collection of tasks in the data base
  /// </summary>
  /// <param name="tasks">Collection of tasks to be inserted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task CreateTasks(IEnumerable<TaskData> tasks,
                   CancellationToken     cancellationToken = default);

  /// <summary>
  ///   Retrieves a task from the data base
  /// </summary>
  /// <param name="taskId">Id of the task to read</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task metadata of the retrieved task
  /// </returns>
  Task<TaskData> ReadTaskAsync(string            taskId,
                               CancellationToken cancellationToken = default);

  /// <summary>
  ///   Query a task status to check for cancellation
  /// </summary>
  /// <param name="taskId">Id of the task to check</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Boolean representing the cancellation status of the task
  /// </returns>
  Task<bool> IsTaskCancelledAsync(string            taskId,
                                  CancellationToken cancellationToken = default);

  /// <summary>
  ///   Update a task status to TaskStatus.Processing
  /// </summary>
  /// <remarks>
  ///   Updates:
  ///   - <see cref="TaskData.Status" />: New status of the task
  ///   - <see cref="TaskData.StartDate" />: Date when the task starts
  ///   - <see cref="TaskData.PodTtl" />: Date TTL on the pod
  /// </remarks>
  /// <param name="taskData">Metadata of the task to start</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task StartTask(TaskData          taskData,
                 CancellationToken cancellationToken = default);

  /// <summary>
  ///   Count tasks matching a given filter
  /// </summary>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tasks that matched the filter
  /// </returns>
  Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                     CancellationToken cancellationToken = default);

  /// <summary>
  ///   Count tasks matching a given filter
  /// </summary>
  /// <param name="filter">Filter expression describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tasks that matched the filter
  /// </returns>
  Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                     CancellationToken                cancellationToken = default);

  /// <summary>
  ///   Count tasks matching a given filter and group by partition and status
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tasks that matched the filter grouped by partition and status
  /// </returns>
  Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   Count tasks matching a given status
  /// </summary>
  /// <param name="status">Status of the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tasks that matched the status
  /// </returns>
  Task<int> CountAllTasksAsync(TaskStatus        status,
                               CancellationToken cancellationToken = default);

  /// <summary>
  ///   Remove a task from the data base given its id
  /// </summary>
  /// <param name="id">Id of the tasks to be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteTaskAsync(string            id,
                       CancellationToken cancellationToken = default);

  /// <summary>
  ///   List all tasks matching a given filter
  /// </summary>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   List of tasks that matched the filter
  /// </returns>
  IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                          CancellationToken cancellationToken = default);

  /// <summary>
  ///   List all tasks matching the given filter and ordering
  /// </summary>
  /// <param name="filter">Filter to select tasks</param>
  /// <param name="orderField">Select the field that will be used to order the tasks</param>
  /// <param name="selector">Expression to select part of the returned task data</param>
  /// <param name="ascOrder">Is the order ascending</param>
  /// <param name="page">The page of results to retrieve</param>
  /// <param name="pageSize">The number of results pages</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of task metadata matching the request and total number of results without paging
  /// </returns>
  Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                  Expression<Func<TaskData, object?>> orderField,
                                                                  Expression<Func<TaskData, T>>       selector,
                                                                  bool                                ascOrder,
                                                                  int                                 page,
                                                                  int                                 pageSize,
                                                                  CancellationToken                   cancellationToken = default);

  /// <summary>
  ///   Find all tasks matching the given filter and ordering
  /// </summary>
  /// <param name="filter">Filter to select tasks</param>
  /// <param name="selector">Expression to select part of the returned task data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task metadata matching the request and total number of results without paging
  /// </returns>
  IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                        Expression<Func<TaskData, T>>    selector,
                                        CancellationToken                cancellationToken = default);

  // TODO Should be compatible with EFCORE : https://learn.microsoft.com/en-us/ef/core/saving/execute-insert-update-delete#updating-multiple-properties
  /// <summary>
  ///   Update one task with the given new values
  /// </summary>
  /// <param name="taskId">Id of the tasks to be updated</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The task metadata before the update
  /// </returns>
  Task<TaskData> UpdateOneTask(string                                                                        taskId,
                               ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                               CancellationToken                                                             cancellationToken = default);

  /// <summary>
  ///   Update the tasks matching the filter with the given new values
  /// </summary>
  /// <param name="filter">Filter to select the tasks to update</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of task matched
  /// </returns>
  Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>                                              filter,
                             ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                             CancellationToken                                                             cancellationToken = default);

  /// <summary>
  ///   List all applications extracted from task metadata matching the given filter and ordering
  /// </summary>
  /// <param name="filter">Filter to select tasks</param>
  /// <param name="orderFields">Select the fields that will be used to order the tasks</param>
  /// <param name="ascOrder">Is the order ascending</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <param name="page">The page of results to retrieve</param>
  /// <param name="pageSize">The number of results pages</param>
  /// <returns>
  ///   Collection of applications metadata matching the request and total number of results without paging
  /// </returns>
  Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>>                    filter,
                                                                                      ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                      bool                                                ascOrder,
                                                                                      int                                                 page,
                                                                                      int                                                 pageSize,
                                                                                      CancellationToken                                   cancellationToken = default);


  /// <summary>
  ///   Remove data dependencies from remaining data dependencies
  /// </summary>
  /// <param name="taskIds">Tasks</param>
  /// <param name="dependenciesToRemove">Dependencies</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task RemoveRemainingDataDependenciesAsync(ICollection<string> taskIds,
                                            ICollection<string> dependenciesToRemove,
                                            CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Retrieve a task's output
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task's output
  /// </returns>
  Task<Output> GetTaskOutput(string            taskId,
                             CancellationToken cancellationToken = default);

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
  /// <param name="taskData">Metadata of the task to acquire</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Metadata of the task we try to acquire
  /// </returns>
  Task<TaskData> AcquireTask(TaskData          taskData,
                             CancellationToken cancellationToken = default);

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
  /// <param name="taskData">Metadata of the task to release</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Metadata of the task we try to release
  /// </returns>
  Task<TaskData> ReleaseTask(TaskData          taskData,
                             CancellationToken cancellationToken = default);

  /// <summary>
  ///   Get reply status metadata of a task given its id
  /// </summary>
  /// <param name="taskIds">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply status metadata
  /// </returns>
  Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskIds,
                                                                     CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Get expected output keys of tasks given their ids
  /// </summary>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The expected output keys
  /// </returns>
  IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                       CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Get expected output keys of a task given its id
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The expected output keys
  /// </returns>
  public async Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                                   CancellationToken cancellationToken = default)
  {
    try
    {
      return await GetTasksExpectedOutputKeys(new[]
                                              {
                                                taskId,
                                              },
                                              cancellationToken)
                   .Select(tuple => tuple.expectedOutputKeys)
                   .SingleAsync(cancellationToken)
                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found");
    }
  }

  /// <summary>
  ///   Get expected parent's ids of a task given its id
  /// </summary>
  /// <param name="taskId">Id of the target task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The parent's ids
  /// </returns>
  Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                             CancellationToken cancellationToken = default);

  /// <summary>
  ///   Retry a task identified by its meta data
  /// </summary>
  /// <param name="taskData">Task metadata of the task to retry</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The id of the freshly created task
  /// </returns>
  Task<string> RetryTask(TaskData          taskData,
                         CancellationToken cancellationToken = default);
}
