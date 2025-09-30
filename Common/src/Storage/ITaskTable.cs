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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

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
  /// <param name="selector">Expression to select part of the returned task data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task metadata of the retrieved task
  /// </returns>
  Task<T> ReadTaskAsync<T>(string                        taskId,
                           Expression<Func<TaskData, T>> selector,
                           CancellationToken             cancellationToken = default);


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
  ///   Remove tasks from the data base given their session id
  /// </summary>
  /// <param name="sessionId">Id of the session from which tasks should be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteTasksAsync(string            sessionId,
                        CancellationToken cancellationToken = default);

  /// <summary>
  ///   Remove tasks from the data base given their ids
  /// </summary>
  /// <param name="taskIds">Collection of task ids to be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteTasksAsync(ICollection<string> taskIds,
                        CancellationToken   cancellationToken = default);

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
  /// <remarks>
  ///   If <paramref name="pageSize" /> is 0, this function can be used to count the number of tasks
  ///   satisfying the condition specified by <paramref name="filter" />
  /// </remarks>
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
  /// <param name="filter">Additional filter on the task</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="before">Whether to return metadata before update</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The task metadata before the update or null if task not found
  /// </returns>
  Task<TaskData?> UpdateOneTask(string                            taskId,
                                Expression<Func<TaskData, bool>>? filter,
                                UpdateDefinition<TaskData>        updates,
                                bool                              before            = false,
                                CancellationToken                 cancellationToken = default);

  /// <summary>
  ///   Update the tasks matching the filter with the given new values
  /// </summary>
  /// <param name="filter">Filter to select the tasks to update</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of task matched
  /// </returns>
  Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                             UpdateDefinition<TaskData>       updates,
                             CancellationToken                cancellationToken = default);

  /// <summary>
  ///   Updates in bulk tasks
  /// </summary>
  /// <param name="bulkUpdates">Enumeration of updates with the filter they apply on</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of task matched
  /// </returns>
  async Task<long> BulkUpdateTasks(IEnumerable<(Expression<Func<TaskData, bool>> filter, UpdateDefinition<TaskData> updates)> bulkUpdates,
                                   CancellationToken                                                                          cancellationToken)
  {
    long n = 0;
    foreach (var (filter, updates) in bulkUpdates)
    {
      n += await UpdateManyTasks(filter,
                                 updates,
                                 cancellationToken)
             .ConfigureAwait(false);
    }

    return n;
  }

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
  ///   Remove data dependencies from remaining data dependencies, and returns tasks that are ready
  /// </summary>
  /// <param name="taskIds">Tasks</param>
  /// <param name="dependenciesToRemove">Dependencies</param>
  /// <param name="selector">Expression to select part of the returned task data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Projected tasks that are ready after the dependencies removal
  /// </returns>
  IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>           taskIds,
                                                              ICollection<string>           dependenciesToRemove,
                                                              Expression<Func<TaskData, T>> selector,
                                                              CancellationToken             cancellationToken = default);
}
