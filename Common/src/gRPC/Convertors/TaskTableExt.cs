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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class TaskTableExt
{
  /// <summary>
  ///   Count tasks matching a given filter
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of tasks that matched the filter
  /// </returns>
  public static Task<IEnumerable<TaskStatusCount>> CountTasksAsync(this ITaskTable   taskTable,
                                                                   TaskFilter        filter,
                                                                   CancellationToken cancellationToken = default)
    => taskTable.CountTasksAsync(filter.ToFilterExpression(),
                                 cancellationToken);

  /// <summary>
  ///   List all tasks matching a given filter
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="filter">Task Filter describing the tasks to be counted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   List of tasks that matched the filter
  /// </returns>
  public static IAsyncEnumerable<string> ListTasksAsync(this ITaskTable   taskTable,
                                                        TaskFilter        filter,
                                                        CancellationToken cancellationToken = default)
    => taskTable.FindTasksAsync(filter.ToFilterExpression(),
                                data => data.TaskId,
                                cancellationToken);

  /// <summary>
  ///   Update the statuses of all tasks matching a given filter
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="filter">Task Filter describing the tasks whose status should be updated</param>
  /// <param name="status">The new task status</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of updated tasks
  /// </returns>
  public static async Task<long> UpdateAllTaskStatusAsync(this ITaskTable   taskTable,
                                                          TaskFilter        filter,
                                                          TaskStatus        status,
                                                          CancellationToken cancellationToken = default)
  {
    if (filter.Included is not null && (filter.Included.Statuses.Contains(TaskStatus.Completed.ToGrpcStatus()) ||
                                        filter.Included.Statuses.Contains(TaskStatus.Cancelled.ToGrpcStatus()) ||
                                        filter.Included.Statuses.Contains(TaskStatus.Error.ToGrpcStatus())     ||
                                        filter.Included.Statuses.Contains(TaskStatus.Retried.ToGrpcStatus())))
    {
      throw new ArmoniKException("The given TaskFilter contains a terminal state, update forbidden");
    }

    return await taskTable.UpdateManyTasks(filter.ToFilterExpression(),
                                           new UpdateDefinition<TaskData>().Set(tdm => tdm.Status,
                                                                                status),
                                           cancellationToken)
                          .ConfigureAwait(false);
  }

  /// <summary>
  ///   Cancel the tasks matching the given filter
  /// </summary>
  /// <param name="taskTable">Interface to manage tasks lifecycle</param>
  /// <param name="filter">Task Filter describing the tasks that should be cancelled</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of cancelled tasks
  /// </returns>
  public static async Task<int> CancelTasks(this ITaskTable   taskTable,
                                            TaskFilter        filter,
                                            CancellationToken cancellationToken = default)
    => (int)await taskTable.UpdateAllTaskStatusAsync(filter,
                                                     TaskStatus.Cancelling,
                                                     cancellationToken)
                           .ConfigureAwait(false);
}
