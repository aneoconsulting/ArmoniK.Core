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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

public static class TaskTableExtensions
{
  public static Task<int> CancelTasks(this ITaskTable   taskTable,
                                      TaskFilter        filter,
                                      CancellationToken cancellationToken = default)
    => taskTable.UpdateAllTaskStatusAsync(filter,
                                          TaskStatus.Cancelling,
                                          cancellationToken);

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
  public static Task SetTaskCanceledAsync(this ITaskTable   taskTable,
                                          TaskData          taskData,
                                          CancellationToken cancellationToken = default)
    => taskTable.UpdateOneTask(taskData.TaskId,
                               new List<(Expression<Func<TaskData, object?>> selector, object? newValue)>
                               {
                                 (data => data.Output, new Output(Error: "",
                                                                  Success: false)),
                                 (data => data.Status, TaskStatus.Cancelled),
                                 (tdm => tdm.EndDate, taskData.EndDate),
                                 (tdm => tdm.CreationToEndDuration, taskData.CreationToEndDuration),
                                 (tdm => tdm.ProcessingToEndDuration, taskData.ProcessingToEndDuration),
                               },
                               cancellationToken);

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
                                             new List<(Expression<Func<TaskData, object?>> selector, object? newValue)>
                                             {
                                               (data => data.Output, new Output(Error: errorDetail,
                                                                                Success: false)),
                                               (data => data.Status, TaskStatus.Error),
                                               (tdm => tdm.EndDate, taskData.EndDate),
                                               (tdm => tdm.CreationToEndDuration, taskData.CreationToEndDuration),
                                               (tdm => tdm.ProcessingToEndDuration, taskData.ProcessingToEndDuration),
                                             },
                                             cancellationToken)
                              .ConfigureAwait(false);

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
  public static Task SetTaskSuccessAsync(this ITaskTable   taskTable,
                                         TaskData          taskData,
                                         CancellationToken cancellationToken = default)
    => taskTable.UpdateOneTask(taskData.TaskId,
                               new List<(Expression<Func<TaskData, object?>> selector, object? newValue)>
                               {
                                 (data => data.Output, new Output(Error: "",
                                                                  Success: true)),
                                 (data => data.Status, TaskStatus.Completed),
                                 (tdm => tdm.EndDate, taskData.EndDate),
                                 (tdm => tdm.CreationToEndDuration, taskData.CreationToEndDuration),
                                 (tdm => tdm.ProcessingToEndDuration, taskData.ProcessingToEndDuration),
                               },
                               cancellationToken);
}
