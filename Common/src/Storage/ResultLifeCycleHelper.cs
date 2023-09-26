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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Helper to manage Results
/// </summary>
public static class ResultLifeCycleHelper
{
  /// <summary>
  ///   Recursively abort results and put task in error when their dependencies are aborted
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskId">The id of the task to process</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task AbortTaskAndResults(ITaskTable        taskTable,
                                               IResultTable      resultTable,
                                               string            taskId,
                                               CancellationToken cancellationToken)
  {
    var taskData = await taskTable.ReadTaskAsync(taskId,
                                                 cancellationToken)
                                  .ConfigureAwait(false);

    if (taskData.Status is not (TaskStatus.Creating or TaskStatus.Cancelled or TaskStatus.Cancelling or TaskStatus.Error))
    {
      return;
    }

    if (taskData.Status is TaskStatus.Creating)
    {
      await taskTable.SetTaskErrorAsync(taskData with
                                        {
                                          EndDate = DateTime.UtcNow,
                                          CreationToEndDuration = DateTime.UtcNow - taskData.EndDate,
                                          ProcessingToEndDuration = DateTime.Now  - taskData.StartDate,
                                        },
                                        "One of the input data is aborted.",
                                        cancellationToken)
                     .ConfigureAwait(false);
    }

    taskTable.Logger.LogInformation("Abort results from {taskId}",
                                    taskData.TaskId);

    var creatingResults = await resultTable.GetResults(taskData.SessionId,
                                                       taskData.ExpectedOutputIds,
                                                       cancellationToken)
                                           .Where(result => result.OwnerTaskId == taskId)
                                           .Select(result => result.ResultId)
                                           .ToListAsync(cancellationToken)
                                           .ConfigureAwait(false);

    await resultTable.AbortTaskResults(taskData.SessionId,
                                       taskData.TaskId,
                                       cancellationToken)
                     .ConfigureAwait(false);

    var dependentTasks = await resultTable.GetResults(taskData.SessionId,
                                                      creatingResults,
                                                      cancellationToken)
                                          .Where(result => result.Status == ResultStatus.Aborted)
                                          .SelectMany(result => result.DependentTasks.ToAsyncEnumerable())
                                          .ToListAsync(cancellationToken)
                                          .ConfigureAwait(false);

    foreach (var task in (await taskTable.GetTaskStatus(dependentTasks,
                                                        cancellationToken)
                                         .ConfigureAwait(false)).Where(status => status.Status != TaskStatus.Error)
                                                                .Select(status => status.TaskId))
    {
      await AbortTaskAndResults(taskTable,
                                resultTable,
                                task,
                                cancellationToken)
        .ConfigureAwait(false);
    }
  }
}
