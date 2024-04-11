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

using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Helper to manage Results
/// </summary>
public static class ResultLifeCycleHelper
{
  /// <summary>
  ///   Abort all tasks and results that depend directly or indirectly of the specified tasks
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskIds">Root tasks that must be aborted</param>
  /// <param name="reason">Abortion message</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task AbortTasksAndResults(ITaskTable          taskTable,
                                                IResultTable        resultTable,
                                                ICollection<string> taskIds,
                                                string?             reason,
                                                CancellationToken   cancellationToken)
  {
    // Early exit if no task is requested for abortion
    if (!taskIds.Any())
    {
      return;
    }

    reason ??= $"Tasks {string.Join(", ", taskIds)} have been explicitly aborted";

    taskTable.Logger.LogInformation("Abort tasks {@tasks}: {reason}",
                                    taskIds,
                                    reason);

    // Find all metadata about the tasks that must be aborted
    var tasks = await taskTable.FindTasksAsync(task => taskIds.Contains(task.TaskId) &&
                                                       (task.Status == TaskStatus.Creating   || task.Status == TaskStatus.Cancelled ||
                                                        task.Status == TaskStatus.Cancelling || task.Status == TaskStatus.Error || task.Status == TaskStatus.Timeout),
                                               task => new
                                                       {
                                                         task.TaskId,
                                                         task.Status,
                                                         task.ExpectedOutputIds,
                                                         task.CreationDate,
                                                         task.ProcessedDate,
                                                         task.ReceptionDate,
                                                       },
                                               cancellationToken)
                               .ToListAsync(cancellationToken)
                               .ConfigureAwait(false);

    // No eligible task for abortion
    if (!tasks.Any())
    {
      return;
    }

    // Abort all the eligible tasks
    var now = DateTime.UtcNow;
    await taskTable.BulkUpdateTasks(tasks.Select(task =>
                                                 {
                                                   Expression<Func<TaskData, bool>> filter = td => td.TaskId == task.TaskId && td.Status == TaskStatus.Creating;
                                                   var updates = new UpdateDefinition<TaskData>().Set(td => td.Status,
                                                                                                      TaskStatus.Error)
                                                                                                 .Set(td => td.Output,
                                                                                                      new Output(Error: reason,
                                                                                                                 Status: OutputStatus.Error))
                                                                                                 .Set(td => td.EndDate,
                                                                                                      now)
                                                                                                 .Set(td => td.CreationToEndDuration,
                                                                                                      now - task.CreationDate)
                                                                                                 .Set(td => td.ProcessingToEndDuration,
                                                                                                      now - task.ProcessedDate)
                                                                                                 .Set(td => td.ReceivedToEndDuration,
                                                                                                      now - task.ReceptionDate);
                                                   return (filter, updates);
                                                 }),
                                    cancellationToken)
                   .ConfigureAwait(false);

    // All dependent results
    var resultIds = tasks.SelectMany(task => task.ExpectedOutputIds)
                         .ToHashSet();

    taskTable.Logger.LogInformation("Abort results {@results}: {reason}",
                                    resultIds,
                                    reason);

    var count = await resultTable
                      .UpdateManyResults(result => resultIds.Contains(result.ResultId) && taskIds.Contains(result.OwnerTaskId) && result.Status == ResultStatus.Created,
                                         new UpdateDefinition<Result>().Set(result => result.Status,
                                                                            ResultStatus.Aborted),
                                         cancellationToken)
                      .ConfigureAwait(false);

    // Early exit if no result has been aborted
    if (count == 0)
    {
      return;
    }

    // Find all the tasks that depend on the aborted results
    var nextTasks = new HashSet<string>();
    await foreach (var dependentTasks in resultTable.GetResults(result => resultIds.Contains(result.ResultId) && result.Status == ResultStatus.Aborted &&
                                                                          taskIds.Contains(result.OwnerTaskId),
                                                                result => result.DependentTasks,
                                                                cancellationToken)
                                                    .ConfigureAwait(false))
    {
      nextTasks.UnionWith(dependentTasks);
    }

    // Recursively abort the tasks that indirectly depend on the aborted tasks
    await AbortTasksAndResults(taskTable,
                               resultTable,
                               nextTasks,
                               reason,
                               cancellationToken)
      .ConfigureAwait(false);
  }

  public static async Task PurgeResultsAsync(IResultTable      resultTable,
                                             IObjectStorage    objectStorage,
                                             string            sessionId,
                                             CancellationToken cancellationToken)
  {
    await foreach (var ids in resultTable.GetResults(result => result.SessionId == sessionId,
                                                     result => result.ResultId,
                                                     cancellationToken)
                                         .ToChunksAsync(500,
                                                        Timeout.InfiniteTimeSpan,
                                                        cancellationToken)
                                         .ConfigureAwait(false))
    {
      await objectStorage.TryDeleteAsync(ids,
                                         cancellationToken)
                         .ConfigureAwait(false);
    }

    await resultTable.UpdateManyResults(result => result.SessionId == sessionId,
                                        new UpdateDefinition<Result>().Set(result => result.Status,
                                                                           ResultStatus.DeletedData),
                                        cancellationToken)
                     .ConfigureAwait(false);
  }
}
