// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Core.Base;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Helper to manage Results
/// </summary>
public static class ResultLifeCycleHelper
{
  /// <summary>
  ///   Terminate all tasks and results that depend directly or indirectly of the specified tasks.
  ///   Tasks are set to an error status (or deleted if requested), and their results are marked as aborted.
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskIds">Root tasks that must be terminated</param>
  /// <param name="resultIds">Root results that must be terminated</param>
  /// <param name="reason">Termination message</param>
  /// <param name="status">Put terminated tasks into the given status</param>
  /// <param name="shouldDeleteTasks">Should delete tasks from database</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task TerminateTasksAndResults(ITaskTable taskTable,
                                                     IResultTable resultTable,
                                                     ICollection<string>? taskIds = null,
                                                     ICollection<string>? resultIds = null,
                                                     string? reason = null,
                                                     TaskStatus status = TaskStatus.Error,
                                                     bool shouldDeleteTasks = false,
                                                     CancellationToken cancellationToken = default)
  {
    taskIds ??= Array.Empty<string>();
    resultIds ??= Array.Empty<string>();

    // Early exit if no termination is actually requested
    if (taskIds.Count == 0 && resultIds.Count == 0)
    {
      return;
    }

    if (reason is null)
    {
      if (taskIds.Count == 0)
      {
        reason = $"Results {string.Join(", ", resultIds)} have been explicitly terminated.";
      }
      else if (resultIds.Count == 0)
      {
        reason = $"Tasks {string.Join(", ", taskIds)} have been explicitly terminated";
      }
      else
      {
        reason = $"Tasks {string.Join(", ", taskIds)} and Results {string.Join(", ", resultIds)} have been explicitly terminated";
      }
    }

    if (taskIds.Any())
    {
      taskTable.Logger.LogInformation("Terminate tasks {@tasks}: {reason}",
                                      taskIds,
                                      reason);

      var taskStatusToSeek = new HashSet<TaskStatus>
      {
        TaskStatus.Creating,
        TaskStatus.Pending,
        TaskStatus.Paused,
        TaskStatus.Cancelled,
        TaskStatus.Cancelling,
        TaskStatus.Error,
        TaskStatus.Timeout
      };

      // Find all metadata about the tasks that must be terminated
      var tasks = await taskTable.FindTasksAsync(task => taskIds.Contains(task.TaskId) &&
                                                         taskStatusToSeek.Contains(task.Status),
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

      if (tasks.Any())
      {
        // Terminate all the eligible tasks
        var now = DateTime.UtcNow;
        if (shouldDeleteTasks)
        {
          await taskTable.DeleteTasksAsync(taskIds,
                                       cancellationToken)
                     .ConfigureAwait(false);
        }
        else
        {
          await taskTable.BulkUpdateTasks(tasks.Select(task =>
          {
            Expression<Func<TaskData, bool>> filter =
              td => td.TaskId == task.TaskId && (td.Status == TaskStatus.Creating || td.Status == TaskStatus.Pending);
            var updates = new UpdateDefinition<TaskData>().Set(td => td.Status,
                                                               status)
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
        }
        // All dependent results
        resultIds = tasks.SelectMany(task => task.ExpectedOutputIds)
                         .Concat(resultIds)
                         .ToHashSet();
      }
    }

    // Early exit if no results need to be terminated (recursion is not needed)
    if (!resultIds.Any())
    {
      return;
    }

    taskTable.Logger.LogInformation("Terminate results {@results}: {reason}",
                                    resultIds,
                                    reason);

    var count = await resultTable.UpdateManyResults(result => resultIds.Contains(result.ResultId) &&
                                                              (taskIds.Contains(result.OwnerTaskId) || string.IsNullOrEmpty(result.OwnerTaskId)) &&
                                                              result.Status == ResultStatus.Created,
                                                    new UpdateDefinition<Result>().Set(result => result.Status,
                                                                                       ResultStatus.Aborted)
                                                                                  .Set(result => result.CompletionDate,
                                                                                       DateTime.UtcNow),
                                                    cancellationToken)
                                 .ConfigureAwait(false);

    // Early exit if no result has been terminated
    if (count == 0)
    {
      return;
    }

    // Find all the tasks that depend on the terminated results
    var nextTasks = new HashSet<string>();
    await foreach (var dependentTasks in resultTable.GetResults(result => resultIds.Contains(result.ResultId) && result.Status == ResultStatus.Aborted &&
                                                                          taskIds.Contains(result.OwnerTaskId),
                                                                result => result.DependentTasks,
                                                                cancellationToken)
                                                    .ConfigureAwait(false))
    {
      nextTasks.UnionWith(dependentTasks);
    }

    // Recursively terminate the tasks that indirectly depend on the terminated tasks
    await TerminateTasksAndResults(taskTable,
                                   resultTable,
                                   nextTasks,
                                   Array.Empty<string>(),
                                   reason,
                                   status,
                                   shouldDeleteTasks,
                                   cancellationToken)
      .ConfigureAwait(false);
  }

  /// <summary>
  ///   Delete all results for a given session
  /// </summary>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="objectStorage">Interface to manage objects</param>
  /// <param name="sessionId">ID of the session to purge</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <remarks>
  ///   This method will delete all results that are in the Completed, Created or Aborted state and their deletion is managed
  ///   by ArmoniK.
  ///   It will also delete the associated objects in the object storage.
  /// </remarks>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task PurgeResultsAsync(IResultTable      resultTable,
                                             IObjectStorage    objectStorage,
                                             string            sessionId,
                                             CancellationToken cancellationToken)
  {
    await foreach (var ids in resultTable.GetResults(result => result.SessionId == sessionId &&
                                                               (result.Status == ResultStatus.Completed || result.Status == ResultStatus.Created ||
                                                                result.Status == ResultStatus.Aborted) && !result.ManualDeletion,
                                                     result => result.OpaqueId,
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

    await resultTable.UpdateManyResults(result => result.SessionId == sessionId && !result.ManualDeletion,
                                        new UpdateDefinition<Result>().Set(result => result.Status,
                                                                           ResultStatus.DeletedData)
                                                                      .Set(result => result.OpaqueId,
                                                                           Array.Empty<byte>()),
                                        cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <summary>
  ///   Delete the result
  /// </summary>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="objectStorage">Interface to manage objects</param>
  /// <param name="resultId">ID of the result to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  public static async Task DeleteResultAsync(IResultTable      resultTable,
                                             IObjectStorage    objectStorage,
                                             string            resultId,
                                             CancellationToken cancellationToken)
  {
    var result = await resultTable.GetResult(resultId,
                                             cancellationToken)
                                  .ConfigureAwait(false);

    if (result.ManualDeletion)
    {
      return;
    }

    //Discard value is used to remove warnings CS4014 !!
    _ = Task.Factory.StartNew(async () =>
                              {
                                await objectStorage.TryDeleteAsync(new[]
                                                                   {
                                                                     result.OpaqueId,
                                                                   },
                                                                   CancellationToken.None)
                                                   .ConfigureAwait(false);
                              },
                              cancellationToken);

    await resultTable.MarkAsDeleted(resultId,
                                    cancellationToken)
                     .ConfigureAwait(false);
  }
}
