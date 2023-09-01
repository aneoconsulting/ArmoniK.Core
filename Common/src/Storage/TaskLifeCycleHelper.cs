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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

public static class TaskLifeCycleHelper
{
  /// <summary>
  ///   Validate and merge task data from the session with the given options
  /// </summary>
  /// <param name="sessionData">Session Metadata</param>
  /// <param name="submissionOptions">Incoming task options</param>
  /// <param name="parentTaskId">Id of the tasks that creates the tasks</param>
  /// <param name="maxPriority">Max priority managed by the queue</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Merged task options
  /// </returns>
  /// <exception cref="InvalidOperationException">when partition in incoming tasks options is not allowed in the session</exception>
  public static TaskOptions ValidateSession(SessionData       sessionData,
                                            TaskOptions?      submissionOptions,
                                            string            parentTaskId,
                                            int               maxPriority,
                                            ILogger           logger,
                                            CancellationToken cancellationToken)
  {
    var localOptions = submissionOptions != null
                         ? TaskOptions.Merge(submissionOptions,
                                             sessionData.Options)
                         : sessionData.Options;

    using var logFunction = logger.LogFunction(parentTaskId);
    using var sessionScope = logger.BeginPropertyScope(("Session", sessionData.SessionId),
                                                       ("TaskId", parentTaskId),
                                                       ("PartitionId", localOptions.PartitionId));

    if (logger.IsEnabled(LogLevel.Trace))
    {
      cancellationToken.Register(() => logger.LogTrace("CancellationToken from ServerCallContext has been triggered"));
    }

    var availablePartitionIds = sessionData.PartitionIds ?? Array.Empty<string>();
    if (!availablePartitionIds.Contains(localOptions.PartitionId))
    {
      throw new InvalidOperationException($"The session {sessionData.SessionId} is assigned to the partitions " +
                                          $"[{string.Join(", ", availablePartitionIds)}], but TaskRequest is assigned to partition {localOptions.PartitionId}");
    }

    if (localOptions.Priority >= maxPriority)
    {
      var exception = new RpcException(new Status(StatusCode.InvalidArgument,
                                                  $"Max priority is {maxPriority}"));
      logger.LogError(exception,
                      "Invalid Argument");
      throw exception;
    }

    return localOptions;
  }

  /// <summary>
  ///   Initiate task creation
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="sessionId">Session Id of the completed results</param>
  /// <param name="parentTaskId">Id of the tasks that creates the tasks</param>
  /// <param name="taskCreationRequests">Tasks to create</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task CreateTasks(ITaskTable                       taskTable,
                                       IResultTable                     resultTable,
                                       string                           sessionId,
                                       string                           parentTaskId,
                                       ICollection<TaskCreationRequest> taskCreationRequests,
                                       ILogger                          logger,
                                       CancellationToken                cancellationToken)
  {
    var parentTaskIds = new List<string>();

    if (!parentTaskId.Equals(sessionId))
    {
      var res = await taskTable.GetParentTaskIds(parentTaskId,
                                                 cancellationToken)
                               .ConfigureAwait(false);
      parentTaskIds.AddRange(res);
    }

    parentTaskIds.Add(parentTaskId);

    await taskTable.CreateTasks(taskCreationRequests.Select(request => new TaskData(sessionId,
                                                                                    request.TaskId,
                                                                                    "",
                                                                                    "",
                                                                                    request.PayloadId,
                                                                                    parentTaskIds,
                                                                                    request.DataDependencies.ToList(),
                                                                                    request.ExpectedOutputKeys.ToList(),
                                                                                    Array.Empty<string>(),
                                                                                    TaskStatus.Creating,
                                                                                    request.Options ??
                                                                                    throw new ArmoniKException("Task Options should not be null here"),
                                                                                    new Output(false,
                                                                                               ""))),
                                cancellationToken)
                   .ConfigureAwait(false);

    var results = taskCreationRequests.SelectMany(r => r.ExpectedOutputKeys);

    // create a dictionary to efficiently access only the results without owners
    // results with owners will be modified through ownership update in FinalizeTaskCreation
    var dict = await resultTable.GetResults(sessionId,
                                            results,
                                            cancellationToken)
                                .Where(result => result.OwnerTaskId == "")
                                .ToDictionaryAsync(result => result.ResultId,
                                                   _ => "",
                                                   cancellationToken)
                                .ConfigureAwait(false);

    foreach (var creationRequest in taskCreationRequests)
    {
      foreach (var expectedOutputKey in creationRequest.ExpectedOutputKeys)
      {
        if (dict.ContainsKey(expectedOutputKey))
        {
          dict[expectedOutputKey] = creationRequest.TaskId;
        }
      }
    }

    if (logger.IsEnabled(LogLevel.Debug))
    {
      foreach (var (resultId, taskId) in dict)
      {
        logger.LogDebug("Set {taskIdOwner} as owner for {resultId}",
                        taskId,
                        resultId);
      }
    }

    await resultTable.SetTaskOwnership(sessionId,
                                       dict.Where(pair => pair.Value != "")
                                           .Select(pair => (pair.Key, pair.Value))
                                           .AsICollection(),
                                       cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <summary>
  ///   Finalize task creation
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="pushQueueStorage">Interface to push tasks in the queue</param>
  /// <param name="taskRequests">Tasks requests to finalize</param>
  /// <param name="sessionId">Session Id of the completed results</param>
  /// <param name="parentTaskId">Id of the tasks that creates the tasks</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task FinalizeTaskCreation(ITaskTable                       taskTable,
                                                IResultTable                     resultTable,
                                                IPushQueueStorage                pushQueueStorage,
                                                ICollection<TaskCreationRequest> taskRequests,
                                                string                           sessionId,
                                                string                           parentTaskId,
                                                ILogger                          logger,
                                                CancellationToken                cancellationToken)
  {
    if (!taskRequests.Any())
    {
      return;
    }

    var parentExpectedOutputKeys = new List<string>();

    if (!parentTaskId.Equals(sessionId))
    {
      parentExpectedOutputKeys.AddRange(await taskTable.GetTaskExpectedOutputKeys(parentTaskId,
                                                                                  cancellationToken)
                                                       .ConfigureAwait(false));
    }

    var taskDataModels = taskRequests.Select(request => new IResultTable.ChangeResultOwnershipRequest(parentExpectedOutputKeys.Intersect(request.ExpectedOutputKeys),
                                                                                                      request.TaskId));
    await resultTable.ChangeResultOwnership(sessionId,
                                            parentTaskId,
                                            taskDataModels,
                                            cancellationToken)
                     .ConfigureAwait(false);

    var readyTasks = new Dictionary<string, List<MessageData>>();


    foreach (var request in taskRequests)
    {
      var dependencies = request.DataDependencies.ToList();
      dependencies.Add(request.PayloadId);

      logger.LogDebug("Process task request {request} with dependencies {dependencies}",
                      request,
                      dependencies);

      if (dependencies.Any())
      {
        // If there is dependencies, we need to register the current task as a dependent of its dependencies.
        // This should be done *before* verifying if the dependencies are satisfied in order to avoid missing
        // any result completion.
        // If a result is completed at the same time, either the submitter will see the result has been completed,
        // Or the Agent will remove the result from the remaining dependencies of the task.
        await resultTable.AddTaskDependency(sessionId,
                                            dependencies,
                                            new List<string>
                                            {
                                              request.TaskId,
                                            },
                                            cancellationToken)
                         .ConfigureAwait(false);

        // Get the dependencies that are already completed in order to remove them from the remaining dependencies.
        var completedDependencies = await resultTable.GetResults(sessionId,
                                                                 dependencies,
                                                                 cancellationToken)
                                                     .Where(result => result.Status == ResultStatus.Completed)
                                                     .Select(result => result.ResultId)
                                                     .ToListAsync(cancellationToken)
                                                     .ConfigureAwait(false);

        // Remove all the dependencies that are already completed from the task.
        // If an Agent has completed one of the dependencies between the GetResults and this remove,
        // One will succeed removing the dependency, the other will silently fail.
        // In either case, the task will be submitted without error by the Agent.
        // If the agent completes the dependencies _before_ the GetResults, both will try to remove it,
        // and both will queue the task.
        // This is benign as it will be handled during dequeue with message deduplication.
        await taskTable.RemoveRemainingDataDependenciesAsync(new[]
                                                             {
                                                               request.TaskId,
                                                             },
                                                             completedDependencies,
                                                             cancellationToken)
                       .ConfigureAwait(false);

        // If all dependencies were already completed, the task is ready to be started.
        if (dependencies.Count == completedDependencies.Count)
        {
          if (readyTasks.TryGetValue(request.Options.PartitionId,
                                     out var msgsData))
          {
            msgsData.Add(new MessageData(request.TaskId,
                                         sessionId,
                                         request.Options));
          }
          else
          {
            readyTasks.Add(request.Options.PartitionId,
                           new List<MessageData>
                           {
                             new(request.TaskId,
                                 sessionId,
                                 request.Options),
                           });
          }
        }
      }
      else
      {
        if (readyTasks.TryGetValue(request.Options.PartitionId,
                                   out var msgsData))
        {
          msgsData.Add(new MessageData(request.TaskId,
                                       sessionId,
                                       request.Options));
        }
        else
        {
          readyTasks.Add(request.Options.PartitionId,
                         new List<MessageData>
                         {
                           new(request.TaskId,
                               sessionId,
                               request.Options),
                         });
        }
      }
    }

    if (readyTasks.Any())
    {
      foreach (var item in readyTasks)
      {
        await pushQueueStorage.PushMessagesAsync(item.Value,
                                                 item.Key,
                                                 cancellationToken)
                              .ConfigureAwait(false);
        await taskTable.FinalizeTaskCreation(item.Value.Select(data => data.TaskId)
                                                 .ToList(),
                                             cancellationToken)
                       .ConfigureAwait(false);
      }
    }
  }

  /// <summary>
  ///   Remove completed results from dependent tasks and submit tasks which dependencies are completed
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="pushQueueStorage">Interface to push tasks in the queue</param>
  /// <param name="sessionId">Session Id of the completed results</param>
  /// <param name="results">Collection of completed results</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task ResolveDependencies(ITaskTable          taskTable,
                                               IResultTable        resultTable,
                                               IPushQueueStorage   pushQueueStorage,
                                               string              sessionId,
                                               ICollection<string> results,
                                               ILogger             logger,
                                               CancellationToken   cancellationToken)
  {
    logger.LogDebug("Submit tasks which new data are available");

    // Get all tasks that depend on the results that were completed by the current task (removing duplicates)
    var dependentTasks = await resultTable.GetResults(sessionId,
                                                      results,
                                                      cancellationToken)
                                          .SelectMany(result => result.DependentTasks.ToAsyncEnumerable())
                                          .ToHashSetAsync(cancellationToken)
                                          .ConfigureAwait(false);

    if (!dependentTasks.Any())
    {
      return;
    }

    if (logger.IsEnabled(LogLevel.Debug))
    {
      logger.LogDebug("Dependent Tasks Dictionary {@dependents}",
                      dependentTasks);
    }

    // Remove all results that were completed by the current task from their dependents.
    // This will try to remove more results than strictly necessary.
    // This is completely safe and should be optimized by the DB.
    await taskTable.RemoveRemainingDataDependenciesAsync(dependentTasks,
                                                         results,
                                                         cancellationToken)
                   .ConfigureAwait(false);

    // Find all tasks whose dependencies are now complete in order to start them.
    // Multiple agents can see the same task as ready and will try to start it multiple times.
    // This is benign as it will be handled during dequeue with message deduplication.
    var groups = (await taskTable.FindTasksAsync(data => dependentTasks.Contains(data.TaskId) && data.Status == TaskStatus.Creating &&
                                                         data.RemainingDataDependencies                      == new Dictionary<string, bool>(),
                                                 data => new
                                                         {
                                                           data.TaskId,
                                                           data.SessionId,
                                                           data.Options,
                                                           data.Options.PartitionId,
                                                           data.Options.Priority,
                                                         },
                                                 cancellationToken)
                                 .ConfigureAwait(false)).GroupBy(data => (data.PartitionId, data.Priority));

    foreach (var group in groups)
    {
      var ids = group.Select(data => data.TaskId)
                     .ToList();

      var msgsData = group.Select(data => new MessageData(data.TaskId,
                                                          data.SessionId,
                                                          data.Options));
      await pushQueueStorage.PushMessagesAsync(msgsData,
                                               group.Key.PartitionId,
                                               cancellationToken)
                            .ConfigureAwait(false);

      await taskTable.FinalizeTaskCreation(ids,
                                           cancellationToken)
                     .ConfigureAwait(false);
    }
  }
}
