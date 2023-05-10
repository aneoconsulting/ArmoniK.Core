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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

public static class TaskLifeCycleHelper
{
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

    await resultTable.SetTaskOwnership(sessionId,
                                       taskCreationRequests.SelectMany(r => r.ExpectedOutputKeys.Select(key => (key, r.TaskId)))
                                                           .AsICollection(),
                                       cancellationToken)
                     .ConfigureAwait(false);
  }

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

    var readyTasks = new Dictionary<(int priority, string partitionId), List<string>>();


    foreach (var request in taskRequests)
    {
      var dependencies = request.DataDependencies.ToList();

      logger.LogDebug("Process task request {request}",
                      request);

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
          if (readyTasks.TryGetValue((request.Options.Priority, request.Options.PartitionId),
                                     out var ids))
          {
            ids.Add(request.TaskId);
          }
          else
          {
            readyTasks.Add((request.Options.Priority, request.Options.PartitionId),
                           new List<string>
                           {
                             request.TaskId,
                           });
          }
        }
      }
      else
      {
        if (readyTasks.TryGetValue((request.Options.Priority, request.Options.PartitionId),
                                   out var ids))
        {
          ids.Add(request.TaskId);
        }
        else
        {
          readyTasks.Add((request.Options.Priority, request.Options.PartitionId),
                         new List<string>
                         {
                           request.TaskId,
                         });
        }
      }
    }

    if (readyTasks.Any())
    {
      foreach (var key in readyTasks.Keys)
      {
        var ids = readyTasks[key];

        await pushQueueStorage.PushMessagesAsync(ids,
                                                 key.partitionId,
                                                 key.priority,
                                                 cancellationToken)
                              .ConfigureAwait(false);
        await taskTable.FinalizeTaskCreation(ids,
                                             cancellationToken)
                       .ConfigureAwait(false);
      }
    }
  }
}
