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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

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
    var localOptions = submissionOptions is not null
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

    if (localOptions.Priority > maxPriority)
    {
      var exception = new RpcException(new Status(StatusCode.InvalidArgument,
                                                  $"Max priority is {maxPriority}"));
      logger.LogError(exception,
                      "Invalid Argument");
      throw exception;
    }

    // we are on client side
    if (sessionData.SessionId == parentTaskId && !sessionData.ClientSubmission)
    {
      throw new SubmissionClosedException("Client submission is closed");
    }

    // we are on worker side
    if (sessionData.SessionId != parentTaskId && !sessionData.WorkerSubmission)
    {
      throw new SubmissionClosedException("Worker submission is closed");
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
                                       CancellationToken                cancellationToken = default)
  {
    if (!taskCreationRequests.Any())
    {
      return;
    }

    var parentTaskIds = new List<string>();
    var createdBy     = string.Empty;

    if (!parentTaskId.Equals(sessionId))
    {
      createdBy = parentTaskId;
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
                                                                                    createdBy,
                                                                                    parentTaskIds,
                                                                                    request.DataDependencies.ToList(),
                                                                                    request.ExpectedOutputKeys.ToList(),
                                                                                    Array.Empty<string>(),
                                                                                    TaskStatus.Creating,
                                                                                    request.Options ??
                                                                                    throw new ArmoniKException("Task Options should not be null here"),
                                                                                    new Output(OutputStatus.Success,
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

    await resultTable.SetTaskOwnership(dict.Where(pair => pair.Value != "")
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
  /// <param name="sessionData">Session data of the completed results</param>
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
                                                SessionData                      sessionData,
                                                string                           parentTaskId,
                                                ILogger                          logger,
                                                CancellationToken                cancellationToken = default)
  {
    if (!taskRequests.Any())
    {
      return;
    }

    logger.LogDebug("Tasks to finalize : {@TaskRequests}",
                    taskRequests);

    var prepareTaskDependencies = PrepareTaskDependencies(taskTable,
                                                          resultTable,
                                                          taskRequests,
                                                          logger,
                                                          cancellationToken);

    // Transfer ownership while dependencies are in preparation
    if (!parentTaskId.Equals(sessionData.SessionId))
    {
      var parentExpectedOutputKeys = (await taskTable.ReadTaskAsync(parentTaskId,
                                                                    data => data.ExpectedOutputIds,
                                                                    cancellationToken)
                                                     .ConfigureAwait(false)).ToHashSet();
      var taskDataModels =
        taskRequests.Select(request => new IResultTable.ChangeResultOwnershipRequest(request.ExpectedOutputKeys.Where(id => parentExpectedOutputKeys.Contains(id)),
                                                                                     request.TaskId));
      await resultTable.ChangeResultOwnership(parentTaskId,
                                              taskDataModels,
                                              cancellationToken)
                       .ConfigureAwait(false);
    }

    var readyTask = await prepareTaskDependencies.ConfigureAwait(false);
    var taskIds = taskRequests.Select(request => request.TaskId)
                              .AsICollection();

    await taskTable.UpdateManyTasks(data => data.Status == TaskStatus.Creating && taskIds.Contains(data.TaskId),
                                    new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                         TaskStatus.Pending),
                                    cancellationToken)
                   .ConfigureAwait(false);

    await EnqueueReadyTasks(taskTable,
                            pushQueueStorage,
                            sessionData,
                            readyTask,
                            logger,
                            cancellationToken)
      .ConfigureAwait(false);
  }

  /// <summary>
  ///   Collect and record all the task dependencies specified in the <paramref name="taskRequests" />,
  ///   and return all the tasks that are ready to be enqueued.
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskRequests">Tasks requests to finalize</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Queue messages for ready tasks
  /// </returns>
  private static async Task<ICollection<MessageData>> PrepareTaskDependencies(ITaskTable                       taskTable,
                                                                              IResultTable                     resultTable,
                                                                              ICollection<TaskCreationRequest> taskRequests,
                                                                              ILogger                          logger,
                                                                              CancellationToken                cancellationToken)
  {
    using var scope = logger.BeginScope("Prepare task dependencies for {@TaskIds}",
                                        taskRequests.ViewSelect(req => req.TaskId));

    var allDependencies       = new HashSet<string>();
    var completedDependencies = new List<string>();

    // Get all the results that are a dependency of at least one task
    foreach (var request in taskRequests)
    {
      allDependencies.UnionWith(request.DataDependencies);
      allDependencies.Add(request.PayloadId);
    }

    logger.LogDebug("Check Result status for results {@ResultIds}",
                    allDependencies);

    // Get the dependencies that are already completed to avoid tracking already completed results
    await foreach (var resultId in resultTable.GetResults(result => allDependencies.Contains(result.ResultId) && result.Status == ResultStatus.Completed,
                                                          result => result.ResultId,
                                                          cancellationToken)
                                              .ConfigureAwait(false))
    {
      allDependencies.Remove(resultId);
      completedDependencies.Add(resultId);
    }

    // Build the mapping between tasks and their dependencies
    var taskDependencies = taskRequests.ToDictionary(request => request.TaskId,
                                                     request =>
                                                     {
                                                       var dependencies = request.DataDependencies.Where(resultId => allDependencies.Contains(resultId))
                                                                                 .ToList();
                                                       if (allDependencies.Contains(request.PayloadId))
                                                       {
                                                         dependencies.Add(request.PayloadId);
                                                       }

                                                       return dependencies;
                                                     });

    // Build the mapping between results and their dependents
    var resultDependencies = new Dictionary<string, ICollection<string>>();
    foreach (var (taskId, resultIds) in taskDependencies)
    {
      if (!resultIds.Any())
      {
        continue;
      }

      foreach (var resultId in resultIds)
      {
        if (resultDependencies.TryGetValue(resultId,
                                           out var resultDependency))
        {
          resultDependency.Add(taskId);
        }
        else
        {
          resultDependencies.Add(resultId,
                                 new HashSet<string>
                                 {
                                   taskId,
                                 });
        }
      }
    }

    // Add dependency to all results
    await resultTable.AddTaskDependencies(resultDependencies,
                                          cancellationToken)
                     .ConfigureAwait(false);

    // Check all the remaining dependencies
    await foreach (var completedResult in resultTable.GetResults(result => allDependencies.Contains(result.ResultId) && result.Status == ResultStatus.Completed,
                                                                 result => result.ResultId,
                                                                 cancellationToken)
                                                     .ConfigureAwait(false))
    {
      allDependencies.Remove(completedResult);
      completedDependencies.Add(completedResult);
    }

    // Remove all the dependencies that are already completed from the task.
    // If an Agent has completed one of the dependencies between the GetResults and this remove,
    // One will succeed removing the dependency, the other will silently fail.
    // In either case, the task will be submitted without error by the Agent.
    // If the agent completes the dependencies _before_ the GetResults, both will try to remove it,
    // and both will queue the task.
    // This is benign as it will be handled during dequeue with message deduplication.
    var x = await taskTable.RemoveRemainingDataDependenciesAsync(taskDependencies.Keys,
                                                                 completedDependencies,
                                                                 data => new MessageData(data.TaskId,
                                                                                         data.SessionId,
                                                                                         data.Options),
                                                                 cancellationToken)
                           .ToListAsync(cancellationToken)
                           .ConfigureAwait(false);
    return x;
  }

  /// <summary>
  ///   Remove completed results from dependent tasks and submit tasks which dependencies are completed
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="pushQueueStorage">Interface to push tasks in the queue</param>
  /// <param name="sessionData">Session data of the completed results</param>
  /// <param name="results">Collection of completed results</param>
  /// <param name="logger">Logger used to produce logs</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task ResolveDependencies(ITaskTable          taskTable,
                                               IResultTable        resultTable,
                                               IPushQueueStorage   pushQueueStorage,
                                               SessionData         sessionData,
                                               ICollection<string> results,
                                               ILogger             logger,
                                               CancellationToken   cancellationToken = default)
  {
    using var activity = logger.BeginScope("Resolving dependencies for {@ResultIds}",
                                           results);

    logger.LogDebug("Submit tasks which new data are available");

    // Get all tasks that depend on the results that were completed by the given results (removing duplicates)
    var dependentTasks = await resultTable.GetResults(sessionData.SessionId,
                                                      results,
                                                      cancellationToken)
                                          .SelectMany(result => result.DependentTasks.ToAsyncEnumerable())
                                          .ToHashSetAsync(cancellationToken)
                                          .ConfigureAwait(false);

    if (!dependentTasks.Any())
    {
      return;
    }

    logger.LogDebug("Dependent Tasks Dictionary {@dependents}",
                    dependentTasks);

    // Remove all results that were completed by the current task from their dependents.
    // This will try to remove more results than strictly necessary.
    // This is completely safe and should be optimized by the DB.
    // Multiple agents can see the same task as ready and will try to start it multiple times.
    // This is benign as it will be handled during dequeue with message deduplication.
    var readyTasks = await taskTable.RemoveRemainingDataDependenciesAsync(dependentTasks,
                                                                          results,
                                                                          data => new MessageData(data.TaskId,
                                                                                                  data.SessionId,
                                                                                                  data.Options),
                                                                          cancellationToken)
                                    .ToListAsync(cancellationToken)
                                    .ConfigureAwait(false);

    logger.LogDebug("Check tasks status for tasks {@TaskIds}, found {@ReadyTasks}",
                    dependentTasks,
                    readyTasks);

    await EnqueueReadyTasks(taskTable,
                            pushQueueStorage,
                            sessionData,
                            readyTasks,
                            logger,
                            cancellationToken)
      .ConfigureAwait(false);
  }

  /// <summary>
  ///   Enqueue all the messages that are ready for enqueueing, and mark them as enqueued in the task table
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="pushQueueStorage">Interface to push tasks in the queue</param>
  /// <param name="sessionData">Data of the session in which the tasks are enqueued</param>
  /// <param name="messages">Messages to enqueue</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  private static async Task EnqueueReadyTasks(ITaskTable               taskTable,
                                              IPushQueueStorage        pushQueueStorage,
                                              SessionData              sessionData,
                                              ICollection<MessageData> messages,
                                              ILogger                  logger,
                                              CancellationToken        cancellationToken)
  {
    if (!messages.Any())
    {
      return;
    }

    if (sessionData.Status != SessionStatus.Paused)
    {
      foreach (var group in messages.GroupBy(msg => (msg.Options.PartitionId, msg.Options.Priority)))
      {
        await pushQueueStorage.PushMessagesAsync(group,
                                                 group.Key.PartitionId,
                                                 cancellationToken)
                              .ConfigureAwait(false);
      }

      logger.LogDebug("Pushed messages : {@Messages}",
                      messages);
    }

    await taskTable.FinalizeTaskCreation(messages.Select(task => task.TaskId)
                                                 .AsICollection(),
                                         sessionData.Status == SessionStatus.Paused,
                                         cancellationToken)
                   .ConfigureAwait(false);
  }

  /// <summary>
  ///   Resume session and its paused tasks
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="sessionTable">Interface to manage session states</param>
  /// <param name="pushQueueStorage">Interface to push tasks in the queue</param>
  /// <param name="sessionId">Id of the session to resume</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The updated data of the session
  /// </returns>
  public static async Task<SessionData> ResumeAsync(ITaskTable        taskTable,
                                                    ISessionTable     sessionTable,
                                                    IPushQueueStorage pushQueueStorage,
                                                    string            sessionId,
                                                    CancellationToken cancellationToken = default)
  {
    var session = await sessionTable.ResumeSessionAsync(sessionId,
                                                        cancellationToken)
                                    .ConfigureAwait(false);

    await foreach (var grouping in taskTable.FindTasksAsync(data => data.SessionId == sessionId && data.Status == TaskStatus.Paused,
                                                            data => new MessageData(data.TaskId,
                                                                                    data.SessionId,
                                                                                    data.Options),
                                                            cancellationToken)
                                            .GroupBy(msg => (msg.Options.PartitionId, msg.Options.Priority))
                                            .WithCancellation(cancellationToken)
                                            .ConfigureAwait(false))
    {
      await foreach (var tasks in grouping.ToChunksAsync(100000,
                                                         TimeSpan.FromSeconds(1),
                                                         cancellationToken)
                                          .ConfigureAwait(false))
      {
        cancellationToken.ThrowIfCancellationRequested();

        var taskIds = tasks.Select(task => task.TaskId)
                           .AsICollection();

        await taskTable.UpdateManyTasks(data => data.SessionId == sessionId && data.Status == TaskStatus.Paused && taskIds.Contains(data.TaskId),
                                        new UpdateDefinition<TaskData>().Set(data => data.OwnerPodId,
                                                                             "")
                                                                        .Set(data => data.OwnerPodName,
                                                                             "")
                                                                        .Set(data => data.ReceptionDate,
                                                                             null)
                                                                        .Set(data => data.AcquisitionDate,
                                                                             null)
                                                                        .Set(data => data.Status,
                                                                             TaskStatus.Submitted),
                                        CancellationToken.None)
                       .ConfigureAwait(false);

        await pushQueueStorage.PushMessagesAsync(tasks,
                                                 grouping.Key.PartitionId,
                                                 CancellationToken.None)
                              .ConfigureAwait(false);
      }
    }

    return session;
  }


  /// <summary>
  ///   Pause session and its paused tasks
  /// </summary>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="sessionTable">Interface to manage session states</param>
  /// <param name="sessionId">Id of the session to pause</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The updated data of the session
  /// </returns>
  public static async Task<SessionData> PauseAsync(ITaskTable        taskTable,
                                                   ISessionTable     sessionTable,
                                                   string            sessionId,
                                                   CancellationToken cancellationToken = default)

  {
    var sessionData = await sessionTable.PauseSessionAsync(sessionId,
                                                           cancellationToken)
                                        .ConfigureAwait(false);

    await taskTable.UpdateManyTasks(data => (data.Status == TaskStatus.Submitted || data.Status == TaskStatus.Dispatched) && data.SessionId == sessionId,
                                    new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                         TaskStatus.Paused),
                                    cancellationToken)
                   .ConfigureAwait(false);
    return sessionData;
  }
}
