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

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Implementation of <see cref="ITaskWatcher" /> for MongoDB
/// </summary>
public class TaskWatcher : ITaskWatcher
{
  private readonly ActivitySource                                          activitySource_;
  private readonly ILogger<TaskWatcher>                                    logger_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider_;
  private          bool                                                    isInitialized_;

  /// <summary>
  ///   Initializes <see cref="TaskWatcher" /> from the given parameters
  /// </summary>
  /// <param name="sessionProvider">MongoDB session provider</param>
  /// <param name="taskCollectionProvider">Task collection provider</param>
  /// <param name="activitySource">Activity source</param>
  /// <param name="logger">Logger used to produce logs</param>
  public TaskWatcher(SessionProvider                                         sessionProvider,
                     MongoCollectionProvider<TaskData, TaskDataModelMapping> taskCollectionProvider,
                     ActivitySource                                          activitySource,
                     ILogger<TaskWatcher>                                    logger)
  {
    sessionProvider_        = sessionProvider;
    taskCollectionProvider_ = taskCollectionProvider;
    activitySource_         = activitySource;
    logger_                 = logger;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      taskCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<NewTask>> GetNewTasks(string            sessionId,
                                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetNewTasks)}");
    activity?.SetTag($"{nameof(GetNewTasks)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<TaskData>>().Match(input => input.OperationType          == ChangeStreamOperationType.Insert &&
                                                                                                input.FullDocument.SessionId == sessionId);

    var changeStreamCursor = await taskCollectionProvider_.Get()
                                                          .WatchAsync(sessionHandle,
                                                                      pipeline,
                                                                      cancellationToken: cancellationToken,
                                                                      options: new ChangeStreamOptions
                                                                               {
                                                                                 FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                                                                               })
                                                          .ConfigureAwait(false);

    return new WatchEnumerable<NewTask, ChangeStreamDocument<TaskData>>(changeStreamCursor,
                                                                        doc => new NewTask(doc.FullDocument.SessionId,
                                                                                           doc.FullDocument.TaskId,
                                                                                           doc.FullDocument.InitialTaskId,
                                                                                           doc.FullDocument.PayloadId,
                                                                                           doc.FullDocument.ExpectedOutputIds,
                                                                                           doc.FullDocument.DataDependencies,
                                                                                           doc.FullDocument.RetryOfIds,
                                                                                           doc.FullDocument.Status));
  }


  /// <inheritdoc />
  public async Task<IAsyncEnumerable<TaskStatusUpdate>> GetTaskStatusUpdates(string            sessionId,
                                                                             CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetTaskStatusUpdates)}");
    activity?.SetTag($"{nameof(GetTaskStatusUpdates)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();

    var changeStreamCursor = await ChangeStreamUpdate.GetUpdates(taskCollectionProvider_.Get(),
                                                                 sessionHandle,
                                                                 document => document.FullDocument.SessionId == sessionId,
                                                                 new[]
                                                                 {
                                                                   nameof(TaskData.Status),
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);

    return new WatchEnumerable<TaskStatusUpdate, ChangeStreamDocument<TaskData>>(changeStreamCursor,
                                                                                 doc => new TaskStatusUpdate(doc.FullDocument.SessionId,
                                                                                                             doc.FullDocument.TaskId,
                                                                                                             doc.FullDocument.Status));
  }
}
