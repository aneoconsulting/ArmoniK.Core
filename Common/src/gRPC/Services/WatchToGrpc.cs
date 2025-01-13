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

using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Convert the events from the different watchers and the data from the database to
///   gRPC objects
/// </summary>
public class WatchToGrpc
{
  private readonly ILogger        logger_;
  private readonly IResultTable   resultTable_;
  private readonly IResultWatcher resultWatcher_;
  private readonly ITaskTable     taskTable_;
  private readonly ITaskWatcher   taskWatcher_;

  /// <summary>
  ///   Initializes the class from the given parameters
  /// </summary>
  /// <param name="taskTable">Interface to access task data</param>
  /// <param name="taskWatcher">Watcher to receive events when tasks are modified</param>
  /// <param name="resultTable">Interface to access result data</param>
  /// <param name="resultWatcher">Watcher to receive events when results are modified</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public WatchToGrpc(ITaskTable     taskTable,
                     ITaskWatcher   taskWatcher,
                     IResultTable   resultTable,
                     IResultWatcher resultWatcher,
                     ILogger        logger)
  {
    taskTable_     = taskTable;
    taskWatcher_   = taskWatcher;
    resultTable_   = resultTable;
    resultWatcher_ = resultWatcher;
    logger_        = logger;
  }

  /// <summary>
  ///   Get the task and result update events from the given session
  /// </summary>
  /// <param name="sessionId">The id of the session</param>
  /// <param name="resultsFilters">Filter for results related events</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <param name="events">Events that should be returned</param>
  /// <param name="tasksFilters">Filter for results related events</param>
  /// <returns>
  ///   An <see cref="IAsyncEnumerable{EventSubscriptionResponse}" /> that contains the update events
  /// </returns>
  public async IAsyncEnumerable<EventSubscriptionResponse> GetEvents(string                                     sessionId,
                                                                     ICollection<EventsEnum>                    events,
                                                                     Filters?                                   tasksFilters,
                                                                     Api.gRPC.V1.Results.Filters?               resultsFilters,
                                                                     [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var channel = Channel.CreateUnbounded<EventSubscriptionResponse>();
    var internalTasksFilter = tasksFilters is null
                                ? data => data.SessionId == sessionId
                                : tasksFilters.ToTaskDataFilter()
                                              .ExpressionAnd(data => data.SessionId == sessionId);
    var internalResultsFilter = resultsFilters is null
                                  ? data => data.SessionId == sessionId
                                  : resultsFilters.ToResultFilter()
                                                  .ExpressionAnd(data => data.SessionId == sessionId);

    logger_.LogDebug("Filter events {events}",
                     events);
    logger_.LogDebug("Task {taskFilter}",
                     internalTasksFilter);
    logger_.LogDebug("Result {resultFilter}",
                     internalResultsFilter);

    using var scope = logger_.BeginPropertyScope(("sessionId", sessionId));

    if (!events.Any() || events.Contains(EventsEnum.NewTask))
    {
      var newTasks = (await taskWatcher_.GetNewTasks(internalTasksFilter,
                                                     cancellationToken)
                                        .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                              {
                                                                                NewTask = new EventSubscriptionResponse.Types.NewTask
                                                                                          {
                                                                                            ParentTaskIds =
                                                                                            {
                                                                                              cur.ParentTaskIds,
                                                                                            },
                                                                                            DataDependencies =
                                                                                            {
                                                                                              cur.DataDependencies,
                                                                                            },
                                                                                            ExpectedOutputKeys =
                                                                                            {
                                                                                              cur.ExpectedOutputKeys,
                                                                                            },
                                                                                            OriginTaskId = cur.OriginTaskId,
                                                                                            PayloadId    = cur.PayloadId,
                                                                                            RetryOfIds =
                                                                                            {
                                                                                              cur.RetryOfIds,
                                                                                            },
                                                                                            Status = cur.Status.ToGrpcStatus(),
                                                                                            TaskId = cur.TaskId,
                                                                                          },
                                                                                SessionId = cur.SessionId,
                                                                              });

      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var cur in newTasks.WithCancellation(cancellationToken)
                                                                      .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("New task {task}",
                                                       cur);

                                      await channel.Writer.WriteAsync(cur,
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.TaskStatusUpdate))
    {
      var newTasks = (await taskWatcher_.GetTaskStatusUpdates(internalTasksFilter,
                                                              cancellationToken)
                                        .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                              {
                                                                                TaskStatusUpdate = new EventSubscriptionResponse.Types.TaskStatusUpdate
                                                                                                   {
                                                                                                     Status = cur.Status.ToGrpcStatus(),
                                                                                                     TaskId = cur.TaskId,
                                                                                                   },
                                                                                SessionId = cur.SessionId,
                                                                              });

      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var cur in newTasks.WithCancellation(cancellationToken)
                                                                      .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("Task status update {update}",
                                                       cur);
                                      await channel.Writer.WriteAsync(cur,
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewResult))
    {
      var newResults = (await resultWatcher_.GetNewResults(internalResultsFilter,
                                                           cancellationToken)
                                            .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                                  {
                                                                                    NewResult = new EventSubscriptionResponse.Types.NewResult
                                                                                                {
                                                                                                  Status   = cur.Status.ToGrpcStatus(),
                                                                                                  OwnerId  = cur.OwnerId,
                                                                                                  ResultId = cur.ResultId,
                                                                                                },
                                                                                    SessionId = cur.SessionId,
                                                                                  });

      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                        .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("New result {result}",
                                                       cur);
                                      await channel.Writer.WriteAsync(cur,
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.ResultStatusUpdate))
    {
      var newResults = (await resultWatcher_.GetResultStatusUpdates(internalResultsFilter,
                                                                    cancellationToken)
                                            .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                                  {
                                                                                    ResultStatusUpdate = new EventSubscriptionResponse.Types.ResultStatusUpdate
                                                                                                         {
                                                                                                           ResultId = cur.ResultId,
                                                                                                           Status   = cur.Status.ToGrpcStatus(),
                                                                                                         },
                                                                                    SessionId = cur.SessionId,
                                                                                  });

      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                        .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("Result status update {update}",
                                                       cur);
                                      await channel.Writer.WriteAsync(cur,
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.ResultOwnerUpdate))
    {
      var newResults = (await resultWatcher_.GetResultOwnerUpdates(internalResultsFilter,
                                                                   cancellationToken)
                                            .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                                  {
                                                                                    ResultOwnerUpdate = new EventSubscriptionResponse.Types.ResultOwnerUpdate
                                                                                                        {
                                                                                                          ResultId        = cur.ResultId,
                                                                                                          CurrentOwnerId  = cur.NewOwner,
                                                                                                          PreviousOwnerId = cur.PreviousOwnerId,
                                                                                                        },
                                                                                    SessionId = cur.SessionId,
                                                                                  });

      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                        .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("Result owner update {update}",
                                                       cur);
                                      await channel.Writer.WriteAsync(cur,
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewTask))
    {
      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var esr in taskTable_.FindTasksAsync(internalTasksFilter,
                                                                                        cur => new
                                                                                               {
                                                                                                 cur.ParentTaskIds,
                                                                                                 cur.DataDependencies,
                                                                                                 cur.ExpectedOutputIds,
                                                                                                 cur.InitialTaskId,
                                                                                                 cur.PayloadId,
                                                                                                 cur.RetryOfIds,
                                                                                                 cur.Status,
                                                                                                 cur.TaskId,
                                                                                                 cur.SessionId,
                                                                                               },
                                                                                        cancellationToken)
                                                                        .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("New task from db {task}",
                                                       esr);
                                      await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                      {
                                                                        NewTask = new EventSubscriptionResponse.Types.NewTask
                                                                                  {
                                                                                    PayloadId = esr.PayloadId,
                                                                                    Status    = esr.Status.ToGrpcStatus(),
                                                                                    DataDependencies =
                                                                                    {
                                                                                      esr.DataDependencies,
                                                                                    },
                                                                                    ExpectedOutputKeys =
                                                                                    {
                                                                                      esr.ExpectedOutputIds,
                                                                                    },
                                                                                    OriginTaskId = esr.InitialTaskId,
                                                                                    ParentTaskIds =
                                                                                    {
                                                                                      esr.ParentTaskIds,
                                                                                    },
                                                                                    RetryOfIds =
                                                                                    {
                                                                                      esr.RetryOfIds,
                                                                                    },
                                                                                    TaskId = esr.TaskId,
                                                                                  },
                                                                        SessionId = esr.SessionId,
                                                                      },
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewResult))
    {
      await Task.Factory.StartNew(async () =>
                                  {
                                    await foreach (var esr in resultTable_.GetResults(internalResultsFilter,
                                                                                      cur => new
                                                                                             {
                                                                                               cur.Status,
                                                                                               cur.OwnerTaskId,
                                                                                               cur.ResultId,
                                                                                               cur.SessionId,
                                                                                             },
                                                                                      cancellationToken)
                                                                          .ConfigureAwait(false))
                                    {
                                      logger_.LogDebug("New result from db {result}",
                                                       esr);
                                      await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                      {
                                                                        NewResult = new EventSubscriptionResponse.Types.NewResult
                                                                                    {
                                                                                      OwnerId  = esr.OwnerTaskId,
                                                                                      ResultId = esr.ResultId,
                                                                                      Status   = esr.Status.ToGrpcStatus(),
                                                                                    },
                                                                        SessionId = esr.SessionId,
                                                                      },
                                                                      CancellationToken.None)
                                                   .ConfigureAwait(false);
                                    }
                                  },
                                  cancellationToken)
                .ConfigureAwait(false);
    }

    await foreach (var esr in channel.Reader.ReadAllAsync(cancellationToken)
                                     .ConfigureAwait(false))
    {
      yield return esr;
    }
  }
}
