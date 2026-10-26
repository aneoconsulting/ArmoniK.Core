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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Exceptions;
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

    // Cancelled when the client leaves or when a source fails, so that every other source
    // releases its subscription right away.
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    try
    {
      if (!events.Any() || events.Contains(EventsEnum.NewTask))
      {
        var newTasks = (await taskWatcher_.GetNewTasks(internalTasksFilter,
                                                       cts.Token)
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

        Forward(newTasks,
                channel.Writer,
                true,
                "New task",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.TaskStatusUpdate))
      {
        var newTasks = (await taskWatcher_.GetTaskStatusUpdates(internalTasksFilter,
                                                                cts.Token)
                                          .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                                {
                                                                                  TaskStatusUpdate = new EventSubscriptionResponse.Types.TaskStatusUpdate
                                                                                                     {
                                                                                                       Status = cur.Status.ToGrpcStatus(),
                                                                                                       TaskId = cur.TaskId,
                                                                                                     },
                                                                                  SessionId = cur.SessionId,
                                                                                });

        Forward(newTasks,
                channel.Writer,
                true,
                "Task status update",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.NewResult))
      {
        var newResults = (await resultWatcher_.GetNewResults(internalResultsFilter,
                                                             cts.Token)
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

        Forward(newResults,
                channel.Writer,
                true,
                "New result",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.ResultStatusUpdate))
      {
        var newResults = (await resultWatcher_.GetResultStatusUpdates(internalResultsFilter,
                                                                      cts.Token)
                                              .ConfigureAwait(false)).Select(cur => new EventSubscriptionResponse
                                                                                    {
                                                                                      ResultStatusUpdate = new EventSubscriptionResponse.Types.ResultStatusUpdate
                                                                                                           {
                                                                                                             ResultId = cur.ResultId,
                                                                                                             Status   = cur.Status.ToGrpcStatus(),
                                                                                                           },
                                                                                      SessionId = cur.SessionId,
                                                                                    });

        Forward(newResults,
                channel.Writer,
                true,
                "Result status update",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.ResultOwnerUpdate))
      {
        var newResults = (await resultWatcher_.GetResultOwnerUpdates(internalResultsFilter,
                                                                     cts.Token)
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

        Forward(newResults,
                channel.Writer,
                true,
                "Result owner update",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.NewTask))
      {
        var tasks = taskTable_.FindTasksAsync(internalTasksFilter,
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
                                              cts.Token)
                              .Select(esr => new EventSubscriptionResponse
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
                                             });

        Forward(tasks,
                channel.Writer,
                false,
                "New task from db",
                cts);
      }

      if (!events.Any() || events.Contains(EventsEnum.NewResult))
      {
        var results = resultTable_.GetResults(internalResultsFilter,
                                              cur => new
                                                     {
                                                       cur.Status,
                                                       cur.OwnerTaskId,
                                                       cur.ResultId,
                                                       cur.SessionId,
                                                     },
                                              cts.Token)
                                  .Select(esr => new EventSubscriptionResponse
                                                 {
                                                   NewResult = new EventSubscriptionResponse.Types.NewResult
                                                               {
                                                                 OwnerId  = esr.OwnerTaskId,
                                                                 ResultId = esr.ResultId,
                                                                 Status   = esr.Status.ToGrpcStatus(),
                                                               },
                                                   SessionId = esr.SessionId,
                                                 });

        Forward(results,
                channel.Writer,
                false,
                "New result from db",
                cts);
      }

      await foreach (var esr in channel.Reader.ReadAllAsync(cancellationToken)
                                       .ConfigureAwait(false))
      {
        yield return esr;
      }
    }
    finally
    {
      cts.Cancel();
    }
  }

  /// <summary>
  ///   Copy the events of a source into the channel read by the client, in the background.
  ///   A watcher never ends on its own: when it stops, or when any source fails, the channel is completed with an
  ///   <see cref="EventStreamInterruptedException" /> so that the client receives an error and can subscribe again,
  ///   instead of waiting on a silent stream.
  /// </summary>
  /// <param name="source">Events to copy</param>
  /// <param name="writer">Channel read by the client</param>
  /// <param name="isWatcher">Whether the source is a watcher (endless) or a database snapshot (finite)</param>
  /// <param name="name">Name of the source used in logs</param>
  /// <param name="cts">Cancelled when the subscription ends; cancelled by this method when the source fails</param>
  private void Forward(IAsyncEnumerable<EventSubscriptionResponse> source,
                       ChannelWriter<EventSubscriptionResponse>    writer,
                       bool                                        isWatcher,
                       string                                      name,
                       CancellationTokenSource                     cts)
  {
    // Read now: the subscription may end and dispose cts before the task starts
    var cancellationToken = cts.Token;

    // The task never throws, so it does not need to be observed
    _ = Task.Run(async () =>
                 {
                   Exception? error = null;
                   try
                   {
                     await foreach (var esr in source.WithCancellation(cancellationToken)
                                                     .ConfigureAwait(false))
                     {
                       logger_.LogDebug("{source} {event}",
                                        name,
                                        esr);
                       await writer.WriteAsync(esr,
                                               CancellationToken.None)
                                   .ConfigureAwait(false);
                     }
                   }
                   catch (Exception e)
                   {
                     error = e;
                   }

                   if (cancellationToken.IsCancellationRequested || (error is null && !isWatcher))
                   {
                     return;
                   }

                   logger_.LogWarning(error,
                                      "Event source {source} stopped unexpectedly, closing the subscription",
                                      name);
                   writer.TryComplete(new EventStreamInterruptedException($"Event source {name} stopped unexpectedly",
                                                                          error));
                   try
                   {
                     cts.Cancel();
                   }
                   catch (ObjectDisposedException)
                   {
                     // The subscription already ended
                   }
                 },
                 CancellationToken.None);
  }
}
