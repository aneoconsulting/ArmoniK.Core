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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Convert the events from the different watchers and the data from the database to
///   gRPC objects
/// </summary>
public class WatchToGrpc
{
  private const    int            PageSize = 100;
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
                     IResultWatcher resultWatcher)
  {
    taskTable_     = taskTable;
    taskWatcher_   = taskWatcher;
    resultTable_   = resultTable;
    resultWatcher_ = resultWatcher;
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
  public IAsyncEnumerable<EventSubscriptionResponse> GetEvents(string                       sessionId,
                                                               ICollection<EventsEnum>      events,
                                                               Filters?                     tasksFilters,
                                                               Api.gRPC.V1.Results.Filters? resultsFilters,
                                                               CancellationToken            cancellationToken)
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

    if (!events.Any() || events.Contains(EventsEnum.NewTask))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var                                            read = 0;
                              var                                            page = 0;
                              (IEnumerable<TaskData> tasks, long totalCount) res;
                              while ((res = await taskTable_.ListTasksAsync(internalTasksFilter,
                                                                            data => data.CreationDate,
                                                                            data => data, // todo: create a class to properly use selector
                                                                            false,
                                                                            page,
                                                                            PageSize,
                                                                            cancellationToken)
                                                            .ConfigureAwait(false)).totalCount > read)
                              {
                                foreach (var cur in res.tasks)
                                {
                                  await channel.Writer.WriteAsync(new EventSubscriptionResponse
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
                                                                                  cur.ExpectedOutputIds,
                                                                                },
                                                                                OriginTaskId = cur.InitialTaskId,
                                                                                PayloadId    = cur.PayloadId,
                                                                                RetryOfIds =
                                                                                {
                                                                                  cur.RetryOfIds,
                                                                                },
                                                                                Status = cur.Status,
                                                                                TaskId = cur.TaskId,
                                                                              },
                                                                    SessionId = cur.SessionId,
                                                                  },
                                                                  CancellationToken.None)
                                               .ConfigureAwait(false);

                                  read++;
                                }

                                page++;
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewResult))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var                                           read = 0;
                              var                                           page = 0;
                              (IEnumerable<Result> results, int totalCount) res;
                              while ((res = await resultTable_.ListResultsAsync(internalResultsFilter,
                                                                                data => data.CreationDate,
                                                                                false,
                                                                                page,
                                                                                PageSize,
                                                                                cancellationToken)
                                                              .ConfigureAwait(false)).totalCount > read)
                              {
                                foreach (var cur in res.results)
                                {
                                  await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                  {
                                                                    NewResult = new EventSubscriptionResponse.Types.NewResult
                                                                                {
                                                                                  Status   = cur.Status,
                                                                                  OwnerId  = cur.OwnerTaskId,
                                                                                  ResultId = cur.ResultId,
                                                                                },
                                                                    SessionId = cur.SessionId,
                                                                  },
                                                                  CancellationToken.None)
                                               .ConfigureAwait(false);

                                  read++;
                                }

                                page++;
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewTask))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var newTasks = await taskWatcher_.GetNewTasks(internalTasksFilter,
                                                                            cancellationToken)
                                                               .ConfigureAwait(false);


                              await foreach (var cur in newTasks.WithCancellation(cancellationToken)
                                                                .ConfigureAwait(false))
                              {
                                await channel.Writer.WriteAsync(new EventSubscriptionResponse
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
                                                                              Status = cur.Status,
                                                                              TaskId = cur.TaskId,
                                                                            },
                                                                  SessionId = cur.SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .ConfigureAwait(false);
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.TaskStatusUpdate))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var newTasks = await taskWatcher_.GetTaskStatusUpdates(internalTasksFilter,
                                                                                     cancellationToken)
                                                               .ConfigureAwait(false);

                              await foreach (var cur in newTasks.WithCancellation(cancellationToken)
                                                                .ConfigureAwait(false))
                              {
                                await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                {
                                                                  TaskStatusUpdate = new EventSubscriptionResponse.Types.TaskStatusUpdate
                                                                                     {
                                                                                       Status = cur.Status,
                                                                                       TaskId = cur.TaskId,
                                                                                     },
                                                                  SessionId = cur.SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .ConfigureAwait(false);
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.NewResult))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var newResults = await resultWatcher_.GetNewResults(internalResultsFilter,
                                                                                  cancellationToken)
                                                                   .ConfigureAwait(false);

                              await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                  .ConfigureAwait(false))
                              {
                                await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                {
                                                                  NewResult = new EventSubscriptionResponse.Types.NewResult
                                                                              {
                                                                                Status   = cur.Status,
                                                                                OwnerId  = cur.OwnerId,
                                                                                ResultId = cur.ResultId,
                                                                              },
                                                                  SessionId = cur.SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .ConfigureAwait(false);
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.ResultStatusUpdate))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var newResults = await resultWatcher_.GetResultStatusUpdates(internalResultsFilter,
                                                                                           cancellationToken)
                                                                   .ConfigureAwait(false);

                              await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                  .ConfigureAwait(false))
                              {
                                await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                {
                                                                  ResultStatusUpdate = new EventSubscriptionResponse.Types.ResultStatusUpdate
                                                                                       {
                                                                                         ResultId = cur.ResultId,
                                                                                         Status   = cur.Status,
                                                                                       },
                                                                  SessionId = cur.SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .ConfigureAwait(false);
                              }
                            },
                            cancellationToken);
    }

    if (!events.Any() || events.Contains(EventsEnum.ResultOwnerUpdate))
    {
      Task.Factory.StartNew(async () =>
                            {
                              var newResults = await resultWatcher_.GetResultOwnerUpdates(internalResultsFilter,
                                                                                          cancellationToken)
                                                                   .ConfigureAwait(false);

                              await foreach (var cur in newResults.WithCancellation(cancellationToken)
                                                                  .ConfigureAwait(false))
                              {
                                await channel.Writer.WriteAsync(new EventSubscriptionResponse
                                                                {
                                                                  ResultOwnerUpdate = new EventSubscriptionResponse.Types.ResultOwnerUpdate
                                                                                      {
                                                                                        ResultId        = cur.ResultId,
                                                                                        CurrentOwnerId  = cur.NewOwner,
                                                                                        PreviousOwnerId = cur.PreviousOwnerId,
                                                                                      },
                                                                  SessionId = cur.SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .ConfigureAwait(false);
                              }
                            },
                            cancellationToken);
    }

    return channel.Reader.ReadAllAsync(cancellationToken);
  }
}
