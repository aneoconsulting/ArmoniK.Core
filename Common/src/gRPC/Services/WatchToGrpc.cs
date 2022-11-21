// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Graphs;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Graphs;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

public class WatchToGrpc
{
  private readonly ITaskTable     taskTable_;
  private readonly ITaskWatcher   taskWatcher_;
  private readonly IResultTable   resultTable_;
  private readonly IResultWatcher resultWatcher_;
  private readonly ILogger        logger_;
  private const    int            PageSize = 100;

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

  public IAsyncEnumerable<GraphContentResponse> GetGraph(string            sessionId,
                                                         CancellationToken cancellationToken)
  {
    var channel = Channel.CreateUnbounded<GraphContentResponse>();

    Task.Factory.StartNew(async () =>
                          {
                            var                                           read = 0;
                            var                                           page = 0;
                            (IEnumerable<TaskData> tasks, int totalCount) res;
                            while ((res = await taskTable_.ListTasksAsync(data => data.SessionId == sessionId,
                                                                          data => data.CreationDate,
                                                                          false,
                                                                          page,
                                                                          PageSize,
                                                                          cancellationToken)
                                                          .ConfigureAwait(false)).totalCount > read)
                            {
                              using var enumerator = res.tasks.GetEnumerator();

                              while (enumerator.MoveNext())
                              {
                                var cur = enumerator.Current;
                                await channel.Writer.WriteAsync(new GraphContentResponse
                                                                {
                                                                  NewTask = new GraphContentResponse.Types.NewTask
                                                                            {
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

    Task.Factory.StartNew(async () =>
                          {
                            var                                           read = 0;
                            var                                           page = 0;
                            (IEnumerable<Result> results, int totalCount) res;
                            while ((res = await resultTable_.ListResultsAsync(data => data.SessionId == sessionId,
                                                                              data => data.CreationDate,
                                                                              false,
                                                                              page,
                                                                              PageSize,
                                                                              cancellationToken)
                                                            .ConfigureAwait(false)).totalCount > read)
                            {
                              using var enumerator = res.results.GetEnumerator();

                              while (enumerator.MoveNext())
                              {
                                var cur = enumerator.Current;
                                await channel.Writer.WriteAsync(new GraphContentResponse
                                                                {
                                                                  NewResult = new GraphContentResponse.Types.NewResult
                                                                              {
                                                                                Status   = cur.Status,
                                                                                OwnerId  = cur.OwnerTaskId,
                                                                                ResultId = cur.Name,
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

    Task.Factory.StartNew(async () =>
                          {
                            var newTasks = await taskWatcher_.GetNewTasks(sessionId,
                                                                          cancellationToken)
                                                             .ConfigureAwait(false);

                            while (newTasks.MoveNext(cancellationToken))
                            {
                              var cur = newTasks.Current;
                              await channel.Writer.WriteAsync(new GraphContentResponse
                                                              {
                                                                NewTask = new GraphContentResponse.Types.NewTask
                                                                          {
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

    Task.Factory.StartNew(async () =>
                          {
                            var newTasks = await taskWatcher_.GetTaskStatusUpdates(sessionId,
                                                                                   cancellationToken)
                                                             .ConfigureAwait(false);

                            while (newTasks.MoveNext(cancellationToken))
                            {
                              var cur = newTasks.Current;
                              await channel.Writer.WriteAsync(new GraphContentResponse
                                                              {
                                                                TaskStatusUpdate = new GraphContentResponse.Types.TaskStatusUpdate
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

    Task.Factory.StartNew(async () =>
                          {
                            var newResults = await resultWatcher_.GetNewResults(sessionId,
                                                                                cancellationToken)
                                                                 .ConfigureAwait(false);

                            while (newResults.MoveNext(cancellationToken))
                            {
                              var cur = newResults.Current;
                              await channel.Writer.WriteAsync(new GraphContentResponse
                                                              {
                                                                NewResult = new GraphContentResponse.Types.NewResult
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

    Task.Factory.StartNew(async () =>
                          {
                            var newResults = await resultWatcher_.GetResultStatusUpdates(sessionId,
                                                                                         cancellationToken)
                                                                 .ConfigureAwait(false);

                            while (newResults.MoveNext(cancellationToken))
                            {
                              var cur = newResults.Current;
                              await channel.Writer.WriteAsync(new GraphContentResponse
                                                              {
                                                                ResultStatusUpdate = new GraphContentResponse.Types.ResultStatusUpdate
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

    Task.Factory.StartNew(async () =>
                          {
                            var newResults = await resultWatcher_.GetResultOwnerUpdates(sessionId,
                                                                                        cancellationToken)
                                                                 .ConfigureAwait(false);

                            while (newResults.MoveNext(cancellationToken))
                            {
                              var cur = newResults.Current;
                              await channel.Writer.WriteAsync(new GraphContentResponse
                                                              {
                                                                ResultOwnerUpdate = new GraphContentResponse.Types.ResultOwnerUpdate
                                                                                    {
                                                                                      ResultId = cur.ResultId,
                                                                                      Current  = cur.NewOwner,
                                                                                      Previous = cur.PreviousOwnerId,
                                                                                    },
                                                                SessionId = cur.SessionId,
                                                              },
                                                              CancellationToken.None)
                                           .ConfigureAwait(false);
                            }
                          },
                          cancellationToken);

    return channel.Reader.ReadAllAsync(cancellationToken);
  }
}
