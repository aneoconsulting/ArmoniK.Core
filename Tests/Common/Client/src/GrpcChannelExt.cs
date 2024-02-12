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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Tasks;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using FilterStatus = ArmoniK.Api.gRPC.V1.Tasks.FilterStatus;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Client;

public static class GrpcChannelExt
{
  public static async IAsyncEnumerable<TaskDetailed> ListTasksAsync(this ChannelBase            channel,
                                                                    Filters                     filters,
                                                                    ListTasksRequest.Types.Sort sort,
                                                                    int                         pageSize = 50)
  {
    var                       page       = 0;
    var                       taskClient = new Tasks.TasksClient(channel);
    ListTasksDetailedResponse res;

    while ((res = await taskClient.ListTasksDetailedAsync(new ListTasksRequest
                                                          {
                                                            Filters  = filters,
                                                            Sort     = sort,
                                                            PageSize = pageSize,
                                                            Page     = page,
                                                          })
                                  .ConfigureAwait(false)).Tasks.Any())
    {
      foreach (var taskDetailed in res.Tasks)
      {
        yield return taskDetailed;
      }

      page++;
    }
  }


  public static async Task LogStatsFromSessionAsync(this ChannelBase channel,
                                                    string           sessionId,
                                                    ILogger          logger)
  {
    var resultClient = new Results.ResultsClient(channel);

    var taskDependencies = new Dictionary<string, TaskDetailed>();
    var taskAggregation  = new List<TaskDetailed>();
    var usageRatio       = new List<double>();


    await foreach (var taskDetailed in channel.ListTasksAsync(new Filters
                                                              {
                                                                Or =
                                                                {
                                                                  new FiltersAnd
                                                                  {
                                                                    And =
                                                                    {
                                                                      new FilterField
                                                                      {
                                                                        Field = new TaskField
                                                                                {
                                                                                  TaskSummaryField = new TaskSummaryField
                                                                                                     {
                                                                                                       Field = TaskSummaryEnumField.SessionId,
                                                                                                     },
                                                                                },
                                                                        FilterString = new FilterString
                                                                                       {
                                                                                         Value    = sessionId,
                                                                                         Operator = FilterStringOperator.Equal,
                                                                                       },
                                                                      },
                                                                    },
                                                                  },
                                                                },
                                                              },
                                                              new ListTasksRequest.Types.Sort
                                                              {
                                                                Direction = SortDirection.Asc,
                                                                Field = new TaskField
                                                                        {
                                                                          TaskSummaryField = new TaskSummaryField
                                                                                             {
                                                                                               Field = TaskSummaryEnumField.TaskId,
                                                                                             },
                                                                        },
                                                              })
                                              .ConfigureAwait(false))
    {
      try
      {
        if (taskDetailed.Status is TaskStatus.Completed or TaskStatus.Error or TaskStatus.Retried)
        {
          var useRatio = (taskDetailed.EndedAt - taskDetailed.StartedAt).ToTimeSpan()
                                                                        .TotalMilliseconds / (taskDetailed.EndedAt - taskDetailed.ReceivedAt).ToTimeSpan()
                                                                                                                                             .TotalMilliseconds;

          usageRatio.Add(useRatio);
        }

        if (taskDetailed.DataDependencies.Count > 0)
        {
          taskAggregation.Add(taskDetailed);
        }

        taskDependencies.Add(taskDetailed.Id,
                             taskDetailed);
      }
      catch (Exception e)
      {
        logger.LogError(e,
                        "Cannot process {@task}",
                        taskDetailed);
        throw;
      }
    }

    var timediff = new List<double>();

    foreach (var agg in taskAggregation)
    {
      var result2Task = resultClient.GetOwnerTaskId(new GetOwnerTaskIdRequest
                                                    {
                                                      ResultId =
                                                      {
                                                        agg.DataDependencies,
                                                      },
                                                      SessionId = sessionId,
                                                    })
                                    .ResultTask.Select(m => m.TaskId);

      var lastDependencyFinished = taskDependencies.Where(pair => result2Task.Contains(pair.Key) && pair.Value.EndedAt is not null)
                                                   .Select(pair => pair.Value)
                                                   .MaxBy(pair => pair.EndedAt);
      if (agg.StartedAt is not null && lastDependencyFinished is not null)
      {
        var diff = agg.StartedAt - lastDependencyFinished.EndedAt;
        timediff.Add(diff.ToTimeSpan()
                         .TotalMilliseconds / 1000);
      }
    }


    if (timediff.Any())
    {
      logger.LogInformation("Time spent between the end of the last dependency and the start of aggregation tasks {min}, {max}, {avg}",
                            timediff.Min(),
                            timediff.Max(),
                            timediff.Average());
    }

    if (usageRatio.Any())
    {
      logger.LogInformation("Ratio between time spent on a task and processing time {min}, {max}, {avg}",
                            usageRatio.Min(),
                            usageRatio.Max(),
                            usageRatio.Average());
    }

    var timeSpentList = taskDependencies.Values.Where(raw => raw.SubmittedAt is not null && raw.CreatedAt is not null)
                                        .Select(raw => (raw.SubmittedAt - raw.CreatedAt).ToTimeSpan()
                                                                                        .TotalMilliseconds / 1000)
                                        .ToList();
    logger.LogInformation("Time spent between {status1} and {status2} : {min}s, {max}s, {avg}s",
                          TaskStatus.Creating,
                          TaskStatus.Submitted,
                          timeSpentList.Min(),
                          timeSpentList.Max(),
                          timeSpentList.Average());

    timeSpentList = taskDependencies.Values.Where(raw => raw.StartedAt is not null && raw.AcquiredAt is not null)
                                    .Select(raw => (raw.StartedAt - raw.AcquiredAt).ToTimeSpan()
                                                                                   .TotalMilliseconds / 1000)
                                    .ToList();
    logger.LogInformation("Time spent between {status1} and {status2} : {min}s, {max}s, {avg}s",
                          TaskStatus.Dispatched,
                          TaskStatus.Processing,
                          timeSpentList.Min(),
                          timeSpentList.Max(),
                          timeSpentList.Average());

    timeSpentList = taskDependencies.Values.Where(raw => raw.EndedAt is not null && raw.StartedAt is not null)
                                    .Select(raw => (raw.EndedAt - raw.StartedAt).ToTimeSpan()
                                                                                .TotalMilliseconds / 1000)
                                    .ToList();
    logger.LogInformation("Time spent between {status1} and {status2} : {min}s, {max}s, {avg}s",
                          TaskStatus.Processing,
                          TaskStatus.Completed,
                          timeSpentList.Min(),
                          timeSpentList.Max(),
                          timeSpentList.Average());

    timeSpentList = taskDependencies.Values.Where(raw => raw.EndedAt is not null && raw.AcquiredAt is not null)
                                    .Select(raw => (raw.EndedAt - raw.AcquiredAt).ToTimeSpan()
                                                                                 .TotalMilliseconds / 1000)
                                    .ToList();
    logger.LogInformation("Time spent between {status1} and {status2} : {min}s, {max}s, {avg}s",
                          TaskStatus.Dispatched,
                          TaskStatus.Completed,
                          timeSpentList.Min(),
                          timeSpentList.Max(),
                          timeSpentList.Average());

    var sessionStart = taskDependencies.Values.Where(raw => raw.CreatedAt is not null)
                                       .Min(raw => raw.CreatedAt);
    var sessionEnd = taskDependencies.Values.Where(raw => raw.EndedAt is not null)
                                     .Max(raw => raw.EndedAt);
    var sessionDuration = (sessionEnd - sessionStart).ToTimeSpan();
    var taskCount       = taskDependencies.Count(pair => pair.Value.Status is TaskStatus.Completed or TaskStatus.Error or TaskStatus.Retried);

    logger.LogInformation("Throughput for session {session} : {sessionThroughput} task/s (completed {nTasks}, total {total} tasks in {timespan})",
                          sessionId,
                          taskCount / sessionDuration.TotalMilliseconds * 1000,
                          taskCount,
                          taskDependencies.Count,
                          sessionDuration);
    var count = 0;
    await foreach (var _ in channel.ListTasksAsync(new Filters
                                                   {
                                                     Or =
                                                     {
                                                       FilterStatus(sessionStart - Duration.FromTimeSpan(TimeSpan.FromMilliseconds(1)),
                                                                    sessionEnd   + Duration.FromTimeSpan(TimeSpan.FromMilliseconds(1)),
                                                                    TaskStatus.Completed,
                                                                    TaskStatus.Error,
                                                                    TaskStatus.Retried),
                                                     },
                                                   },
                                                   new ListTasksRequest.Types.Sort
                                                   {
                                                     Direction = SortDirection.Asc,
                                                     Field = new TaskField
                                                             {
                                                               TaskSummaryField = new TaskSummaryField
                                                                                  {
                                                                                    Field = TaskSummaryEnumField.TaskId,
                                                                                  },
                                                             },
                                                   })
                                   .ConfigureAwait(false))
    {
      count++;
    }

    logger.LogInformation("Throughput during session {session} : {throughput} task/s ({nTasks} tasks in {timespan})",
                          sessionId,
                          count / sessionDuration.TotalMilliseconds * 1000,
                          count,
                          sessionDuration);
  }

  private static IEnumerable<FiltersAnd> FilterStatus(Timestamp           createdAfter,
                                                      Timestamp           endedBefore,
                                                      params TaskStatus[] statuses)
    => statuses.Select(status => new FiltersAnd
                                 {
                                   And =
                                   {
                                     new FilterField
                                     {
                                       Field = new TaskField
                                               {
                                                 TaskSummaryField = new TaskSummaryField
                                                                    {
                                                                      Field = TaskSummaryEnumField.CreatedAt,
                                                                    },
                                               },
                                       FilterDate = new FilterDate
                                                    {
                                                      Operator = FilterDateOperator.After,
                                                      Value    = createdAfter,
                                                    },
                                     },
                                     new FilterField
                                     {
                                       Field = new TaskField
                                               {
                                                 TaskSummaryField = new TaskSummaryField
                                                                    {
                                                                      Field = TaskSummaryEnumField.EndedAt,
                                                                    },
                                               },
                                       FilterDate = new FilterDate
                                                    {
                                                      Operator = FilterDateOperator.Before,
                                                      Value    = endedBefore,
                                                    },
                                     },
                                     new FilterField
                                     {
                                       Field = new TaskField
                                               {
                                                 TaskSummaryField = new TaskSummaryField
                                                                    {
                                                                      Field = TaskSummaryEnumField.Status,
                                                                    },
                                               },
                                       FilterStatus = new FilterStatus
                                                      {
                                                        Operator = FilterStatusOperator.Equal,
                                                        Value    = status,
                                                      },
                                     },
                                   },
                                 });

  private static IEnumerable<FiltersAnd> FilterStatus(string              sessionId,
                                                      params TaskStatus[] statuses)
    => statuses.Select(status => new FiltersAnd
                                 {
                                   And =
                                   {
                                     new FilterField
                                     {
                                       Field = new TaskField
                                               {
                                                 TaskSummaryField = new TaskSummaryField
                                                                    {
                                                                      Field = TaskSummaryEnumField.SessionId,
                                                                    },
                                               },
                                       FilterString = new FilterString
                                                      {
                                                        Operator = FilterStringOperator.Equal,
                                                        Value    = sessionId,
                                                      },
                                     },
                                     new FilterField
                                     {
                                       Field = new TaskField
                                               {
                                                 TaskSummaryField = new TaskSummaryField
                                                                    {
                                                                      Field = TaskSummaryEnumField.Status,
                                                                    },
                                               },
                                       FilterStatus = new FilterStatus
                                                      {
                                                        Operator = FilterStatusOperator.Equal,
                                                        Value    = status,
                                                      },
                                     },
                                   },
                                 });

  private static Api.gRPC.V1.Results.FiltersAnd ResultsFilter(string resultId)
    => new()
       {
         And =
         {
           new Api.gRPC.V1.Results.FilterField
           {
             Field = new ResultField
                     {
                       ResultRawField = new ResultRawField
                                        {
                                          Field = ResultRawEnumField.ResultId,
                                        },
                     },
             FilterString = new FilterString
                            {
                              Operator = FilterStringOperator.Equal,
                              Value    = resultId,
                            },
           },
         },
       };

  public static async Task WaitForResultsAsync(this ChannelBase    channel,
                                               string              sessionId,
                                               ICollection<string> resultIds,
                                               CancellationToken   cancellationToken)
  {
    var eventsClient = new Events.EventsClient(channel);

    var resultsNotFound = resultIds.ToDictionary(id => id,
                                                 _ => true);

    using var streamingCall = eventsClient.GetEvents(new EventSubscriptionRequest
                                                     {
                                                       SessionId = sessionId,
                                                       ReturnedEvents =
                                                       {
                                                         EventsEnum.ResultStatusUpdate,
                                                         EventsEnum.NewResult,
                                                       },
                                                       ResultsFilters = new Api.gRPC.V1.Results.Filters
                                                                        {
                                                                          Or =
                                                                          {
                                                                            resultIds.Select(ResultsFilter),
                                                                          },
                                                                        },
                                                     });


    await foreach (var resp in streamingCall.ResponseStream.ReadAllAsync(cancellationToken)
                                            .ConfigureAwait(false))
    {
      if (resp.UpdateCase == EventSubscriptionResponse.UpdateOneofCase.ResultStatusUpdate && resultsNotFound.ContainsKey(resp.ResultStatusUpdate.ResultId))
      {
        if (resp.ResultStatusUpdate.Status == ResultStatus.Completed)
        {
          resultsNotFound.Remove(resp.ResultStatusUpdate.ResultId);
          if (!resultsNotFound.Any())
          {
            break;
          }
        }

        if (resp.ResultStatusUpdate.Status == ResultStatus.Aborted)
        {
          throw new Exception($"Result {resp.ResultStatusUpdate.ResultId} has been aborted");
        }
      }

      if (resp.UpdateCase == EventSubscriptionResponse.UpdateOneofCase.NewResult && resultsNotFound.ContainsKey(resp.NewResult.ResultId))
      {
        if (resp.NewResult.Status == ResultStatus.Completed)
        {
          resultsNotFound.Remove(resp.NewResult.ResultId);
          if (!resultsNotFound.Any())
          {
            break;
          }
        }

        if (resp.NewResult.Status == ResultStatus.Aborted)
        {
          throw new Exception($"Result {resp.NewResult.ResultId} has been aborted");
        }
      }
    }
  }


  public static async Task<SessionStats> ComputeThroughput(this ChannelBase  channel,
                                                           string            sessionId,
                                                           CancellationToken cancellationToken = default)
  {
    var client = new Tasks.TasksClient(channel);

    var first = (await client.ListTasksAsync(new ListTasksRequest
                                             {
                                               Page     = 0,
                                               PageSize = 1,
                                               Filters = new Filters
                                                         {
                                                           Or =
                                                           {
                                                             FilterStatus(sessionId,
                                                                          TaskStatus.Error,
                                                                          TaskStatus.Completed,
                                                                          TaskStatus.Retried),
                                                           },
                                                         },
                                               Sort = new ListTasksRequest.Types.Sort
                                                      {
                                                        Direction = SortDirection.Asc,
                                                        Field = new TaskField
                                                                {
                                                                  TaskSummaryField = new TaskSummaryField
                                                                                     {
                                                                                       Field = TaskSummaryEnumField.CreatedAt,
                                                                                     },
                                                                },
                                                      },
                                             },
                                             cancellationToken: cancellationToken)).Tasks.Single();

    var end = await client.ListTasksAsync(new ListTasksRequest
                                          {
                                            Page     = 0,
                                            PageSize = 1,
                                            Filters = new Filters
                                                      {
                                                        Or =
                                                        {
                                                          FilterStatus(sessionId,
                                                                       TaskStatus.Error,
                                                                       TaskStatus.Completed,
                                                                       TaskStatus.Retried),
                                                        },
                                                      },
                                            Sort = new ListTasksRequest.Types.Sort
                                                   {
                                                     Direction = SortDirection.Desc,
                                                     Field = new TaskField
                                                             {
                                                               TaskSummaryField = new TaskSummaryField
                                                                                  {
                                                                                    Field = TaskSummaryEnumField.EndedAt,
                                                                                  },
                                                             },
                                                   },
                                          },
                                          cancellationToken: cancellationToken);

    var last = end.Tasks.Single();

    return new SessionStats
           {
             Duration   = (last.EndedAt - first.CreatedAt).ToTimeSpan(),
             TasksCount = end.Total,
           };
  }
}
