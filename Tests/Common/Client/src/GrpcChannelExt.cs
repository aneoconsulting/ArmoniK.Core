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
using System.Threading.Tasks;

using Armonik.Api.gRPC.V1;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;

using Armonik.Api.gRPC.V1.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using FilterField = Armonik.Api.gRPC.V1.Tasks.FilterField;
using Filters = Armonik.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = Armonik.Api.gRPC.V1.Tasks.FiltersAnd;
using FilterStatus = Armonik.Api.gRPC.V1.Tasks.FilterStatus;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Client;

public static class GrpcChannelExt
{
  public static async IAsyncEnumerable<TaskRaw> ListTasksAsync(this ChannelBase            channel,
                                                               Filters                     filters,
                                                               ListTasksRequest.Types.Sort sort)
  {
    var               read       = 0;
    var               page       = 0;
    var               taskClient = new Tasks.TasksClient(channel);
    ListTasksResponse res;

    while ((res = await taskClient.ListTasksAsync(new ListTasksRequest
                                                  {
                                                    Filters  = filters,
                                                    Sort     = sort,
                                                    PageSize = 50,
                                                    Page     = page,
                                                  })
                                  .ConfigureAwait(false)).Total > read)
    {
      foreach (var taskSummary in res.Tasks)
      {
        var taskRaw = taskClient.GetTask(new GetTaskRequest
                                         {
                                           TaskId = taskSummary.Id,
                                         })
                                .Task;
        read++;
        yield return taskRaw;
      }

      page++;
    }
  }


  public static async Task LogStatsFromSessionAsync(this ChannelBase channel,
                                                    string           sessionId,
                                                    ILogger          logger)
  {
    var resultClient = new Results.ResultsClient(channel);

    var taskDependencies = new Dictionary<string, TaskRaw>();
    var taskAggregation  = new List<TaskRaw>();
    var usageRatio       = new List<double>();


    await foreach (var taskRaw in channel.ListTasksAsync(new Filters
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
      if (taskRaw.Status is TaskStatus.Completed or TaskStatus.Error or TaskStatus.Retried)
      {
        var useRatio = (taskRaw.EndedAt - taskRaw.StartedAt).ToTimeSpan()
                                                            .TotalMilliseconds / (taskRaw.EndedAt - taskRaw.ReceivedAt).ToTimeSpan()
                                                                                                                       .TotalMilliseconds;

        usageRatio.Add(useRatio);
      }

      if (taskRaw.DataDependencies.Count > 0)
      {
        taskAggregation.Add(taskRaw);
      }

      taskDependencies.Add(taskRaw.Id,
                           taskRaw);
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
}
