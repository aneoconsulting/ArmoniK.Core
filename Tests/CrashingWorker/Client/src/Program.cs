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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Samples.CrashingWorker.Client.Options;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Formatting.Compact;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Samples.CrashingWorker.Client;

internal static class Program
{
  private static async Task Main()
  {
    var builder       = new ConfigurationBuilder().AddEnvironmentVariables();
    var configuration = builder.Build();
    var seriLogger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                              .Enrich.FromLogContext()
                                              .WriteTo.Console(new CompactJsonFormatter())
                                              .CreateBootstrapLogger();

    var logger = new LoggerFactory().AddSerilog(seriLogger)
                                    .CreateLogger("Bench Program");

    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();
    logger.LogInformation("gRPC options : {@grpcOptions}",
                          options);
    var testOptions = new CrashingWorkerOptions();
    configuration.GetSection(CrashingWorkerOptions.SettingSection)
                 .Bind(testOptions);
    logger.LogInformation("options : {@options}",
                          testOptions);
    using var _ = logger.BeginPropertyScope(("@options", testOptions));

    var channelPool = new ObjectPool<ChannelBase>(() => GrpcChannelFactory.CreateChannel(options!));

    // Create a new session
    var createSessionReply = await channelPool.WithInstanceAsync(async channel =>
                                                                 {
                                                                   var client = new Sessions.SessionsClient(channel);

                                                                   var req = new CreateSessionRequest
                                                                             {
                                                                               DefaultTaskOption = new TaskOptions
                                                                                                   {
                                                                                                     MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                                                                                     MaxRetries  = 2,
                                                                                                     Priority    = 1,
                                                                                                     PartitionId = testOptions.Partition,
                                                                                                   },
                                                                               PartitionIds =
                                                                               {
                                                                                 testOptions.Partition,
                                                                               },
                                                                             };
                                                                   return await client.CreateSessionAsync(req);
                                                                 })
                                              .ConfigureAwait(false);
    logger.LogInformation("Session Id : {sessionId}",
                          createSessionReply.SessionId);

    Console.CancelKeyPress += (_,
                               args) =>
                              {
                                args.Cancel = true;
                                using var channel = channelPool.Get();
                                var       client  = new Sessions.SessionsClient(channel);
                                client.CancelSession(new CancelSessionRequest
                                                     {
                                                       SessionId = createSessionReply.SessionId,
                                                     });

                                Environment.Exit(0);
                              };

    var nTasks = 10;

    // create payloads
    var payloadIds = await channelPool.WithInstanceAsync(async channel =>
                                                         {
                                                           var resultClient = new Results.ResultsClient(channel);

                                                           var payloads = await resultClient.CreateResultsAsync(new CreateResultsRequest
                                                                                                                {
                                                                                                                  SessionId = createSessionReply.SessionId,
                                                                                                                  Results =
                                                                                                                  {
                                                                                                                    Enumerable.Range(0,
                                                                                                                                     nTasks)
                                                                                                                              .Select(i => new CreateResultsRequest.Types
                                                                                                                                           .ResultCreate
                                                                                                                                           {
                                                                                                                                             Name = $"payload {i}",
                                                                                                                                             Data = ByteString
                                                                                                                                               .CopyFromUtf8($"{i}"),
                                                                                                                                           }),
                                                                                                                  },
                                                                                                                })
                                                                                            .ConfigureAwait(false);

                                                           return payloads.Results.Select(raw => raw.ResultId);
                                                         },
                                                         CancellationToken.None)
                                      .ConfigureAwait(false);


    var resultIds = await channelPool.WithInstanceAsync(async channel =>
                                                        {
                                                          var resultClient = new Results.ResultsClient(channel);


                                                          var results = await resultClient.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                                                                      {
                                                                                                                        SessionId = createSessionReply.SessionId,
                                                                                                                        Results =
                                                                                                                        {
                                                                                                                          Enumerable.Range(0,
                                                                                                                                           nTasks)
                                                                                                                                    .Select(i => new
                                                                                                                                                 CreateResultsMetaDataRequest
                                                                                                                                                 .Types.ResultCreate
                                                                                                                                                 {
                                                                                                                                                   Name = $"result {i}",
                                                                                                                                                 }),
                                                                                                                        },
                                                                                                                      });

                                                          return results.Results.Select(raw => raw.ResultId)
                                                                        .AsICollection();
                                                        },
                                                        CancellationToken.None)
                                     .ConfigureAwait(false);

    await channelPool.WithInstanceAsync(async channel =>
                                        {
                                          var tasksClient = new Tasks.TasksClient(channel);

                                          var submitResponse = await tasksClient.SubmitTasksAsync(new SubmitTasksRequest
                                                                                                  {
                                                                                                    SessionId = createSessionReply.SessionId,
                                                                                                    TaskCreations =
                                                                                                    {
                                                                                                      resultIds.Select((_,
                                                                                                                        i) => new SubmitTasksRequest.Types.TaskCreation
                                                                                                                              {
                                                                                                                                PayloadId = payloadIds.ElementAt(i),
                                                                                                                                ExpectedOutputKeys =
                                                                                                                                {
                                                                                                                                  resultIds.ElementAt(i),
                                                                                                                                },
                                                                                                                              }),
                                                                                                    },
                                                                                                  });

                                          if (logger.IsEnabled(LogLevel.Debug))
                                          {
                                            foreach (var info in submitResponse.TaskInfos)
                                            {
                                              logger.LogDebug("task created {taskId}",
                                                              info.TaskId);
                                            }
                                          }
                                        },
                                        CancellationToken.None)
                     .ConfigureAwait(false);

    await channelPool.WithInstanceAsync(async channel =>
                                        {
                                          var client = new Events.EventsClient(channel);

                                          foreach (var resultId in resultIds)
                                          {
                                            try
                                            {
                                              await client.WaitForResultsAsync(createSessionReply.SessionId,
                                                                               new[]
                                                                               {
                                                                                 resultId,
                                                                               },
                                                                               CancellationToken.None)
                                                          .ConfigureAwait(false);
                                            }
                                            catch (Exception)
                                            {
                                              continue;
                                            }

                                            throw new Exception("Result should have been aborted");
                                          }
                                        },
                                        CancellationToken.None)
                     .ConfigureAwait(false);


    await channelPool.WithInstanceAsync(async channel =>
                                        {
                                          var client = new Results.ResultsClient(channel);

                                          foreach (var resultId in resultIds)
                                          {
                                            var result = await client.GetResultAsync(new GetResultRequest
                                                                                     {
                                                                                       ResultId = resultId,
                                                                                     });

                                            if (result.Result.Status != ResultStatus.Aborted)
                                            {
                                              throw new Exception("Result should have been aborted");
                                            }
                                          }
                                        },
                                        CancellationToken.None)
                     .ConfigureAwait(false);

    await channelPool.WithInstanceAsync(async channel =>
                                        {
                                          var client = new Tasks.TasksClient(channel);

                                          var tasks = await client.ListTasksAsync(new ListTasksRequest
                                                                                  {
                                                                                    Filters = new Filters
                                                                                              {
                                                                                                Or =
                                                                                                {
                                                                                                  new FiltersAnd
                                                                                                  {
                                                                                                    And =
                                                                                                    {
                                                                                                      new FilterField
                                                                                                      {
                                                                                                        FilterString = new FilterString
                                                                                                                       {
                                                                                                                         Operator = FilterStringOperator.Equal,
                                                                                                                         Value    = createSessionReply.SessionId,
                                                                                                                       },
                                                                                                        Field = new TaskField
                                                                                                                {
                                                                                                                  TaskSummaryField = new TaskSummaryField
                                                                                                                                     {
                                                                                                                                       Field = TaskSummaryEnumField
                                                                                                                                         .SessionId,
                                                                                                                                     },
                                                                                                                },
                                                                                                      },
                                                                                                    },
                                                                                                  },
                                                                                                },
                                                                                              },
                                                                                    Sort = new ListTasksRequest.Types.Sort
                                                                                           {
                                                                                             Direction = SortDirection.Asc,
                                                                                             Field = new TaskField
                                                                                                     {
                                                                                                       TaskSummaryField = new TaskSummaryField
                                                                                                                          {
                                                                                                                            Field = TaskSummaryEnumField.TaskId,
                                                                                                                          },
                                                                                                     },
                                                                                           },
                                                                                    Page     = 1,
                                                                                    PageSize = 100,
                                                                                  });

                                          if (tasks.Tasks.Any(task => task.Status is not TaskStatus.Error or TaskStatus.Retried))
                                          {
                                            throw new Exception("Tasks statuses from session should be error or retried");
                                          }
                                        },
                                        CancellationToken.None)
                     .ConfigureAwait(false);
    logger.LogInformation("Successful end of the application");
  }
}
