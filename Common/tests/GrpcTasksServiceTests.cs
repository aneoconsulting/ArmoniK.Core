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

using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(GrpcTasksService))]
public class GrpcTasksServiceTests
{
  [SetUp]
  public void Setup()
    => helper_ = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<FunctionExecutionMetricsFactory>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder => builder.MapGrpcService<GrpcTasksService>(),
                                          true);

  [TearDown]
  public void TearDown()
    => helper_?.Dispose();

  private TestDatabaseProvider? helper_;


  [Test]
  public async Task CancelNotExistingTaskShouldSucceed()
  {
    await helper_!.App.StartAsync()
                  .ConfigureAwait(false);

    var server = helper_.App.GetTestServer();

    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var client = new Tasks.TasksClient(channel);

    var response = client.CancelTasks(new CancelTasksRequest
                                      {
                                        TaskIds =
                                        {
                                          "test",
                                        },
                                      });

    Assert.That(response,
                Is.Not.Null);
    Assert.That(response.Tasks.Count,
                Is.EqualTo(0));
  }

  [Test]
  public async Task CancelNonScheduledTaskShouldSucceed()
  {
    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<FunctionExecutionMetricsFactory>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
                                            builder.MapGrpcService<GrpcTasksService>();
                                            builder.MapGrpcService<GrpcSessionsService>();
                                            builder.MapGrpcService<GrpcResultsService>();
                                          },
                                          true);
    await helper.App.StartAsync()
                .ConfigureAwait(false);

    var server = helper.App.GetTestServer();

    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var session_id = new Sessions.SessionsClient(channel).CreateSession(new CreateSessionRequest
                                                                        {
                                                                          DefaultTaskOption = new TaskOptions
                                                                                              {
                                                                                                MaxRetries = 1,
                                                                                                Priority   = 2,
                                                                                                MaxDuration = new Duration
                                                                                                              {
                                                                                                                Seconds = 500,
                                                                                                                Nanos   = 0,
                                                                                                              },
                                                                                              },
                                                                        })
                                                         .SessionId;
    var result_client = new Results.ResultsClient(channel);
    var result_id = result_client.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                        {
                                                          SessionId = session_id,
                                                          Results =
                                                          {
                                                            new CreateResultsMetaDataRequest.Types.ResultCreate
                                                            {
                                                              Name = "result_id",
                                                            },
                                                          },
                                                        })
                                 .Results.Single()
                                 .ResultId;
    var payload_id = result_client.CreateResults(new CreateResultsRequest
                                                 {
                                                   SessionId = session_id,
                                                   Results =
                                                   {
                                                     new CreateResultsRequest.Types.ResultCreate
                                                     {
                                                       Name = "payload",
                                                     },
                                                   },
                                                 })
                                  .Results.Single()
                                  .ResultId;

    var client = new Tasks.TasksClient(channel);
    // Submit an unschedulable task
    var task_id = client.SubmitTasks(new SubmitTasksRequest
                                     {
                                       SessionId = session_id,
                                       TaskCreations =
                                       {
                                         new SubmitTasksRequest.Types.TaskCreation
                                         {
                                           PayloadId = payload_id,
                                           DataDependencies =
                                           {
                                             result_id,
                                           },
                                         },
                                       },
                                     })
                        .TaskInfos.Single()
                        .TaskId;

    var response = client.CancelTasks(new CancelTasksRequest
                                      {
                                        TaskIds =
                                        {
                                          task_id,
                                        },
                                      });

    Assert.That(response,
                Is.Not.Null);
    Assert.That(response.Tasks.Count,
                Is.EqualTo(1));
    Assert.That(response.Tasks.Single()
                        .Id,
                Is.EqualTo(task_id));
  }

  [Test]
  public async Task SubmitTaskWithoutCreationsShouldFailWithRpcException()
  {
    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<FunctionExecutionMetricsFactory>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
                                            builder.MapGrpcService<GrpcTasksService>();
                                            builder.MapGrpcService<GrpcSessionsService>();
                                          },
                                          true,
                                          true);

    await helper.App.StartAsync()
                .ConfigureAwait(false);

    var server = helper.App.GetTestServer();

    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var sessionId = new Sessions.SessionsClient(channel).CreateSession(new CreateSessionRequest
                                                                       {
                                                                         DefaultTaskOption = new TaskOptions
                                                                                             {
                                                                                               MaxRetries = 1,
                                                                                               Priority   = 2,
                                                                                               MaxDuration = new Duration
                                                                                                             {
                                                                                                               Seconds = 500,
                                                                                                               Nanos   = 0,
                                                                                                             },
                                                                                             },
                                                                       })
                                                        .SessionId;

    var client = new Tasks.TasksClient(channel);
    Assert.That(delegate
                {
                  client.SubmitTasks(new SubmitTasksRequest
                                     {
                                       SessionId = sessionId,
                                     });
                },
                Throws.InstanceOf<RpcException>()
                      .With.Property(nameof(RpcException.StatusCode))
                      .EqualTo(StatusCode.InvalidArgument));
  }

  [Test]
  public async Task SubmitTaskWithSubmissionClosedShouldFailWithRpcException()
  {
    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<FunctionExecutionMetricsFactory>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
                                            builder.MapGrpcService<GrpcTasksService>();
                                            builder.MapGrpcService<GrpcSessionsService>();
                                          },
                                          true,
                                          true);

    await helper.App.StartAsync()
                .ConfigureAwait(false);

    var server = helper.App.GetTestServer();

    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var sessionId = new Sessions.SessionsClient(channel).CreateSession(new CreateSessionRequest
                                                                       {
                                                                         DefaultTaskOption = new TaskOptions
                                                                                             {
                                                                                               MaxRetries = 1,
                                                                                               Priority   = 2,
                                                                                               MaxDuration = new Duration
                                                                                                             {
                                                                                                               Seconds = 500,
                                                                                                               Nanos   = 0,
                                                                                                             },
                                                                                             },
                                                                       })
                                                        .SessionId;

    new Sessions.SessionsClient(channel).StopSubmission(new StopSubmissionRequest
                                                        {
                                                          Client    = true,
                                                          SessionId = sessionId,
                                                        });

    var client = new Tasks.TasksClient(channel);
    Assert.That(delegate
                {
                  client.SubmitTasks(new SubmitTasksRequest
                                     {
                                       SessionId = sessionId,
                                       TaskCreations =
                                       {
                                         new SubmitTasksRequest.Types.TaskCreation
                                         {
                                           PayloadId = "payload",
                                         },
                                       },
                                     });
                },
                Throws.InstanceOf<RpcException>()
                      .With.Property(nameof(RpcException.StatusCode))
                      .EqualTo(StatusCode.FailedPrecondition));
  }
}
