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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
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

using Moq;
using Moq.Protected;

using NUnit.Framework;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(GrpcSessionsService))]
public class GrpcSessionsServiceTests
{
  [Test]
  public async Task CancelNotExistingSessionShouldFail()
  {
    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<MeterHolder>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddScoped(typeof(FunctionExecutionMetrics<>))
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder => builder.MapGrpcService<GrpcSessionsService>(),
                                          true);

    await helper.App.StartAsync()
                .ConfigureAwait(false);

    var server = helper.App.GetTestServer();
    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var sessionClient = new Sessions.SessionsClient(channel);

    var exception = Assert.Throws<RpcException>(() => sessionClient.CancelSession(new CancelSessionRequest
                                                                                  {
                                                                                    SessionId = "non-existent-session-id",
                                                                                  }));

    Assert.Multiple(() =>
                    {
                      Assert.That(exception,
                                  Is.Not.Null);
                      Assert.That(exception.StatusCode,
                                  Is.EqualTo(StatusCode.NotFound));
                    });
  }

  [Test]
  public async Task CancelSessionShouldNotifyAgents()
  {
    var httpMessageHandlerMock = new Mock<HttpMessageHandler>();
    var capturedUris           = new ConcurrentBag<string>();

    httpMessageHandlerMock.Protected()
                          .Setup<Task<HttpResponseMessage>>("SendAsync",
                                                            ItExpr.IsAny<HttpRequestMessage>(),
                                                            ItExpr.IsAny<CancellationToken>())
                          .ReturnsAsync((HttpRequestMessage request,
                                         CancellationToken  _) =>
                                        {
                                          capturedUris.Add(request.RequestUri!.ToString());
                                          return new HttpResponseMessage(HttpStatusCode.OK);
                                        });

    var httpClient = new HttpClient(httpMessageHandlerMock.Object);

    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<MeterHolder>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddScoped(typeof(FunctionExecutionMetrics<>))
                                                                  .AddSingleton(httpClient)
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
                                            builder.MapGrpcService<GrpcSessionsService>();
                                            builder.MapGrpcService<GrpcResultsService>();
                                            builder.MapGrpcService<GrpcTasksService>();
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

    var sessionClient = new Sessions.SessionsClient(channel);

    // Create a session
    var sessionId = sessionClient.CreateSession(new CreateSessionRequest
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

    // Create a result (payload)
    var resultClient = new Results.ResultsClient(channel);
    var payloadId = resultClient.CreateResults(new CreateResultsRequest
                                               {
                                                 SessionId = sessionId,
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

    // Insert tasks with OwnerPodId directly into the database
    var taskTable = helper.GetRequiredService<ITaskTable>();
    var taskId1 = Guid.NewGuid()
                      .ToString();
    var taskId2 = Guid.NewGuid()
                      .ToString();
    var taskId3 = Guid.NewGuid()
                      .ToString();
    var taskId4 = Guid.NewGuid()
                      .ToString();

    await taskTable.CreateTasks(new[]
                                {
                                  new TaskData(sessionId,
                                               taskId1,
                                               "ownerPodId1",
                                               "ownerPodName1",
                                               payloadId,
                                               "CreatedBy",
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>(),
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               new Base.DataStructures.TaskOptions(),
                                               new Output(OutputStatus.Success,
                                                          "")),
                                  new TaskData(sessionId,
                                               taskId2,
                                               "ownerPodId2",
                                               "ownerPodName2",
                                               payloadId,
                                               "CreatedBy",
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>(),
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               new Base.DataStructures.TaskOptions(),
                                               new Output(OutputStatus.Success,
                                                          "")),
                                  new TaskData(sessionId,
                                               taskId3,
                                               "ownerPodId1",
                                               "ownerPodName1",
                                               payloadId,
                                               "CreatedBy",
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>(),
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               new Base.DataStructures.TaskOptions(),
                                               new Output(OutputStatus.Success,
                                                          "")),
                                  new TaskData(sessionId,
                                               taskId4,
                                               "ownerPodId1",
                                               "ownerPodName3",
                                               payloadId,
                                               "CreatedBy",
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>(),
                                               Array.Empty<string>(),
                                               TaskStatus.Completed,
                                               new Base.DataStructures.TaskOptions(),
                                               new Output(OutputStatus.Success,
                                                          "")),
                                })
                   .ConfigureAwait(false);

    // Cancel the session
    var response = sessionClient.CancelSession(new CancelSessionRequest
                                               {
                                                 SessionId = sessionId,
                                               });

    // Verify that HTTP requests were made to notify agents
    // Note: HTTP URLs normalize hostnames to lowercase per RFC standards
    var expectedUris = new[]
                       {
                         "http://ownerpodid1:1080/stopcancelledtask",
                         "http://ownerpodid2:1080/stopcancelledtask",
                       };

    Assert.Multiple(() =>
                    {
                      Assert.That(response,
                                  Is.Not.Null);
                      Assert.That(response.Session,
                                  Is.Not.Null);
                      Assert.That(capturedUris,
                                  Is.EquivalentTo(expectedUris));
                    });


    httpMessageHandlerMock.Protected()
                          .Verify("SendAsync",
                                  Times.Exactly(2),
                                  ItExpr.IsAny<HttpRequestMessage>(),
                                  ItExpr.IsAny<CancellationToken>());
  }

  [Test]
  public async Task CancelSessionShouldHandleUnreachableAgents()
  {
    var httpMessageHandlerMock = new Mock<HttpMessageHandler>();

    httpMessageHandlerMock.Protected()
                          .Setup<Task<HttpResponseMessage>>("SendAsync",
                                                            ItExpr.IsAny<HttpRequestMessage>(),
                                                            ItExpr.IsAny<CancellationToken>())
                          .ThrowsAsync(new HttpRequestException("Connection refused",
                                                                new SocketException((int)SocketError.ConnectionRefused)));

    var httpClient = new HttpClient(httpMessageHandlerMock.Object);

    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<MeterHolder>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddScoped(typeof(FunctionExecutionMetrics<>))
                                                                  .AddSingleton(httpClient)
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
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

    var sessionClient = new Sessions.SessionsClient(channel);

    // Create a session
    var sessionId = sessionClient.CreateSession(new CreateSessionRequest
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

    // Create a result (payload)
    var resultClient = new Results.ResultsClient(channel);
    var payloadId = resultClient.CreateResults(new CreateResultsRequest
                                               {
                                                 SessionId = sessionId,
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

    // Insert a task with OwnerPodId directly into the database
    var taskTable = helper.GetRequiredService<ITaskTable>();
    var taskId = Guid.NewGuid()
                     .ToString();

    await taskTable.CreateTasks(new[]
                                {
                                  new TaskData(sessionId,
                                               taskId,
                                               "ownerPodId",
                                               "ownerPodName",
                                               payloadId,
                                               "CreatedBy",
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>(),
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               new Base.DataStructures.TaskOptions(),
                                               new Output(OutputStatus.Success,
                                                          "")),
                                })
                   .ConfigureAwait(false);

    // Cancel the session - should not throw even if agent is unreachable
    var response = sessionClient.CancelSession(new CancelSessionRequest
                                               {
                                                 SessionId = sessionId,
                                               });

    Assert.Multiple(() =>
                    {
                      Assert.That(response,
                                  Is.Not.Null);
                      Assert.That(response.Session,
                                  Is.Not.Null);
                      Assert.That(response.Session.SessionId,
                                  Is.EqualTo(sessionId));
                    });

    httpMessageHandlerMock.Protected()
                          .Verify("SendAsync",
                                  Times.Once(),
                                  ItExpr.IsAny<HttpRequestMessage>(),
                                  ItExpr.IsAny<CancellationToken>());
  }

  [Test]
  public async Task CancelExistingSessionShouldSucceed()
  {
    var helper = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddSingleton<IPartitionTable, SimplePartitionTable>()
                                                                  .AddSingleton<Injection.Options.Submitter>()
                                                                  .AddSingleton<MeterHolder>()
                                                                  .AddSingleton<AgentIdentifier>()
                                                                  .AddScoped(typeof(FunctionExecutionMetrics<>))
                                                                  .AddHttpClient()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder =>
                                          {
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

    var sessionClient = new Sessions.SessionsClient(channel);

    // Create a session
    var sessionId = sessionClient.CreateSession(new CreateSessionRequest
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

    // Cancel the session
    var response = sessionClient.CancelSession(new CancelSessionRequest
                                               {
                                                 SessionId = sessionId,
                                               });

    Assert.Multiple(() =>
                    {
                      Assert.That(response,
                                  Is.Not.Null);
                      Assert.That(response.Session,
                                  Is.Not.Null);
                      Assert.That(response.Session.SessionId,
                                  Is.EqualTo(sessionId));
                    });
  }
}
