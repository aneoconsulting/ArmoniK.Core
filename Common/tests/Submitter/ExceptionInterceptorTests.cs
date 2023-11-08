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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;

// ReSharper disable AccessToModifiedClosure

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
internal class ExceptionInterceptorTests
{
  [SetUp]
  public void SetUp()
  {
  }

  [OneTimeSetUp]
  public void OneTimeSetUp()
  {
  }

  [TearDown]
  public async Task TearDown()
  {
    if (helper_ != null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }
  }

  private GrpcSubmitterServiceHelper? helper_;

  [Test]
  public async Task TooManyExceptionUnaryCallShouldBeUnhealthy()
  {
    Exception? ex = null;
    var request = new CreateSessionRequest
                  {
                    PartitionIds =
                    {
                      "Part",
                    },
                    DefaultTaskOption = new TaskOptions
                                        {
                                          Priority    = 1,
                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                          MaxRetries  = 1,
                                        },
                  };
    var noErrorReply = new CreateSessionReply
                       {
                         SessionId = "",
                       };
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<Base.DataStructures.TaskOptions>(),
                                                             It.IsAny<CancellationToken>()))
                 .Returns(() => ex is null
                                  ? Task.FromResult(noErrorReply)
                                  : Task.FromException<CreateSessionReply>(ex));

    var interceptor = new ExceptionInterceptor(new Injection.Options.Submitter
                                               {
                                                 MaxErrorAllowed = 1,
                                               },
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor)
                                                                 .AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>()));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);

    // Call #1-4 without error
    ex = null;
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.AreEqual(noErrorReply,
                      client.CreateSession(request));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #5-8 with client error
    ex = new PartitionNotFoundException("Client error");
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.Throws<RpcException>(() => client.CreateSession(request));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #9 with server error
    ex = new ApplicationException("server error");
    Assert.Throws<RpcException>(() => client.CreateSession(request));
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #10 with server error
    ex = new ApplicationException("server error");
    Assert.Throws<RpcException>(() => client.CreateSession(request));
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await interceptor.Check(HealthCheckTag.Liveness)
                                         .ConfigureAwait(false)).Status);
    // Call #11 without error
    ex = null;
    Assert.AreEqual(noErrorReply,
                    client.CreateSession(request));
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await interceptor.Check(HealthCheckTag.Liveness)
                                         .ConfigureAwait(false)).Status);
  }

  [Test]
  public async Task TooManyExceptionClientStreamShouldBeUnhealthy()
  {
    Exception? ex            = null;
    var        failAfter     = 0;
    var        mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<Base.DataStructures.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           It.IsAny<CancellationToken>()))
                 .Returns(async (string                          _,
                                 string                          _,
                                 Base.DataStructures.TaskOptions _,
                                 IAsyncEnumerable<TaskRequest>   requests,
                                 CancellationToken               cancellationToken) =>
                          {
                            var i = 0;
                            if (failAfter == 0 && ex is not null)
                            {
                              throw ex;
                            }

                            await foreach (var req in requests.WithCancellation(cancellationToken)
                                                              .ConfigureAwait(false))
                            {
                              _ =  req;
                              i += 1;
                              if (i >= failAfter && ex is not null)
                              {
                                throw ex;
                              }
                            }

                            if (ex is not null)
                            {
                              throw ex;
                            }

                            return new List<TaskCreationRequest>
                                   {
                                     new("taskId",
                                         "taskId",
                                         new Base.DataStructures.TaskOptions(new Dictionary<string, string>(),
                                                                             TimeSpan.FromSeconds(2),
                                                                             5,
                                                                             1,
                                                                             "Partition",
                                                                             "",
                                                                             "",
                                                                             "",
                                                                             "",
                                                                             ""),
                                         new[]
                                         {
                                           "output",
                                         },
                                         Array.Empty<string>()),
                                   };
                          });

    var interceptor = new ExceptionInterceptor(new Injection.Options.Submitter
                                               {
                                                 MaxErrorAllowed = 1,
                                               },
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor)
                                                                 .AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>()));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);

    var noErrorReply = new CreateTaskReply
                       {
                         CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                              {
                                                CreationStatuses =
                                                {
                                                  new CreateTaskReply.Types.CreationStatus
                                                  {
                                                    TaskInfo = new CreateTaskReply.Types.TaskInfo
                                                               {
                                                                 TaskId = "taskId",
                                                                 ExpectedOutputKeys =
                                                                 {
                                                                   "output",
                                                                 },
                                                                 PayloadId = "taskId",
                                                               },
                                                  },
                                                },
                                              },
                       };

    // Helper to call createLargeTasks
    async Task<CreateTaskReply> CreateLargeTasks(int               nbMessage,
                                                 CancellationToken cancellationToken = new())
    {
      var streamingCall = client.CreateLargeTasks(cancellationToken: cancellationToken);
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                                   {
                                                                     SessionId = "SessionId",
                                                                     TaskOptions = new TaskOptions
                                                                                   {
                                                                                     MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                                                                                     MaxRetries  = 1,
                                                                                     PartitionId = "Part",
                                                                                     Priority    = 1,
                                                                                   },
                                                                   },
                                                   },
                                                   cancellationToken)
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitTask = new InitTaskRequest
                                                                {
                                                                  Header = new TaskRequestHeader
                                                                           {
                                                                             ExpectedOutputKeys =
                                                                             {
                                                                               "output",
                                                                             },
                                                                           },
                                                                },
                                                   },
                                                   cancellationToken)
                         .ConfigureAwait(false);
      for (var i = 0; i < nbMessage; ++i)
      {
        await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                     {
                                                       TaskPayload = new DataChunk
                                                                     {
                                                                       Data = ByteString.CopyFromUtf8("payload"),
                                                                     },
                                                     },
                                                     cancellationToken)
                           .ConfigureAwait(false);
      }

      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     TaskPayload = new DataChunk
                                                                   {
                                                                     DataComplete = true,
                                                                   },
                                                   },
                                                   cancellationToken)
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitTask = new InitTaskRequest
                                                                {
                                                                  LastTask = true,
                                                                },
                                                   },
                                                   cancellationToken)
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.CompleteAsync()
                         .ConfigureAwait(false);
      return await streamingCall.ResponseAsync.ConfigureAwait(false);
    }

    // Call #1-4 without error
    ex = null;
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.AreEqual(noErrorReply,
                      await CreateLargeTasks(10)
                        .ConfigureAwait(false));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #5-8 with client error
    ex = new RpcException(new Status(StatusCode.InvalidArgument,
                                     "Client error"),
                          "Client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.ThrowsAsync<RpcException>(() => CreateLargeTasks(10));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #9 with server error
    ex        = new ApplicationException("server error");
    failAfter = 0;
    Assert.ThrowsAsync<RpcException>(() => CreateLargeTasks(10));
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #10 with server error
    ex        = new ApplicationException("server error");
    failAfter = 1;
    Assert.ThrowsAsync<RpcException>(() => CreateLargeTasks(10));
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await interceptor.Check(HealthCheckTag.Liveness)
                                         .ConfigureAwait(false)).Status);
  }

  [Test]
  public async Task TooManyExceptionServerStreamShouldBeUnhealthy()
  {
    Exception? ex         = null;
    var        nbMessages = 2;
    var        failAfter  = 0;

    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            It.IsAny<CancellationToken>()))
                 .Returns(async (ResultRequest                    _,
                                 IServerStreamWriter<ResultReply> replyStreamWriter,
                                 CancellationToken                cancellationToken) =>
                          {
                            foreach (var i in Enumerable.Range(0,
                                                               nbMessages))
                            {
                              if (i >= failAfter && ex is not null)
                              {
                                throw ex;
                              }

                              await replyStreamWriter.WriteAsync(new ResultReply
                                                                 {
                                                                   Result = new DataChunk
                                                                            {
                                                                              Data = ByteString.CopyFromUtf8("payload"),
                                                                            },
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);
                            }

                            if (nbMessages >= failAfter && ex is not null)
                            {
                              throw ex;
                            }

                            await replyStreamWriter.WriteAsync(new ResultReply
                                                               {
                                                                 Result = new DataChunk
                                                                          {
                                                                            DataComplete = true,
                                                                          },
                                                               },
                                                               CancellationToken.None)
                                                   .ConfigureAwait(false);
                            if (ex is not null)
                            {
                              throw ex;
                            }
                          });

    var interceptor = new ExceptionInterceptor(new Injection.Options.Submitter
                                               {
                                                 MaxErrorAllowed = 3,
                                               },
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor)
                                                                 .AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>()));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);

    async Task TryGetResult()
    {
      var response = client.TryGetResultStream(new ResultRequest
                                               {
                                                 ResultId = "Key",
                                                 Session  = "Session",
                                               });
      await foreach (var res in response.ResponseStream.ReadAllAsync()
                                        .ConfigureAwait(false))
      {
        _ = res;
      }
    }

    // Call #1-4 without error
    ex = null;
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.DoesNotThrowAsync(TryGetResult);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #5-8 with client error
    ex = new TaskNotFoundException("Client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.ThrowsAsync<RpcException>(TryGetResult);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #9 with server error
    ex        = new ApplicationException("server error");
    failAfter = 0;
    Assert.ThrowsAsync<RpcException>(TryGetResult);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #10 with server error
    ex        = new ApplicationException("server error");
    failAfter = 1;
    Assert.ThrowsAsync<RpcException>(TryGetResult);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #11 with server error
    ex        = new ApplicationException("server error");
    failAfter = 2;
    Assert.ThrowsAsync<RpcException>(TryGetResult);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #12 with server error
    ex        = new ApplicationException("server error");
    failAfter = 3;
    Assert.ThrowsAsync<RpcException>(TryGetResult);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await interceptor.Check(HealthCheckTag.Liveness)
                                         .ConfigureAwait(false)).Status);
  }
}
