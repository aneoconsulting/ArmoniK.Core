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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using Moq;

using NUnit.Framework;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using Type = System.Type;

// ReSharper disable AccessToModifiedClosure

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
internal class ExceptionInterceptorTests
{
  [SetUp]
  public void SetUp()
    => lifetime_ = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);

  [OneTimeSetUp]
  public void OneTimeSetUp()
  {
  }

  [TearDown]
  public async Task TearDown()
  {
    if (helper_ is not null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }

    lifetime_.StopApplication();
  }

  private GrpcSubmitterServiceHelper? helper_;
  private ApplicationLifetime         lifetime_ = null!;

  private ExceptionManager BuildExceptionManager(int maxError)
    => new(lifetime_,
           new OptionsWrapper<ConsoleLifetimeOptions>(new ConsoleLifetimeOptions()),
           new HostingEnvironment(),
           new OptionsWrapper<HostOptions>(new HostOptions()),
           NullLogger<ExceptionManager>.Instance,
           new ExceptionManager.Options(TimeSpan.Zero,
                                        maxError));

  /// <summary>
  ///   All concrete exception types deriving from <see cref="ArmoniKException" />, discovered by reflection across the
  ///   Base and Common assemblies.
  /// </summary>
  private static IEnumerable<Type> ArmoniKExceptionTypes
    => new[]
      {
        typeof(ArmoniKException).Assembly,     // ArmoniK.Core.Base
        typeof(ExceptionInterceptor).Assembly, // ArmoniK.Core.Common
      }.SelectMany(assembly => assembly.GetTypes())
       .Where(type => typeof(ArmoniKException).IsAssignableFrom(type) && !type.IsAbstract)
       .Distinct();

  /// <summary>
  ///   Exception types that are intentionally allowed to resolve to the generic <see cref="ArmoniKException" /> ->
  ///   <see cref="StatusCode.Internal" /> mapping. Any other exception reaching that fallback must be given a dedicated
  ///   mapping in <see cref="ExceptionInterceptor" /> or added here on purpose.
  /// </summary>
  private static readonly HashSet<Type> InternalFallbackWhitelist = new()
                                                                    {
                                                                      typeof(ArmoniKException),
                                                                      typeof(TaskAlreadyExistsException),
                                                                      typeof(WorkerDownException),
                                                                      typeof(QueueInsertionFailedException),
                                                                    };

  [Test]
  [TestCaseSource(nameof(ArmoniKExceptionTypes))]
  public void ArmoniKExceptionShouldNeverMapToUnknown(Type exceptionType)
  {
    using var exceptionManager = BuildExceptionManager(int.MaxValue / 2);
    var interceptor = new ExceptionInterceptor(exceptionManager,
                                               NullLogger<ExceptionInterceptor>.Instance);
    var exception = (Exception)Activator.CreateInstance(exceptionType)!;

    var rpc = Assert.CatchAsync<RpcException>(async () => await interceptor.UnaryServerHandler<object, object>(new object(),
                                                                                                               TestServerCallContext.Create(),
                                                                                                               (_,
                                                                                                                _) => throw exception)
                                                                           .ConfigureAwait(false));

    Assert.That(rpc!.StatusCode,
                Is.Not.EqualTo(StatusCode.Unknown),
                $"{exceptionType.Name} maps to Unknown; add a mapping in ExceptionInterceptor.HandleException.");

    if (rpc.StatusCode == StatusCode.Internal)
    {
      Assert.That(InternalFallbackWhitelist,
                  Does.Contain(exceptionType),
                  $"{exceptionType.Name} falls through to the generic ArmoniKException -> Internal mapping. " +
                  "Add a dedicated arm in ExceptionInterceptor.HandleException, or add it to InternalFallbackWhitelist.");
    }
  }

  [Test]
  [Obsolete]
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

    using var exceptionManager = BuildExceptionManager(1);
    var interceptor = new ExceptionInterceptor(exceptionManager,
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));

    // Call #1-4 without error
    ex = null;
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.That(client.CreateSession(request),
                  Is.EqualTo(noErrorReply));
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #5-8 with client error
    ex = new PartitionNotFoundException("Client error");
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.That(() => client.CreateSession(request),
                  Throws.InstanceOf<RpcException>());
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #9 with server error
    ex = new ApplicationException("server error");
    Assert.That(() => client.CreateSession(request),
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    // Call #10 with server error
    ex = new ApplicationException("server error");
    Assert.That(() => client.CreateSession(request),
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
    // Call #11 without error
    ex = null;
    Assert.That(client.CreateSession(request),
                Is.EqualTo(noErrorReply));
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
  }

  [Test]
  [Obsolete]
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

    using var exceptionManager = BuildExceptionManager(1);
    var interceptor = new ExceptionInterceptor(exceptionManager,
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));

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
      Assert.That(await CreateLargeTasks(10)
                    .ConfigureAwait(false),
                  Is.EqualTo(noErrorReply));
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #5-8 with client error
    ex = new RpcException(new Status(StatusCode.InvalidArgument,
                                     "Client error"),
                          "Client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.That(() => CreateLargeTasks(10),
                  Throws.InstanceOf<RpcException>());
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #9 with server error
    ex        = new ApplicationException("server error");
    failAfter = 0;
    Assert.That(() => CreateLargeTasks(10),
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    // Call #10 with server error
    ex        = new ApplicationException("server error");
    failAfter = 1;
    Assert.That(() => CreateLargeTasks(10),
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
  }

  [Test]
  [Obsolete]
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

    using var exceptionManager = BuildExceptionManager(3);
    var interceptor = new ExceptionInterceptor(exceptionManager,
                                               NullLogger<ExceptionInterceptor>.Instance);
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object,
                                             services => services.AddSingleton(interceptor));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));

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
      Assert.That(TryGetResult,
                  Throws.Nothing);
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #5-8 with client error
    ex = new TaskNotFoundException("Client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.That(TryGetResult,
                  Throws.InstanceOf<RpcException>());
      Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }

    // Call #9 with server error
    ex        = new ApplicationException("server error");
    failAfter = 0;
    Assert.That(TryGetResult,
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    // Call #10 with server error
    ex        = new ApplicationException("server error");
    failAfter = 1;
    Assert.That(TryGetResult,
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    // Call #11 with server error
    ex        = new ApplicationException("server error");
    failAfter = 2;
    Assert.That(TryGetResult,
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    // Call #12 with server error
    ex        = new ApplicationException("server error");
    failAfter = 3;
    Assert.That(TryGetResult,
                Throws.InstanceOf<RpcException>());
    Assert.That((await interceptor.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
  }
}
