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
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Enum = System.Enum;
using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
internal class IntegrationGrpcSubmitterServiceTest
{
  private GrpcSubmitterServiceHelper helper_;

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
    await helper_.StopServer()
                 .ConfigureAwait(false);
    helper_.Dispose();
  }

  [Test]
  public async Task GetServiceConfigurationShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetServiceConfiguration(It.IsAny<Empty>(),
                                                                       It.IsAny<CancellationToken>()))
                 .Returns(() => Task.FromResult(new Configuration
                                                {
                                                  DataChunkMaxSize = 42,
                                                }));

    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object);
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    var response = client.GetServiceConfiguration(new Empty());

    Assert.AreEqual(42,
                    response.DataChunkMaxSize);
  }

  [Test]
  public async Task TryGetResultShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            It.IsAny<CancellationToken>()))
                 .Returns(async (ResultRequest                    _,
                                 IServerStreamWriter<ResultReply> replyStreamWriter,
                                 CancellationToken                _) =>
                          {
                            await replyStreamWriter.WriteAsync(new ResultReply
                                                               {
                                                                 Result = new DataChunk
                                                                          {
                                                                            DataComplete = true,
                                                                          },
                                                               },
                                                               CancellationToken.None)
                                                   .ConfigureAwait(false);
                            return Task.CompletedTask;
                          });

    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object);
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    var response = client.TryGetResultStream(new ResultRequest
                                             {
                                               Key     = "Key",
                                               Session = "Session",
                                             });

    var result = await response.ResponseStream.ReadAllAsync()
                               .SingleAsync()
                               .ConfigureAwait(false);

    Console.WriteLine(result);

    Assert.AreEqual(ResultReply.TypeOneofCase.Result,
                    result.TypeCase);
  }

  public enum SubmitterMockOutput
  {
    ThrowsAsync,
    Throws,
    Returns,
  }

  public static ISubmitter CreateSubmitterThrowsException(Exception           exception,
                                                          SubmitterMockOutput output)
  {
    var mockSubmitter = new Mock<ISubmitter>();
    var expressions = new List<Expression<Func<ISubmitter, Task>>>
                      {
                        submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            It.IsAny<CancellationToken>()),
                        submitter => submitter.CancelSession(It.IsAny<string>(),
                                                             It.IsAny<CancellationToken>()),
                        submitter => submitter.CancelTasks(It.IsAny<TaskFilter>(),
                                                           It.IsAny<CancellationToken>()),
                        submitter => submitter.FinalizeTaskCreation(It.IsAny<IEnumerable<Storage.TaskRequest>>(),
                                                                    It.IsAny<int>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CancellationToken>()),
                        submitter => submitter.StartTask(It.IsAny<string>(),
                                                         It.IsAny<CancellationToken>()),
                        submitter => submitter.UpdateTaskStatusAsync(It.IsAny<string>(),
                                                                     It.IsAny<TaskStatus>(),
                                                                     It.IsAny<CancellationToken>()),
                        submitter => submitter.CompleteTaskAsync(It.IsAny<string>(),
                                                                 It.IsAny<Output>(),
                                                                 It.IsAny<CancellationToken>()),
                      };

    foreach (var expression in expressions)
    {
      switch (output)
      {
        case SubmitterMockOutput.ThrowsAsync:
          mockSubmitter.Setup(expression)
                       .ThrowsAsync(exception);
          break;
        case SubmitterMockOutput.Throws:
          mockSubmitter.Setup(expression)
                       .Throws(exception);
          break;
        case SubmitterMockOutput.Returns:
          mockSubmitter.Setup(expression)
                       .Returns(() => throw exception);
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(output),
                                                output,
                                                null);
      }

    }

    return mockSubmitter.Object;
  }

  public static ISubmitter CreateSubmitterThrowsExceptionOnly(Exception exception)
  {
    var mockSubmitter = new Mock<ISubmitter>();
    var expressions = new List<Expression<Func<ISubmitter, Task>>>
                      {
                        submitter => submitter.GetServiceConfiguration(It.IsAny<Empty>(),
                                                                       It.IsAny<CancellationToken>()),

                        submitter => submitter.CountTasks(It.IsAny<TaskFilter>(),
                                                          It.IsAny<CancellationToken>()),
                        submitter => submitter.CreateSession(It.IsAny<string>(),
                                                             It.IsAny<TaskOptions>(),
                                                             It.IsAny<CancellationToken>()),
                        submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           It.IsAny<CancellationToken>()),

                        submitter => submitter.ResubmitTask(It.IsAny<TaskData>(),
                                                            It.IsAny<CancellationToken>()),
                        submitter => submitter.WaitForCompletion(It.IsAny<WaitRequest>(),
                                                                 It.IsAny<CancellationToken>()),
                        submitter => submitter.TryGetTaskOutputAsync(It.IsAny<ResultRequest>(),
                                                                     It.IsAny<CancellationToken>()),
                        submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        It.IsAny<CancellationToken>()),
                        submitter => submitter.GetStatusAsync(It.IsAny<GetStatusrequest>(),
                                                              It.IsAny<CancellationToken>()),
                        submitter => submitter.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                              It.IsAny<CancellationToken>()),
                      };

    foreach (var expression in expressions)
    {
      mockSubmitter.Setup(expression)
                   .Throws(exception);
    }

    return mockSubmitter.Object;
  }


  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  public async Task<StatusCode?> TryGetResultThrowsException(Exception           exception,
                                                             SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsException(exception,
                                                                            output));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = await client.TryGetResultStream(new ResultRequest
                                          {
                                            Key     = "Key",
                                            Session = "Session",
                                          })
                      .ResponseStream.ReadAllAsync()
                      .SingleAsync()
                      .ConfigureAwait(false);
      Assert.Fail("TryGetResult should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  public async Task<StatusCode?> CancelSessionThrowsException(Exception           exception,
                                                              SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsException(exception,
                                                                            output));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.CancelSession(new Session
                               {
                                 Id = "Session",
                               });
      Assert.Fail("TryGetResult should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }


  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  public async Task<StatusCode?> GetServiceConfigurationThrowsException(Exception           exception,
                                                                        SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.GetServiceConfiguration(new Empty());
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  public async Task<StatusCode?> CancelTasksThrowsException(Exception           exception,
                                                            SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsException(exception,
                                                                            output));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.CancelTasks(new TaskFilter
                             {
                               Task = new TaskFilter.Types.IdsRequest
                                      {
                                        Ids =
                                        {
                                          "Task",
                                        },
                                      },
                             });
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> CountTasksThrowsException(Exception           exception,
                                                           SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.CountTasks(new TaskFilter
                            {
                              Task = new TaskFilter.Types.IdsRequest
                                     {
                                       Ids =
                                       {
                                         "Task",
                                       },
                                     },
                            });
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> CreateSessionThrowsException(Exception           exception,
                                                              SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.CreateSession(new CreateSessionRequest
                               {
                                 Id = "Id",
                                 DefaultTaskOption = new TaskOptions
                                                     {
                                                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                       MaxRetries  = 2,
                                                       Priority    = 2,
                                                     },
                               });
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> CreateSmallTasksThrowsException(Exception           exception,
                                                                 SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.CreateSmallTasks(new CreateSmallTaskRequest
                                  {
                                    SessionId = "Session",
                                    TaskRequests =
                                    {
                                      new Api.gRPC.V1.TaskRequest
                                      {
                                        Id      = "Id",
                                        Payload = ByteString.CopyFromUtf8("Payload"),
                                      },
                                    },
                                    TaskOptions = new TaskOptions
                                                  {
                                                    MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                    MaxRetries  = 2,
                                                    Priority    = 2,
                                                  },
                                  });
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> CreateLargeTasksThrowsException(Exception           exception,
                                                                 SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      var streamingCall = client.CreateLargeTasks();
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                                   {
                                                                     SessionId = "SessionId",
                                                                   },
                                                   })
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitTask = new InitTaskRequest
                                                                {
                                                                  Header = new TaskRequestHeader
                                                                           {
                                                                             Id = "Id",
                                                                           },
                                                                },
                                                   })
                         .ConfigureAwait(false);
      await streamingCall.ResponseAsync.WaitAsync(CancellationToken.None)
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.CompleteAsync()
                         .ConfigureAwait(false);
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  public async Task<StatusCode?> WaitForCompletionThrowsException(Exception           exception,
                                                                  SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      var _ = client.WaitForCompletion(new WaitRequest());

      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> TryGetTaskOutputThrowsException(Exception           exception,
                                                                 SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.TryGetTaskOutput(new ResultRequest());
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> WaitForAvailabilityThrowsException(Exception           exception,
                                                                    SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.WaitForAvailability(new ResultRequest());
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> GetStatusThrowsException(Exception           exception,
                                                          SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.GetStatus(new GetStatusrequest());
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> GetStatusAsyncThrowsException(Exception           exception,
                                                          SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = await client.GetStatusAsync(new GetStatusrequest())
                      .ConfigureAwait(false);
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> ListTasksThrowsException(Exception           exception,
                                                          SubmitterMockOutput output)
  {
    helper_ = new GrpcSubmitterServiceHelper(CreateSubmitterThrowsExceptionOnly(exception));
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    try
    {
      _ = client.ListTasks(new TaskFilter
                           {
                             Task = new TaskFilter.Types.IdsRequest
                                    {
                                      Ids =
                                      {
                                        "Task",
                                      },
                                    },
                           });
      Assert.Fail("Function should throw an exception");
    }
    catch (RpcException e)
    {
      return e.StatusCode;
    }

    return null;
  }

  public static IEnumerable TestCasesOutput
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new ArmoniKException(),
                                      output).Returns(StatusCode.Internal);
        yield return new TestCaseData(new Exception(),
                                      output).Returns(StatusCode.Unknown);
      }
    }
  }

  public static IEnumerable TestCasesOutputTaskNotFound
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new TaskNotFoundException(),
                                      output).Returns(StatusCode.NotFound);
      }
    }
  }

  public static IEnumerable TestCasesOutputResultNotFound
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new ResultNotFoundException(),
                                      output).Returns(StatusCode.NotFound);
      }
    }
  }

  public static IEnumerable TestCasesOutputSessionNotFound
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new SessionNotFoundException(),
                                      output).Returns(StatusCode.NotFound);
      }
    }
  }

  public static IEnumerable TestCasesOutputResultDataNotFound
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new ObjectDataNotFoundException(),
                                      output).Returns(StatusCode.NotFound);
      }
    }
  }


  public static IEnumerable TestCasesOutputTaskNotFoundInternal
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new TaskNotFoundException(),
                                      output).Returns(StatusCode.Internal);
      }
    }
  }

  public static IEnumerable TestCasesOutputResultNotFoundInternal
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new ResultNotFoundException(),
                                      output).Returns(StatusCode.Internal);
      }
    }
  }

  public static IEnumerable TestCasesOutputSessionNotFoundInternal
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new SessionNotFoundException(),
                                      output).Returns(StatusCode.Internal);
      }
    }
  }

  public static IEnumerable TestCasesOutputResultDataNotFoundInternal
  {
    get
    {
      foreach (var output in Enum.GetValues(typeof(SubmitterMockOutput)))
      {
        yield return new TestCaseData(new ObjectDataNotFoundException(),
                                      output).Returns(StatusCode.Internal);
      }
    }
  }

}
