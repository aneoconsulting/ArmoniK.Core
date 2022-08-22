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
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

[TestFixture]
public class GrpcSubmitterServiceTests
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  [Test]
  public async Task TryGetResultStreamConstructionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => Task.CompletedTask);

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    await service.TryGetResultStream(new ResultRequest
                                     {
                                       ResultId = "Key",
                                       Session  = "Session",
                                     },
                                     helperServerStreamWriter,
                                     TestServerCallContext.Create())
                 .ConfigureAwait(false);

    Assert.AreEqual(0,
                    helperServerStreamWriter.Messages.Count);
  }

  [Test]
  public async Task TryGetResultStreamArmoniKExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamTaskNotFoundExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamResultNotFoundExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new ResultNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamResultDataNotFoundExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new ObjectDataNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamInvalidOperationExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         ResultId = "Key",
                                         Session  = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetTaskOutputInvalidOperationExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetTaskOutputAsync(It.IsAny<TaskOutputRequest>(),
                                                                     CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.TryGetTaskOutput(new TaskOutputRequest
                                     {
                                       TaskId  = "Key",
                                       Session = "Session",
                                     },
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetTaskOutputArmonikExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetTaskOutputAsync(It.IsAny<TaskOutputRequest>(),
                                                                     CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.TryGetTaskOutput(new TaskOutputRequest
                                     {
                                       TaskId  = "Key",
                                       Session = "Session",
                                     },
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetTaskOutputAsync(It.IsAny<TaskOutputRequest>(),
                                                                     CancellationToken.None))
                 .Returns(() => Task.FromResult(new Output
                                                {
                                                  Ok = new Empty(),
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var output = await service.TryGetTaskOutput(new TaskOutputRequest
                                                {
                                                  TaskId  = "Key",
                                                  Session = "Session",
                                                },
                                                TestServerCallContext.Create())
                              .ConfigureAwait(false);

    Assert.AreEqual(Output.TypeOneofCase.Ok,
                    output.TypeCase);
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed2()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetTaskOutputAsync(It.IsAny<TaskOutputRequest>(),
                                                                     CancellationToken.None))
                 .Returns(() => Task.FromResult(new Output
                                                {
                                                  Error = new Output.Types.Error
                                                          {
                                                            Details = "Test error",
                                                          },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var output = await service.TryGetTaskOutput(new TaskOutputRequest
                                                {
                                                  TaskId  = "Key",
                                                  Session = "Session",
                                                },
                                                TestServerCallContext.Create())
                              .ConfigureAwait(false);

    Assert.AreEqual(Output.TypeOneofCase.Error,
                    output.TypeCase);
  }

  [Test]
  public async Task GetConfigurationInvalidOperationExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetServiceConfiguration(It.IsAny<Empty>(),
                                                                       CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.GetServiceConfiguration(new Empty(),
                                            TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetConfigurationShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetServiceConfiguration(It.IsAny<Empty>(),
                                                                       CancellationToken.None))
                 .Returns(() => Task.FromResult(new Configuration
                                                {
                                                  DataChunkMaxSize = 32,
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.GetServiceConfiguration(new Empty(),
                                                         TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(32,
                    response.DataChunkMaxSize);
  }

  [Test]
  public async Task CancelSessionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelSession(It.IsAny<string>(),
                                                             CancellationToken.None))
                 .Returns(() => Task.CompletedTask);

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CancelSession(new Session
                                               {
                                                 Id = "Session",
                                               },
                                               TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreNotEqual(null,
                       response);
  }

  [Test]
  public async Task CancelSessionInvalidOperationExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelSession(It.IsAny<string>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CancelSession(new Session
                                  {
                                    Id = "Session",
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CancelSessionArmoniKExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelSession(It.IsAny<string>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CancelSession(new Session
                                  {
                                    Id = "Session",
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CancelSessionSessionNotExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelSession(It.IsAny<string>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new SessionNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CancelSession(new Session
                                  {
                                    Id = "Session",
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }


  [Test]
  public async Task CancelTasksShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelTasks(It.IsAny<TaskFilter>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.CompletedTask);

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CancelTasks(new TaskFilter
                                             {
                                               Session = new TaskFilter.Types.IdsRequest
                                                         {
                                                           Ids =
                                                           {
                                                             "Session",
                                                           },
                                                         },
                                             },
                                             TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreNotEqual(null,
                       response);
  }

  [Test]
  public async Task CancelTasksArmonikExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelTasks(It.IsAny<TaskFilter>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CancelTasks(new TaskFilter
                                {
                                  Session = new TaskFilter.Types.IdsRequest
                                            {
                                              Ids =
                                              {
                                                "Session",
                                              },
                                            },
                                },
                                TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CancelTasksInvalidExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CancelTasks(It.IsAny<TaskFilter>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new InvalidAsynchronousStateException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CancelTasks(new TaskFilter
                                {
                                  Session = new TaskFilter.Types.IdsRequest
                                            {
                                              Ids =
                                              {
                                                "Session",
                                              },
                                            },
                                },
                                TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateSessionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => Task.FromResult(new CreateSessionReply
                                                {
                                                  SessionId = string.Empty,
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CreateSession(new CreateSessionRequest
                                               {
                                                 DefaultTaskOption = new TaskOptions(),
                                               },
                                               TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreNotEqual(null,
                       response);
  }

  [Test]
  public async Task CreateSessionInvalidExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSession(new CreateSessionRequest
                                  {
                                    DefaultTaskOption = new TaskOptions(),
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateSessionArmonikExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSession(new CreateSessionRequest
                                  {
                                    PartitionIds =
                                    {
                                      "part1",
                                      "part2",
                                    },
                                    DefaultTaskOption = new TaskOptions
                                                        {
                                                          PartitionId = "part1",
                                                        },
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateSmallTasksShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult((new List<Storage.TaskRequest>().AsEnumerable(), new int())));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CreateSmallTasks(new CreateSmallTaskRequest
                                                  {
                                                    SessionId   = "SessionId",
                                                    TaskOptions = new TaskOptions(),
                                                    TaskRequests =
                                                    {
                                                      new Api.gRPC.V1.TaskRequest
                                                      {
                                                        Payload = ByteString.Empty,
                                                        DataDependencies =
                                                        {
                                                          "dep",
                                                        },
                                                        ExpectedOutputKeys =
                                                        {
                                                          "out",
                                                        },
                                                      },
                                                    },
                                                  },
                                                  TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    response.ResponseCase);
  }

  [Test]
  public async Task CreateSmallArmonikExceptionTasksShouldFail()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSmallTasks(new CreateSmallTaskRequest
                                     {
                                       SessionId   = "SessionId",
                                       TaskOptions = new TaskOptions(),
                                       TaskRequests =
                                       {
                                         new Api.gRPC.V1.TaskRequest
                                         {
                                           Payload = ByteString.Empty,
                                           DataDependencies =
                                           {
                                             "dep",
                                           },
                                           ExpectedOutputKeys =
                                           {
                                             "out",
                                           },
                                         },
                                       },
                                     },
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateSmallExceptionTasksShouldFail()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSmallTasks(new CreateSmallTaskRequest
                                     {
                                       SessionId   = "SessionId",
                                       TaskOptions = new TaskOptions(),
                                       TaskRequests =
                                       {
                                         new Api.gRPC.V1.TaskRequest
                                         {
                                           Payload = ByteString.Empty,
                                           DataDependencies =
                                           {
                                             "dep",
                                           },
                                           ExpectedOutputKeys =
                                           {
                                             "out",
                                           },
                                         },
                                       },
                                     },
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateLargeTasksShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult((new List<Storage.TaskRequest>().AsEnumerable(), new int())));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = new TaskOptions(),
                                   SessionId   = "SessionId",
                                 },
                 },
               };
    var helper = new TestHelperAsyncStreamReader<CreateLargeTaskRequest>(list);

    mockSubmitter.Verify();

    var response = await service.CreateLargeTasks(helper,
                                                  TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    response.ResponseCase);
  }

  [Test]
  public async Task CreateLargeTasksEmptyStringShouldThrowException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult((new List<Storage.TaskRequest>().AsEnumerable(), new int())));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var helper = new TestHelperAsyncStreamReader<CreateLargeTaskRequest>(new List<CreateLargeTaskRequest>());

    mockSubmitter.Verify();

    try
    {
      await service.CreateLargeTasks(helper,
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);

      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.InvalidArgument,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateLargeTasksArmonikExceptionShouldThrowException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = new TaskOptions(),
                                   SessionId   = "SessionId",
                                 },
                 },
               };
    var helper = new TestHelperAsyncStreamReader<CreateLargeTaskRequest>(list);

    mockSubmitter.Verify();

    try
    {
      await service.CreateLargeTasks(helper,
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);

      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CreateLargeTasksExceptionShouldThrowException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateTasks(It.IsAny<string>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = new TaskOptions(),
                                   SessionId   = "SessionId",
                                 },
                 },
               };
    var helper = new TestHelperAsyncStreamReader<CreateLargeTaskRequest>(list);

    mockSubmitter.Verify();

    try
    {
      await service.CreateLargeTasks(helper,
                                     TestServerCallContext.Create())
                   .ConfigureAwait(false);

      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CountTasksShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CountTasks(It.IsAny<TaskFilter>(),
                                                          CancellationToken.None))
                 .Returns(() => Task.FromResult(new Count
                                                {
                                                  Values =
                                                  {
                                                    new StatusCount
                                                    {
                                                      Count  = 5,
                                                      Status = TaskStatus.Completed,
                                                    },
                                                  },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CountTasks(new TaskFilter
                                            {
                                              Task = new TaskFilter.Types.IdsRequest
                                                     {
                                                       Ids =
                                                       {
                                                         "TaskId",
                                                       },
                                                     },
                                            },
                                            TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(5,
                    response.Values.Single()
                            .Count);
    Assert.AreEqual(TaskStatus.Completed,
                    response.Values.Single()
                            .Status);
  }

  [Test]
  public async Task CountTasksExceptionShouldThrowException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CountTasks(It.IsAny<TaskFilter>(),
                                                          CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CountTasks(new TaskFilter
                               {
                                 Task = new TaskFilter.Types.IdsRequest
                                        {
                                          Ids =
                                          {
                                            "TaskId",
                                          },
                                        },
                               },
                               TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task CountTasksArmonikExceptionShouldThrowException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CountTasks(It.IsAny<TaskFilter>(),
                                                          CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CountTasks(new TaskFilter
                               {
                                 Task = new TaskFilter.Types.IdsRequest
                                        {
                                          Ids =
                                          {
                                            "TaskId",
                                          },
                                        },
                               },
                               TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForCompletionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForCompletion(It.IsAny<WaitRequest>(),
                                                                 CancellationToken.None))
                 .Returns(() => Task.FromResult(new Count
                                                {
                                                  Values =
                                                  {
                                                    new StatusCount
                                                    {
                                                      Count  = 5,
                                                      Status = TaskStatus.Completed,
                                                    },
                                                  },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.WaitForCompletion(new WaitRequest
                                                   {
                                                     Filter = new TaskFilter
                                                              {
                                                                Task = new TaskFilter.Types.IdsRequest
                                                                       {
                                                                         Ids =
                                                                         {
                                                                           "TaskId",
                                                                         },
                                                                       },
                                                              },
                                                   },
                                                   TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(5,
                    response.Values.Single()
                            .Count);
    Assert.AreEqual(TaskStatus.Completed,
                    response.Values.Single()
                            .Status);
  }

  [Test]
  public async Task WaitForCompletionArmonikExceptionShouldFail()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForCompletion(It.IsAny<WaitRequest>(),
                                                                 CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForCompletion(new WaitRequest
                                      {
                                        Filter = new TaskFilter
                                                 {
                                                   Task = new TaskFilter.Types.IdsRequest
                                                          {
                                                            Ids =
                                                            {
                                                              "TaskId",
                                                            },
                                                          },
                                                 },
                                      },
                                      TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForCompletionExceptionShouldFail()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForCompletion(It.IsAny<WaitRequest>(),
                                                                 CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForCompletion(new WaitRequest
                                      {
                                        Filter = new TaskFilter
                                                 {
                                                   Task = new TaskFilter.Types.IdsRequest
                                                          {
                                                            Ids =
                                                            {
                                                              "TaskId",
                                                            },
                                                          },
                                                 },
                                      },
                                      TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForAvailabilityShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        CancellationToken.None))
                 .Returns(() => Task.FromResult(new AvailabilityReply
                                                {
                                                  Ok = new Empty(),
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.WaitForAvailability(new ResultRequest
                                                     {
                                                       ResultId = "Key",
                                                     },
                                                     TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(AvailabilityReply.TypeOneofCase.Ok,
                    response.TypeCase);
  }

  [Test]
  public async Task WaitForAvailabilityExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForAvailability(new ResultRequest
                                        {
                                          ResultId = "Key",
                                        },
                                        TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForAvailabilityArmonikExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForAvailability(new ResultRequest
                                        {
                                          ResultId = "Key",
                                        },
                                        TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForAvailabilityTaskNotFoundExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForAvailability(new ResultRequest
                                        {
                                          ResultId = "Key",
                                        },
                                        TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task WaitForAvailabilityResultNotFoundExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.WaitForAvailabilityAsync(It.IsAny<ResultRequest>(),
                                                                        CancellationToken.None))
                 .Returns(() => throw new ResultNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.WaitForAvailability(new ResultRequest
                                        {
                                          ResultId = "Key",
                                        },
                                        TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetStatusShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetTaskStatusAsync(It.IsAny<GetTaskStatusRequest>(),
                                                                  CancellationToken.None))
                 .Returns(() => Task.FromResult(new GetTaskStatusReply
                                                {
                                                  IdStatuses =
                                                  {
                                                    new GetTaskStatusReply.Types.IdStatus
                                                    {
                                                      Status = TaskStatus.Completed,
                                                      TaskId = "TaskId",
                                                    },
                                                  },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.GetTaskStatus(new GetTaskStatusRequest
                                               {
                                                 TaskIds =
                                                 {
                                                   "TaskId",
                                                 },
                                               },
                                               TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    response.IdStatuses.Single()
                            .Status);
  }

  [Test]
  public async Task GetStatusTaskNotFoundExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetTaskStatusAsync(It.IsAny<GetTaskStatusRequest>(),
                                                                  CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.GetTaskStatus(new GetTaskStatusRequest
                                  {
                                    TaskIds =
                                    {
                                      "TaskId",
                                    },
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetStatusArmonikExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetTaskStatusAsync(It.IsAny<GetTaskStatusRequest>(),
                                                                  CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.GetTaskStatus(new GetTaskStatusRequest
                                  {
                                    TaskIds =
                                    {
                                      "TaskId",
                                    },
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetStatusExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetTaskStatusAsync(It.IsAny<GetTaskStatusRequest>(),
                                                                  CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.GetTaskStatus(new GetTaskStatusRequest
                                  {
                                    TaskIds =
                                    {
                                      "TaskId",
                                    },
                                  },
                                  TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task ListTasksShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                              CancellationToken.None))
                 .Returns(() => Task.FromResult(new TaskIdList
                                                {
                                                  TaskIds =
                                                  {
                                                    "TaskId",
                                                  },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.ListTasks(new TaskFilter
                                           {
                                             Task = new TaskFilter.Types.IdsRequest
                                                    {
                                                      Ids =
                                                      {
                                                        "TaskId",
                                                      },
                                                    },
                                           },
                                           TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(new List<string>
                    {
                      "TaskId",
                    },
                    response.TaskIds);
  }

  [Test]
  public async Task ListTaskExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                              CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.ListTasks(new TaskFilter
                              {
                                Task = new TaskFilter.Types.IdsRequest
                                       {
                                         Ids =
                                         {
                                           "TaskId",
                                         },
                                       },
                              },
                              TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task ListTaskArmonikExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                              CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.ListTasks(new TaskFilter
                              {
                                Task = new TaskFilter.Types.IdsRequest
                                       {
                                         Ids =
                                         {
                                           "TaskId",
                                         },
                                       },
                              },
                              TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task ListTaskTaskNotFoundExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                              CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.ListTasks(new TaskFilter
                              {
                                Task = new TaskFilter.Types.IdsRequest
                                       {
                                         Ids =
                                         {
                                           "TaskId",
                                         },
                                       },
                              },
                              TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetResultStatusAsyncArmoniKNotFoundExceptionShouldThrow()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetResultStatusAsync(It.IsAny<GetResultStatusRequest>(),
                                                                    CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.GetResultStatus(new GetResultStatusRequest
                                    {
                                      SessionId = "sessionId",
                                      ResultIds =
                                      {
                                        "Result",
                                      },
                                    },
                                    TestServerCallContext.Create())
                   .ConfigureAwait(false);
      Assert.Fail();
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task GetResultStatusShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetResultStatusAsync(It.IsAny<GetResultStatusRequest>(),
                                                                    CancellationToken.None))
                 .Returns(() => Task.FromResult(new GetResultStatusReply
                                                {
                                                  IdStatuses =
                                                  {
                                                    new GetResultStatusReply.Types.IdStatus
                                                    {
                                                      Status   = ResultStatus.Completed,
                                                      ResultId = "ResultId",
                                                    },
                                                  },
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.GetResultStatus(new GetResultStatusRequest
                                                 {
                                                   ResultIds =
                                                   {
                                                     "ResultId",
                                                   },
                                                 },
                                                 TestServerCallContext.Create())
                                .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Completed,
                    response.IdStatuses.Single()
                            .Status);
  }
}
