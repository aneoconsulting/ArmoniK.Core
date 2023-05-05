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
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
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

  private readonly Mock<IResultTable>  mockResultTable_  = new();
  private readonly Mock<ISessionTable> mockSessionTable_ = new();
  private readonly Mock<ITaskTable>    mockTaskTable_    = new();
  private readonly Mock<ISubmitter>    mockSubmitter_    = new();

  private readonly TaskOptions taskOptions_ = new()
                                              {
                                                ApplicationName      = "",
                                                ApplicationNamespace = "",
                                                ApplicationService   = "",
                                                ApplicationVersion   = "",
                                                EngineType           = "",
                                                MaxDuration          = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                                                MaxRetries           = 5,
                                                PartitionId          = "Partition",
                                                Priority             = 1,
                                              };

  [Test]
  public async Task TryGetResultStreamConstructionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();

    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => Task.CompletedTask);

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskOutput(It.IsAny<string>(),
                                                    CancellationToken.None))
        .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskOutput(It.IsAny<string>(),
                                                    CancellationToken.None))
        .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskOutput(It.IsAny<string>(),
                                                    CancellationToken.None))
        .Returns(() => Task.FromResult(new Output(true,
                                                  "")));

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var output = await service.TryGetTaskOutput(new TaskOutputRequest
                                                {
                                                  TaskId  = "Key",
                                                  Session = "Session",
                                                },
                                                TestServerCallContext.Create())
                              .ConfigureAwait(false);

    Assert.AreEqual(Api.gRPC.V1.Output.TypeOneofCase.Ok,
                    output.TypeCase);
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed2()
  {
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskOutput(It.IsAny<string>(),
                                                    CancellationToken.None))
        .Returns(() => Task.FromResult(new Output(false,
                                                  "Test Error")));

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var output = await service.TryGetTaskOutput(new TaskOutputRequest
                                                {
                                                  TaskId  = "Key",
                                                  Session = "Session",
                                                },
                                                TestServerCallContext.Create())
                              .ConfigureAwait(false);

    Assert.AreEqual(Api.gRPC.V1.Output.TypeOneofCase.Error,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.UpdateAllTaskStatusAsync(It.IsAny<TaskFilter>(),
                                                               TaskStatus.Cancelling,
                                                               CancellationToken.None))
        .Returns(() => Task.FromResult(1));

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.UpdateAllTaskStatusAsync(It.IsAny<TaskFilter>(),
                                                               TaskStatus.Cancelling,
                                                               CancellationToken.None))
        .Throws<ArmoniKException>();

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.UpdateAllTaskStatusAsync(It.IsAny<TaskFilter>(),
                                                               TaskStatus.Cancelling,
                                                               CancellationToken.None))
        .Returns(() => throw new InvalidAsynchronousStateException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
                                                             It.IsAny<Storage.TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => Task.FromResult(new CreateSessionReply
                                                {
                                                  SessionId = string.Empty,
                                                }));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CreateSession(new CreateSessionRequest
                                               {
                                                 DefaultTaskOption = new TaskOptions
                                                                     {
                                                                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                                                                     },
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
                                                             It.IsAny<Storage.TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSession(new CreateSessionRequest
                                  {
                                    DefaultTaskOption = new TaskOptions
                                                        {
                                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
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
  public async Task CreateSessionArmonikExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<Storage.TaskOptions>(),
                                                             CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult(new List<TaskCreationRequest>().AsICollection()));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    var response = await service.CreateSmallTasks(new CreateSmallTaskRequest
                                                  {
                                                    SessionId   = "SessionId",
                                                    TaskOptions = taskOptions_,
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSmallTasks(new CreateSmallTaskRequest
                                     {
                                       SessionId   = "SessionId",
                                       TaskOptions = taskOptions_,
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    mockSubmitter.Verify();

    try
    {
      await service.CreateSmallTasks(new CreateSmallTaskRequest
                                     {
                                       SessionId   = "SessionId",
                                       TaskOptions = taskOptions_,
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult(new List<TaskCreationRequest>().AsICollection()));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = taskOptions_,
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => Task.FromResult(new List<TaskCreationRequest>().AsICollection()));

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                                           It.IsAny<Storage.TaskOptions?>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = taskOptions_,
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
                                                           It.IsAny<Storage.TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<TaskRequest>>(),
                                                           CancellationToken.None))
                 .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

    var list = new List<CreateLargeTaskRequest>
               {
                 new()
                 {
                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = taskOptions_,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskStatus(It.IsAny<ICollection<string>>(),
                                                    CancellationToken.None))
        .Returns(() => Task.FromResult(new[]
                                       {
                                         new GetTaskStatusReply.Types.IdStatus
                                         {
                                           Status = TaskStatus.Completed,
                                           TaskId = "TaskId",
                                         },
                                       }.AsEnumerable()));

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskStatus(It.IsAny<ICollection<string>>(),
                                                    CancellationToken.None))
        .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskStatus(It.IsAny<ICollection<string>>(),
                                                    CancellationToken.None))
        .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.GetTaskStatus(It.IsAny<ICollection<string>>(),
                                                    CancellationToken.None))
        .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                     CancellationToken.None))
        .Returns(() => new List<string>
                       {
                         "TaskId",
                       }.ToAsyncEnumerable());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                     CancellationToken.None))
        .Returns(() => throw new Exception());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                     CancellationToken.None))
        .Returns(() => throw new ArmoniKException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<ITaskTable>();
    mock.Setup(taskTable => taskTable.ListTasksAsync(It.IsAny<TaskFilter>(),
                                                     CancellationToken.None))
        .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mock.Object,
                                           mockSessionTable_.Object,
                                           mockResultTable_.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<IResultTable>();
    mock.Setup(resultTable => resultTable.GetResultStatus(It.IsAny<IEnumerable<string>>(),
                                                          It.IsAny<string>(),
                                                          CancellationToken.None))
        .Returns(() => throw new TaskNotFoundException());

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mock.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
    var mock = new Mock<IResultTable>();
    mock.Setup(resultTable => resultTable.GetResultStatus(It.IsAny<IEnumerable<string>>(),
                                                          It.IsAny<string>(),
                                                          CancellationToken.None))
        .Returns(() => Task.FromResult(new[]
                                       {
                                         new GetResultStatusReply.Types.IdStatus
                                         {
                                           Status   = ResultStatus.Completed,
                                           ResultId = "ResultId",
                                         },
                                       }.AsEnumerable()));

    var service = new GrpcSubmitterService(mockSubmitter_.Object,
                                           mockTaskTable_.Object,
                                           mockSessionTable_.Object,
                                           mock.Object,
                                           NullLogger<GrpcSubmitterService>.Instance);

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
