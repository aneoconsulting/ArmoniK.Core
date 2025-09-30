// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;
using Moq.Language.Flow;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
[Obsolete]
internal class IntegrationGrpcSubmitterServiceTest
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
    if (helper_ is not null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }
  }

  private readonly Mock<ISubmitter> mockSubmitter_ = new();

  private GrpcSubmitterServiceHelper? helper_;

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
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    var response = client.TryGetResultStream(new ResultRequest
                                             {
                                               ResultId = "Key",
                                               Session  = "Session",
                                             });

    var result = await response.ResponseStream.ReadAllAsync()
                               .SingleAsync()
                               .ConfigureAwait(false);

    Console.WriteLine(result);

    Assert.AreEqual(ResultReply.TypeOneofCase.Result,
                    result.TypeCase);
  }

  public class AsyncThrowExceptionSubmitter<T> : ISubmitter
    where T : Exception, new()
  {
    public Task CancelSession(string            sessionId,
                              CancellationToken cancellationToken)
      => Task.FromException(new T());

    public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                  TaskOptions       defaultTaskOptions,
                                                  CancellationToken cancellationToken)
      => Task.FromException<CreateSessionReply>(new T());

    public Task<ICollection<TaskCreationRequest>> CreateTasks(string                        sessionId,
                                                              string                        parentTaskId,
                                                              TaskOptions?                  options,
                                                              IAsyncEnumerable<TaskRequest> taskRequests,
                                                              CancellationToken             cancellationToken)
      => Task.FromException<ICollection<TaskCreationRequest>>(new T());

    public Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                                     SessionData                      sessionData,
                                     string                           parentTaskId,
                                     CancellationToken                cancellationToken)
      => Task.FromException(new T());

    public Task<Configuration> GetServiceConfiguration(Empty             request,
                                                       CancellationToken cancellationToken)
      => Task.FromException<Configuration>(new T());

    public Task TryGetResult(ResultRequest                    request,
                             IServerStreamWriter<ResultReply> responseStream,
                             CancellationToken                cancellationToken)
      => Task.FromException(new T());

    public Task<Count> WaitForCompletion(WaitRequest       request,
                                         CancellationToken cancellationToken)
      => Task.FromException<Count>(new T());

    public Task CompleteTaskAsync(TaskData          taskData,
                                  SessionData       sessionData,
                                  bool              resubmit,
                                  Output            output,
                                  CancellationToken cancellationToken = default)
      => Task.FromException(new T());

    public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                            CancellationToken contextCancellationToken)
      => Task.FromException<AvailabilityReply>(new T());

    public Task SetResult(string                                 sessionId,
                          string                                 ownerTaskId,
                          string                                 key,
                          IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                          CancellationToken                      cancellationToken)
      => Task.FromException(new T());
  }

  public class ThrowExceptionSubmitter<T> : ISubmitter
    where T : Exception, new()
  {
    public Task CancelSession(string            sessionId,
                              CancellationToken cancellationToken)
      => throw new T();

    public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                  TaskOptions       defaultTaskOptions,
                                                  CancellationToken cancellationToken)
      => throw new T();

    public Task<ICollection<TaskCreationRequest>> CreateTasks(string                        sessionId,
                                                              string                        parentTaskId,
                                                              TaskOptions?                  options,
                                                              IAsyncEnumerable<TaskRequest> taskRequests,
                                                              CancellationToken             cancellationToken)
      => throw new T();

    public Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                                     SessionData                      sessionData,
                                     string                           parentTaskId,
                                     CancellationToken                cancellationToken)
      => throw new T();

    public Task<Configuration> GetServiceConfiguration(Empty             request,
                                                       CancellationToken cancellationToken)
      => throw new T();

    public Task TryGetResult(ResultRequest                    request,
                             IServerStreamWriter<ResultReply> responseStream,
                             CancellationToken                cancellationToken)
      => throw new T();

    public Task<Count> WaitForCompletion(WaitRequest       request,
                                         CancellationToken cancellationToken)
      => throw new T();

    public Task CompleteTaskAsync(TaskData          taskData,
                                  SessionData       sessionData,
                                  bool              resubmit,
                                  Output            output,
                                  CancellationToken cancellationToken = default)
      => throw new T();

    public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                            CancellationToken contextCancellationToken)
      => throw new T();

    public Task SetResult(string                                 sessionId,
                          string                                 ownerTaskId,
                          string                                 key,
                          IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                          CancellationToken                      cancellationToken)
      => throw new T();
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
  public async Task<StatusCode?> TryGetResultThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = await client.TryGetResultStream(new ResultRequest
                                          {
                                            ResultId = "Key",
                                            Session  = "Session",
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
  [Timeout(10000)]
  public async Task<StatusCode?> CancelSessionThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
      Console.WriteLine(e);
      return e.StatusCode;
    }

    return null;
  }


  [Test]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutput))]
  public async Task<StatusCode?> GetServiceConfigurationThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
                  nameof(TestCasesTaskTableInternal))]
  public async Task<StatusCode?> CancelTasksThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
                  nameof(TestCasesTaskTableInternal))]
  public async Task<StatusCode?> CountTasksThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputPartitionNotFoundInvalid))]
  public async Task<StatusCode?> CreateSessionThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = client.CreateSession(new CreateSessionRequest
                               {
                                 DefaultTaskOption = new Api.gRPC.V1.TaskOptions
                                                     {
                                                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                       MaxRetries  = 2,
                                                       Priority    = 2,
                                                       PartitionId = "Partition",
                                                     },
                                 PartitionIds =
                                 {
                                   "Partition",
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
  public async Task<StatusCode?> CreateSmallTasksThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
                                        Payload = ByteString.CopyFromUtf8("Payload"),
                                      },
                                    },
                                    TaskOptions = new Api.gRPC.V1.TaskOptions
                                                  {
                                                    MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                    MaxRetries  = 2,
                                                    Priority    = 2,
                                                    PartitionId = "Partition",
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
  public async Task<StatusCode?> CreateLargeTasksThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      var streamingCall = client.CreateLargeTasks();
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                                   {
                                                                     SessionId = "SessionId",
                                                                     TaskOptions = new Api.gRPC.V1.TaskOptions
                                                                                   {
                                                                                     MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                                                                     MaxRetries  = 2,
                                                                                     Priority    = 1,
                                                                                   },
                                                                   },
                                                   })
                         .ConfigureAwait(false);
      await streamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                   {
                                                     InitTask = new InitTaskRequest
                                                                {
                                                                  Header = new TaskRequestHeader(),
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
  public async Task<StatusCode?> WaitForCompletionThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
                  nameof(TestCasesTaskTable))]
  public async Task<StatusCode?> TryGetTaskOutputThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = client.TryGetTaskOutput(new TaskOutputRequest());
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
  [Obsolete("Method tested is obsolete")]
  public async Task<StatusCode?> WaitForAvailabilityThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
                  nameof(TestCasesTaskTable))]
  public async Task<StatusCode?> GetStatusThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = client.GetTaskStatus(new GetTaskStatusRequest());
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
                  nameof(TestCasesTaskTable))]
  public async Task<StatusCode?> GetTaskStatusAsyncThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = await client.GetTaskStatusAsync(new GetTaskStatusRequest())
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
                  nameof(TestCasesResultTable))]
  [Obsolete("Method tested is obsolete")]
  public async Task<StatusCode?> GetResultStatusAsyncThrowsException(IResultTable resultTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(resultTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                                  .ConfigureAwait(false));

    try
    {
      _ = await client.GetResultStatusAsync(new GetResultStatusRequest())
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
                  nameof(TestCasesTaskTable))]
  public async Task<StatusCode?> ListTasksThrowsException(ITaskTable taskTable)
  {
    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter_.Object,
                                             collection => collection.AddSingleton(taskTable));
    var client = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(await helper_.CreateChannel()
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
      yield return new TestCaseData(new ThrowExceptionSubmitter<ArmoniKException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<ArmoniKException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new ThrowExceptionSubmitter<Exception>()).Returns(StatusCode.Unknown);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<Exception>()).Returns(StatusCode.Unknown);
    }
  }

  public static IEnumerable TestCasesOutputTaskNotFound
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<TaskNotFoundException>()).Returns(StatusCode.NotFound);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<TaskNotFoundException>()).Returns(StatusCode.NotFound);
    }
  }

  public static IEnumerable TestCasesOutputResultNotFound
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<ResultNotFoundException>()).Returns(StatusCode.NotFound);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<ResultNotFoundException>()).Returns(StatusCode.NotFound);
    }
  }

  public static IEnumerable TestCasesOutputSessionNotFound
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<SessionNotFoundException>()).Returns(StatusCode.NotFound);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<SessionNotFoundException>()).Returns(StatusCode.NotFound);
    }
  }

  public static IEnumerable TestCasesOutputResultDataNotFound
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<ObjectDataNotFoundException>()).Returns(StatusCode.NotFound);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<ObjectDataNotFoundException>()).Returns(StatusCode.NotFound);
    }
  }


  public static IEnumerable TestCasesOutputTaskNotFoundInternal
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<TaskNotFoundException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<TaskNotFoundException>()).Returns(StatusCode.Internal);
    }
  }

  public static IEnumerable TestCasesOutputResultNotFoundInternal
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<ResultNotFoundException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<ResultNotFoundException>()).Returns(StatusCode.Internal);
    }
  }

  public static IEnumerable TestCasesOutputSessionNotFoundInternal
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<SessionNotFoundException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<SessionNotFoundException>()).Returns(StatusCode.Internal);
    }
  }

  public static IEnumerable TestCasesOutputResultDataNotFoundInternal
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<ObjectDataNotFoundException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<ObjectDataNotFoundException>()).Returns(StatusCode.Internal);
    }
  }

  public static IEnumerable TestCasesOutputPartitionNotFoundInvalid
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionSubmitter<PartitionNotFoundException>()).Returns(StatusCode.InvalidArgument);
      yield return new TestCaseData(new AsyncThrowExceptionSubmitter<PartitionNotFoundException>()).Returns(StatusCode.InvalidArgument);
    }
  }

  public class ThrowExceptionTaskTable<T> : ITaskTable
    where T : Exception, new()

  {
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => throw new T();

    public Task Init(CancellationToken cancellationToken)
      => throw new T();

    public TimeSpan PollingDelayMin { get; } = TimeSpan.Zero;
    public TimeSpan PollingDelayMax { get; } = TimeSpan.Zero;
    public ILogger  Logger          { get; } = NullLogger.Instance;

    public Task CreateTasks(IEnumerable<TaskData> tasks,
                            CancellationToken     cancellationToken = default)
      => throw new T();

    public Task<TConv> ReadTaskAsync<TConv>(string                            taskId,
                                            Expression<Func<TaskData, TConv>> selector,
                                            CancellationToken                 cancellationToken = default)
      => throw new T();

    public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                              CancellationToken                cancellationToken = default)
      => throw new T();

    public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
      => throw new T();

    public Task<int> CountAllTasksAsync(TaskStatus        status,
                                        CancellationToken cancellationToken = default)
      => throw new T();

    public Task DeleteTaskAsync(string            id,
                                CancellationToken cancellationToken = default)
      => throw new T();

    public Task DeleteTasksAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
      => throw new T();

    public Task DeleteTasksAsync(ICollection<string> taskIds,
                                 CancellationToken   cancellationToken = default)
      => throw new T();

    public Task<(IEnumerable<TData> tasks, long totalCount)> ListTasksAsync<TData>(Expression<Func<TaskData, bool>>    filter,
                                                                                   Expression<Func<TaskData, object?>> orderField,
                                                                                   Expression<Func<TaskData, TData>>   selector,
                                                                                   bool                                ascOrder,
                                                                                   int                                 page,
                                                                                   int                                 pageSize,
                                                                                   CancellationToken                   cancellationToken = default)
      => throw new T();

    public IAsyncEnumerable<TData> FindTasksAsync<TData>(Expression<Func<TaskData, bool>>  filter,
                                                         Expression<Func<TaskData, TData>> selector,
                                                         CancellationToken                 cancellationToken = default)
      => throw new T();

    public Task<TaskData?> UpdateOneTask(string                            taskId,
                                         Expression<Func<TaskData, bool>>? filter,
                                         UpdateDefinition<TaskData>        updates,
                                         bool                              before,
                                         CancellationToken                 cancellationToken = default)
      => throw new T();

    public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                      UpdateDefinition<TaskData>       updates,
                                      CancellationToken                cancellationToken = default)
      => throw new T();

    public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                               ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                               bool ascOrder,
                                                                                               int page,
                                                                                               int pageSize,
                                                                                               CancellationToken cancellationToken = default)
      => throw new T();

    public IAsyncEnumerable<TData> RemoveRemainingDataDependenciesAsync<TData>(ICollection<string>               taskIds,
                                                                               ICollection<string>               dependenciesToRemove,
                                                                               Expression<Func<TaskData, TData>> selector,
                                                                               CancellationToken                 cancellationToken = default)
      => throw new T();
  }

  public static IEnumerable TestCasesTaskTable
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionTaskTable<TaskNotFoundException>()).Returns(StatusCode.NotFound);
      yield return new TestCaseData(new ThrowExceptionTaskTable<Exception>()).Returns(StatusCode.Unknown);
    }
  }

  public static IEnumerable TestCasesTaskTableInternal
  {
    get
    {
      yield return new TestCaseData(new ThrowExceptionTaskTable<TaskNotFoundException>()).Returns(StatusCode.Internal);
      yield return new TestCaseData(new ThrowExceptionTaskTable<Exception>()).Returns(StatusCode.Unknown);
    }
  }

  private static IResultTable CreateResultTableMock(Action<ISetup<IResultTable, IAsyncEnumerable<ResultIdStatus>>> setupAction)
  {
    var mock = new Mock<IResultTable>();
    var setup = mock.Setup(table => table.GetResults(It.IsAny<Expression<Func<Result, bool>>>(),
                                                     It.IsAny<Expression<Func<Result, ResultIdStatus>>>(),
                                                     It.IsAny<CancellationToken>()));
    setupAction.Invoke(setup);
    return mock.Object;
  }

  public static IEnumerable TestCasesResultTable
  {
    get
    {
      foreach (var (exception, statusCode) in new[]
                                              {
                                                (new TaskNotFoundException(), StatusCode.Internal),
                                                (new ResultNotFoundException(), StatusCode.NotFound),
                                                (new Exception(), StatusCode.Unknown),
                                              })
      {
        yield return new TestCaseData(CreateResultTableMock(setup => setup.Throws(exception))).Returns(statusCode);
        yield return new TestCaseData(CreateResultTableMock(setup => setup.Returns(() => throw exception))).Returns(statusCode);
      }
    }
  }
}
