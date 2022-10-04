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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
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

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.Storage.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
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
    if (helper_ != null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }
  }

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

    public Task CancelTasks(TaskFilter        request,
                            CancellationToken cancellationToken)
      => Task.FromException(new T());

    public Task<Count> CountTasks(TaskFilter        request,
                                  CancellationToken cancellationToken)
      => Task.FromException<Count>(new T());

    public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                  TaskOptions       defaultTaskOptions,
                                                  CancellationToken cancellationToken)
      => Task.FromException<CreateSessionReply>(new T());

    public Task<(IEnumerable<TaskRequest> requests, int priority, string partitionId)> CreateTasks(string                                      sessionId,
                                                                                                   string                                      parentTaskId,
                                                                                                   TaskOptions                                 options,
                                                                                                   IAsyncEnumerable<gRPC.Services.TaskRequest> taskRequests,
                                                                                                   CancellationToken                           cancellationToken)
      => Task.FromException<(IEnumerable<TaskRequest>, int, string)>(new T());

    public Task FinalizeTaskCreation(IEnumerable<TaskRequest> requests,
                                     int                      priority,
                                     string                   partitionId,
                                     string                   sessionId,
                                     string                   parentTaskId,
                                     CancellationToken        cancellationToken)
      => Task.FromException(new T());

    public Task StartTask(string            taskId,
                          CancellationToken cancellationToken = default)
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

    public Task UpdateTaskStatusAsync(string            id,
                                      TaskStatus        status,
                                      CancellationToken cancellationToken = default)
      => Task.FromException(new T());

    public Task CompleteTaskAsync(TaskData          taskData,
                                  bool              resubmit,
                                  Output            output,
                                  CancellationToken cancellationToken = default)
      => Task.FromException(new T());

    public Task<Output> TryGetTaskOutputAsync(TaskOutputRequest request,
                                              CancellationToken contextCancellationToken)
      => Task.FromException<Output>(new T());

    public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                            CancellationToken contextCancellationToken)
      => Task.FromException<AvailabilityReply>(new T());

    public Task<GetTaskStatusReply> GetTaskStatusAsync(GetTaskStatusRequest request,
                                                       CancellationToken    contextCancellationToken)
      => Task.FromException<GetTaskStatusReply>(new T());

    public Task<GetResultStatusReply> GetResultStatusAsync(GetResultStatusRequest request,
                                                           CancellationToken      contextCancellationToken)
      => Task.FromException<GetResultStatusReply>(new T());

    public Task<TaskIdList> ListTasksAsync(TaskFilter        request,
                                           CancellationToken contextCancellationToken)
      => Task.FromException<TaskIdList>(new T());

    public Task<SessionIdList> ListSessionsAsync(SessionFilter     request,
                                                 CancellationToken contextCancellationToken)
      => Task.FromException<SessionIdList>(new T());

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

    public Task CancelTasks(TaskFilter        request,
                            CancellationToken cancellationToken)
      => throw new T();

    public Task<Count> CountTasks(TaskFilter        request,
                                  CancellationToken cancellationToken)
      => throw new T();

    public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                  TaskOptions       defaultTaskOptions,
                                                  CancellationToken cancellationToken)
      => throw new T();

    public Task<(IEnumerable<TaskRequest> requests, int priority, string partitionId)> CreateTasks(string                                      sessionId,
                                                                                                   string                                      parentTaskId,
                                                                                                   TaskOptions                                 options,
                                                                                                   IAsyncEnumerable<gRPC.Services.TaskRequest> taskRequests,
                                                                                                   CancellationToken                           cancellationToken)
      => throw new T();

    public Task FinalizeTaskCreation(IEnumerable<TaskRequest> requests,
                                     int                      priority,
                                     string                   partitionId,
                                     string                   sessionId,
                                     string                   parentTaskId,
                                     CancellationToken        cancellationToken)
      => throw new T();

    public Task StartTask(string            taskId,
                          CancellationToken cancellationToken = default)
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

    public Task UpdateTaskStatusAsync(string            id,
                                      TaskStatus        status,
                                      CancellationToken cancellationToken = default)
      => throw new T();

    public Task CompleteTaskAsync(TaskData          taskData,
                                  bool              resubmit,
                                  Output            output,
                                  CancellationToken cancellationToken = default)
      => throw new T();

    public Task<Output> TryGetTaskOutputAsync(TaskOutputRequest request,
                                              CancellationToken contextCancellationToken)
      => throw new T();

    public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                            CancellationToken contextCancellationToken)
      => throw new T();

    public Task<GetTaskStatusReply> GetTaskStatusAsync(GetTaskStatusRequest request,
                                                       CancellationToken    contextCancellationToken)
      => throw new T();

    public Task<GetResultStatusReply> GetResultStatusAsync(GetResultStatusRequest request,
                                                           CancellationToken      contextCancellationToken)
      => throw new T();

    public Task<TaskIdList> ListTasksAsync(TaskFilter        request,
                                           CancellationToken contextCancellationToken)
      => throw new T();

    public Task<SessionIdList> ListSessionsAsync(SessionFilter     request,
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  public async Task<StatusCode?> CancelTasksThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> CountTasksThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                                 DefaultTaskOption = new TaskOptions
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
                                    TaskOptions = new TaskOptions
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> TryGetTaskOutputThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> GetStatusThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> GetTaskStatusAsyncThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFound))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFoundInternal))]
  public async Task<StatusCode?> GetResultStatusAsyncThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                  nameof(TestCasesOutput))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultDataNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputResultNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputSessionNotFoundInternal))]
  [TestCaseSource(typeof(IntegrationGrpcSubmitterServiceTest),
                  nameof(TestCasesOutputTaskNotFound))]
  public async Task<StatusCode?> ListTasksThrowsException(ISubmitter submitter)
  {
    helper_ = new GrpcSubmitterServiceHelper(submitter);
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
                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.MaxValue),
                                          MaxRetries  = 1,
                                        },
                  };
    var noErrorReply = new CreateSessionReply
                       {
                         SessionId = "",
                       };
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.CreateSession(It.IsAny<IList<string>>(),
                                                             It.IsAny<TaskOptions>(),
                                                             It.IsAny<CancellationToken>()))
                 .Returns(() =>
                          {
                            if (ex is null)
                            {
                              return Task.FromResult(noErrorReply);
                            }

                            return Task.FromException<CreateSessionReply>(ex);
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
    /*
    ex = new ArmoniKException("client error");
    foreach (var _ in Enumerable.Range(0,
                                       4))
    {
      Assert.Throws<RpcException>(() => client.CreateSession(clientErrorRequest));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }
    */
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
                                                           It.IsAny<TaskOptions>(),
                                                           It.IsAny<IAsyncEnumerable<gRPC.Services.TaskRequest>>(),
                                                           It.IsAny<CancellationToken>()))
                 .Returns(async (string                                      _,
                                 string                                      _,
                                 TaskOptions                                 _,
                                 IAsyncEnumerable<gRPC.Services.TaskRequest> requests,
                                 CancellationToken                           cancellationToken) =>
                          {
                            var i = 0;
                            if (failAfter == 0 && ex is not null)
                            {
                              throw ex;
                            }

                            await foreach (var req in requests.WithCancellation(cancellationToken)
                                                              .ConfigureAwait(false))
                            {
                              i += 1;
                              if (i >= failAfter && ex is not null)
                              {
                                throw ex;
                              }
                            }

                            return (new List<TaskRequest>
                                    {
                                      new("taskId",
                                          new[]
                                          {
                                            "output",
                                          },
                                          Array.Empty<string>()),
                                    }, new int(), string.Empty);
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
                                                               },
                                                  },
                                                },
                                              },
                       };

    // Helper to call createLargeTasks
    async Task<CreateTaskReply> createLargeTasks(int               nbMessage,
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
                                                                                     MaxDuration = Duration.FromTimeSpan(TimeSpan.MaxValue),
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
                      await createLargeTasks(10)
                        .ConfigureAwait(false));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }

    // Call #5-8 with client error
    /*
    ex = new ArmoniKException("client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.ThrowsAsync<RpcException>(() => createLargeTasks(10));
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }
    */

    // Call #9 with server error
    ex        = new ApplicationException("server error");
    failAfter = 0;
    Assert.ThrowsAsync<RpcException>(() => createLargeTasks(10));
    Assert.AreEqual(HealthStatus.Healthy,
                    (await interceptor.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
    // Call #10 with server error
    ex        = new ApplicationException("server error");
    failAfter = 1;
    Assert.ThrowsAsync<RpcException>(() => createLargeTasks(10));
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
      var response = client!.TryGetResultStream(new ResultRequest
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
    /*
    ex = new ArmoniKException("client error");
    foreach (var i in Enumerable.Range(0,
                                       4))
    {
      failAfter = i;
      Assert.ThrowsAsync<RpcException>(TryGetResult);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await interceptor.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
    }
    */
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
}
