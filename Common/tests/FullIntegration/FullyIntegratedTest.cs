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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Moq;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Api.gRPC.V1.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.FullIntegration;

[TestFixture]
public class FullyIntegratedTest
{
  private GrpcSubmitterServiceHelper helper_;

  [SetUp]
  public void Setup()
  {

  }

  [TearDown]
  public async Task TearDown()
  {
    await helper_.StopServer()
                 .ConfigureAwait(false);
    helper_              = null;
  }




  [Test]
  public async Task GetServiceConfigurationShouldSucceed()
  {
    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    using var testServiceProvider = new TestPollingAgentProvider(mockStreamHandler.Object);

    helper_ = new GrpcSubmitterServiceHelper(testServiceProvider.Submitter);
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));

    var response = client.GetServiceConfiguration(new Empty());

    Assert.AreEqual(PayloadConfiguration.MaxChunkSize,
                    response.DataChunkMaxSize);
  }

  [Test]
  public async Task FullRunShouldSucceed()
  {
    var mockStreamHandler = new WorkerStreamHandlerFullTest();

    using var testServiceProvider = new TestPollingAgentProvider(mockStreamHandler);
    helper_ = new GrpcSubmitterServiceHelper(testServiceProvider.Submitter);
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));
    const string sessionId = "MySession";
    const string taskId    = "MyTask";

    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                        MaxRetries  = 4,
                        Priority    = 2,
                      };
    client.CreateSession(new CreateSessionRequest
                         {
                           Id                = sessionId,
                           DefaultTaskOption = taskOptions,
                         });

    var taskRequest = new TaskRequest
                      {
                        Id      = taskId,
                        Payload = ByteString.CopyFromUtf8("taskPayload"),
                      };

    taskRequest.ExpectedOutputKeys.Add(taskId);

    var taskCreationReply = client.CreateSmallTasks(new CreateSmallTaskRequest
    {
      SessionId = sessionId,
      TaskOptions = taskOptions,
      TaskRequests =
                              {
                                new[]
                                {
                                  taskRequest,
                                },
                              },
    });
    Assert.AreEqual(CreateTaskReply.DataOneofCase.Successfull,
                    taskCreationReply.DataCase);

    var taskWaitForCompletion = client.WaitForCompletion(new WaitRequest
                                                         {
                                                           Filter = new TaskFilter
                                                                    {
                                                                      Task = new TaskFilter.Types.IdsRequest
                                                                             {
                                                                               Ids =
                                                                               {
                                                                                 taskId,
                                                                               },
                                                                             },
                                                                    },
                                                         });

    var taskStatus = taskWaitForCompletion.Values.Single();
    Assert.AreEqual(1,
                    taskStatus.Count);
    Assert.AreEqual(TaskStatus.Completed,
                    taskStatus.Status);
  }

  [Test]
  [Ignore("Unstable error management in Pollster")]
  public async Task TaskRetryShouldSucceed()
  {
    var mockStreamHandler = new WorkerStreamHandlerErrorRetryTest(new ArmoniKException());

    using var testServiceProvider = new TestPollingAgentProvider(mockStreamHandler);
    helper_ = new GrpcSubmitterServiceHelper(testServiceProvider.Submitter);
    var client = new Api.gRPC.V1.Submitter.SubmitterClient(await helper_.CreateChannel()
                                                                        .ConfigureAwait(false));
    const string sessionId = "MySession";
    const string taskId = "MyTask";

    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
      MaxRetries = 4,
      Priority = 2,
    };
    client.CreateSession(new CreateSessionRequest
    {
      Id = sessionId,
      DefaultTaskOption = taskOptions,
    });

    var taskRequest = new TaskRequest
    {
      Id = taskId,
      Payload = ByteString.CopyFromUtf8("taskPayload"),
    };

    taskRequest.ExpectedOutputKeys.Add(taskId);

    var taskCreationReply = client.CreateSmallTasks(new CreateSmallTaskRequest
    {
      SessionId = sessionId,
      TaskOptions = taskOptions,
      TaskRequests =
                              {
                                new[]
                                {
                                  taskRequest,
                                },
                              },
    });
    Assert.AreEqual(CreateTaskReply.DataOneofCase.Successfull,
                    taskCreationReply.DataCase);

    var taskWaitForCompletion = client.WaitForCompletion(new WaitRequest
                                                         {
                                                           Filter = new TaskFilter
                                                                    {
                                                                      Task = new TaskFilter.Types.IdsRequest
                                                                             {
                                                                               Ids =
                                                                               {
                                                                                 taskId,
                                                                               },
                                                                             },
                                                                    },
                                                         });

    var taskStatus = taskWaitForCompletion.Values.Single();
    Assert.AreEqual(1,
                    taskStatus.Count);
    Assert.AreEqual(TaskStatus.Completed,
                    taskStatus.Status);


  }
}
