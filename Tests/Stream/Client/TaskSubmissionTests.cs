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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

internal class TaskSubmissionTests
{
  private ChannelBase? channel_;
  private string?      partition_;

  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 "GrpcClient:Endpoint", "http://localhost:5001"
                                               },
                                               {
                                                 "Partition", "TestPartition"
                                               },
                                             };

    var builder = new ConfigurationBuilder().AddInMemoryCollection(baseConfig)
                                            .AddEnvironmentVariables();
    var configuration = builder.Build();
    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>()!;

    partition_ = configuration.GetValue<string>("Partition");

    Console.WriteLine($"endpoint : {options.Endpoint}");
    channel_ = GrpcChannelFactory.CreateChannel(options);
  }

  [Test]
  public async Task SubmitTasksShouldSucceed()
  {
    var sessionClient = new Sessions.SessionsClient(channel_);
    var createSessionReply = await sessionClient.CreateSessionAsync(new CreateSessionRequest
                                                                    {
                                                                      DefaultTaskOption = new TaskOptions
                                                                                          {
                                                                                            Priority    = 1,
                                                                                            MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                                                            MaxRetries  = 2,
                                                                                            PartitionId = partition_,
                                                                                          },
                                                                      PartitionIds =
                                                                      {
                                                                        partition_,
                                                                      },
                                                                    });

    var resultsClient = new Results.ResultsClient(channel_);

    var results = await resultsClient.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                 {
                                                                   SessionId = createSessionReply.SessionId,
                                                                   Results =
                                                                   {
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = "MyResult",
                                                                     },
                                                                   },
                                                                 });

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.Compute,
                    DataBytes = BitConverter.GetBytes(3),
                    ResultKey = results.Results.Single()
                                       .ResultId,
                  };
    var payloads = await resultsClient.CreateResultsAsync(new CreateResultsRequest
                                                          {
                                                            SessionId = createSessionReply.SessionId,
                                                            Results =
                                                            {
                                                              new CreateResultsRequest.Types.ResultCreate
                                                              {
                                                                Name = "MyPayload",
                                                                Data = UnsafeByteOperations.UnsafeWrap(payload.Serialize()),
                                                              },
                                                            },
                                                          });


    var tasksClient = new Tasks.TasksClient(channel_);

    var tasks = await tasksClient.SubmitTasksAsync(new SubmitTasksRequest
                                                   {
                                                     SessionId = createSessionReply.SessionId,
                                                     TaskCreations =
                                                     {
                                                       new SubmitTasksRequest.Types.TaskCreation
                                                       {
                                                         ExpectedOutputKeys =
                                                         {
                                                           results.Results.Select(r => r.ResultId),
                                                         },
                                                         PayloadId = payloads.Results.Single()
                                                                             .ResultId,
                                                       },
                                                     },
                                                   })
                                 .ConfigureAwait(false);

    var taskData = await tasksClient.GetTaskAsync(new GetTaskRequest
                                                  {
                                                    TaskId = tasks.TaskInfos.Single()
                                                                  .TaskId,
                                                  })
                                    .ConfigureAwait(false);

    Assert.Contains(taskData.Task.Status,
                    new List<TaskStatus>
                    {
                      TaskStatus.Completed,
                      TaskStatus.Submitted,
                      TaskStatus.Dispatched,
                      TaskStatus.Processing,
                    });

    var eventClient = new Events.EventsClient(channel_);
    await eventClient.WaitForResultsAsync(createSessionReply.SessionId,
                                          results.Results.ViewSelect(result => result.ResultId),
                                          CancellationToken.None)
                     .ConfigureAwait(false);

    taskData = await tasksClient.GetTaskAsync(new GetTaskRequest
                                              {
                                                TaskId = tasks.TaskInfos.Single()
                                                              .TaskId,
                                              })
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    taskData.Task.Status);

    var data = await resultsClient.DownloadResultData(createSessionReply.SessionId,
                                                      results.Results.Single()
                                                             .ResultId,
                                                      CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(9,
                    BitConverter.ToInt32(TestPayload.Deserialize(data)
                                                    ?.DataBytes));
  }

  [Test]
  public async Task SubmitTasksBeforePayloadShouldSucceed()
  {
    var sessionClient = new Sessions.SessionsClient(channel_);
    var createSessionReply = await sessionClient.CreateSessionAsync(new CreateSessionRequest
                                                                    {
                                                                      DefaultTaskOption = new TaskOptions
                                                                                          {
                                                                                            Priority    = 1,
                                                                                            MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                                                            MaxRetries  = 2,
                                                                                            PartitionId = partition_,
                                                                                          },
                                                                      PartitionIds =
                                                                      {
                                                                        partition_,
                                                                      },
                                                                    });

    var resultsClient = new Results.ResultsClient(channel_);

    var results = await resultsClient.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                 {
                                                                   SessionId = createSessionReply.SessionId,
                                                                   Results =
                                                                   {
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = "MyResult",
                                                                     },
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = "MyPayload",
                                                                     },
                                                                   },
                                                                 });

    var resultId = results.Results.Single(raw => raw.Name == "MyResult")
                          .ResultId;
    var payloadId = results.Results.Single(raw => raw.Name == "MyPayload")
                           .ResultId;

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.Compute,
                    DataBytes = BitConverter.GetBytes(3),
                    ResultKey = resultId,
                  };

    var tasksClient = new Tasks.TasksClient(channel_);

    var tasks = await tasksClient.SubmitTasksAsync(new SubmitTasksRequest
                                                   {
                                                     SessionId = createSessionReply.SessionId,
                                                     TaskCreations =
                                                     {
                                                       new SubmitTasksRequest.Types.TaskCreation
                                                       {
                                                         ExpectedOutputKeys =
                                                         {
                                                           results.Results.Select(r => r.ResultId),
                                                         },
                                                         PayloadId = payloadId,
                                                       },
                                                     },
                                                   })
                                 .ConfigureAwait(false);

    var taskData = await tasksClient.GetTaskAsync(new GetTaskRequest
                                                  {
                                                    TaskId = tasks.TaskInfos.Single()
                                                                  .TaskId,
                                                  })
                                    .ConfigureAwait(false);

    Assert.Contains(taskData.Task.Status,
                    new List<TaskStatus>
                    {
                      TaskStatus.Creating,
                    });

    var uploadStream = resultsClient.UploadResultData();
    await uploadStream.RequestStream.WriteAsync(new UploadResultDataRequest
                                                {
                                                  Id = new UploadResultDataRequest.Types.ResultIdentifier
                                                       {
                                                         ResultId  = payloadId,
                                                         SessionId = createSessionReply.SessionId,
                                                       },
                                                })
                      .ConfigureAwait(false);

    await uploadStream.RequestStream.WriteAsync(new UploadResultDataRequest
                                                {
                                                  DataChunk = UnsafeByteOperations.UnsafeWrap(payload.Serialize()),
                                                })
                      .ConfigureAwait(false);

    await uploadStream.RequestStream.CompleteAsync()
                      .ConfigureAwait(false);

    var uploadStreamResponse = await uploadStream.ResponseAsync.ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Completed,
                    uploadStreamResponse.Result.Status);

    var eventClient = new Events.EventsClient(channel_);
    await eventClient.WaitForResultsAsync(createSessionReply.SessionId,
                                          new[]
                                          {
                                            resultId,
                                          },
                                          CancellationToken.None)
                     .ConfigureAwait(false);

    taskData = await tasksClient.GetTaskAsync(new GetTaskRequest
                                              {
                                                TaskId = tasks.TaskInfos.Single()
                                                              .TaskId,
                                              })
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    taskData.Task.Status);

    var data = await resultsClient.DownloadResultData(createSessionReply.SessionId,
                                                      resultId,
                                                      CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(9,
                    BitConverter.ToInt32(TestPayload.Deserialize(data)
                                                    ?.DataBytes));
  }
}
