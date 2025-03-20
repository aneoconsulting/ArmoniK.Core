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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NUnit.Framework;

using ResultStatus = ArmoniK.Api.gRPC.V1.ResultStatus;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(GrpcResultsService))]
public class GrpcResultsServiceTests
{
  [SetUp]
  public void Setup()
  {
    helper_ = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
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
                                         builder.MapGrpcService<GrpcTasksService>();
                                         builder.MapGrpcService<GrpcSessionsService>();
                                         builder.MapGrpcService<GrpcResultsService>();
                                       },
                                       true);

    helper_!.App.Start();

    var server = helper_!.App.GetTestServer();

    channel_ = GrpcChannel.ForAddress("http://localhost:9999",
                                      new GrpcChannelOptions
                                      {
                                        HttpHandler = server.CreateHandler(),
                                      });

    session_ = new Sessions.SessionsClient(channel_).CreateSession(new CreateSessionRequest
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
                                                                   });
  }

  [TearDown]
  public void TearDown()
  {
    helper_?.App.DisposeAsync()
           .WaitSync();
    helper_?.Dispose();
  }

  private TestDatabaseProvider? helper_;
  private GrpcChannel?          channel_;
  private CreateSessionReply?   session_;


  [Test]
  public void ImportingNonExistingDataShouldFail()
  {
    var resultClient = new Results.ResultsClient(channel_);

    var resultId = resultClient.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                      {
                                                        SessionId = session_!.SessionId,
                                                        Results =
                                                        {
                                                          new CreateResultsMetaDataRequest.Types.ResultCreate
                                                          {
                                                            Name = "Result for import",
                                                          },
                                                        },
                                                      })
                               .Results.Single()
                               .ResultId;

    var ex = Assert.Throws<RpcException>(() =>
                                         {
                                           resultClient.ImportResultsData(new ImportResultsDataRequest
                                                                          {
                                                                            SessionId = session_!.SessionId,
                                                                            Results =
                                                                            {
                                                                              new ImportResultsDataRequest.Types.ResultOpaqueId
                                                                              {
                                                                                OpaqueId = ByteString.CopyFromUtf8("This does not exist in the object data"),
                                                                                ResultId = resultId,
                                                                              },
                                                                            },
                                                                          });
                                         });

    // Ensure the exception has the expected status code
    Assert.That(ex!.StatusCode,
                Is.EqualTo(StatusCode.NotFound));
  }

  [Test]
  public async Task ImportingNonExistingResultShouldFail()
  {
    var resultClient = new Results.ResultsClient(channel_);

    var objectStorage = helper_!.GetRequiredService<IObjectStorage>();

    var data = new List<ReadOnlyMemory<byte>>
               {
                 new(Encoding.ASCII.GetBytes("data for result")),
               };

    var (id, _) = await objectStorage.AddOrUpdateAsync(new ObjectData
                                                       {
                                                         ResultId  = "",
                                                         SessionId = "",
                                                       },
                                                       data.ToAsyncEnumerable())
                                     .ConfigureAwait(false);


    var ex = Assert.Throws<RpcException>(() =>
                                         {
                                           resultClient.ImportResultsData(new ImportResultsDataRequest
                                                                          {
                                                                            SessionId = session_!.SessionId,
                                                                            Results =
                                                                            {
                                                                              new ImportResultsDataRequest.Types.ResultOpaqueId
                                                                              {
                                                                                OpaqueId = ByteString.CopyFrom(id),
                                                                                ResultId = "not existing result",
                                                                              },
                                                                            },
                                                                          });
                                         });

    // Ensure the exception has the expected status code
    Assert.That(ex!.StatusCode,
                Is.EqualTo(StatusCode.NotFound));
  }

  [Test]
  public async Task ImportingCompletedResultShouldFail()
  {
    var resultClient = new Results.ResultsClient(channel_);

    var resultId = resultClient.CreateResults(new CreateResultsRequest
                                              {
                                                SessionId = session_!.SessionId,
                                                Results =
                                                {
                                                  new CreateResultsRequest.Types.ResultCreate
                                                  {
                                                    Name = "Result for import",
                                                    Data = ByteString.CopyFromUtf8("Completed result for import"),
                                                  },
                                                },
                                              })
                               .Results.Single()
                               .ResultId;

    var objectStorage = helper_!.GetRequiredService<IObjectStorage>();

    var data = new List<ReadOnlyMemory<byte>>
               {
                 new(Encoding.ASCII.GetBytes("data for result")),
               };

    var (id, _) = await objectStorage.AddOrUpdateAsync(new ObjectData
                                                       {
                                                         ResultId  = "",
                                                         SessionId = "",
                                                       },
                                                       data.ToAsyncEnumerable())
                                     .ConfigureAwait(false);

    var ex = Assert.Throws<RpcException>(() =>
                                         {
                                           resultClient.ImportResultsData(new ImportResultsDataRequest
                                                                          {
                                                                            SessionId = session_!.SessionId,
                                                                            Results =
                                                                            {
                                                                              new ImportResultsDataRequest.Types.ResultOpaqueId
                                                                              {
                                                                                OpaqueId = ByteString.CopyFrom(id),
                                                                                ResultId = resultId,
                                                                              },
                                                                            },
                                                                          });
                                         });

    // Ensure the exception has the expected status code
    Assert.That(ex!.StatusCode,
                Is.EqualTo(StatusCode.FailedPrecondition));
  }

  [Test]
  public async Task ImportDataShouldSucceed()
  {
    var resultClient = new Results.ResultsClient(channel_);

    var resultId = resultClient.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                      {
                                                        SessionId = session_!.SessionId,
                                                        Results =
                                                        {
                                                          new CreateResultsMetaDataRequest.Types.ResultCreate
                                                          {
                                                            Name = "Result for import",
                                                          },
                                                        },
                                                      })
                               .Results.Single()
                               .ResultId;

    var objectStorage = helper_!.GetRequiredService<IObjectStorage>();

    var data = new List<ReadOnlyMemory<byte>>
               {
                 new(Encoding.ASCII.GetBytes("data for result")),
                 new(Encoding.ASCII.GetBytes("data for result")),
               };

    var (id, size) = await objectStorage.AddOrUpdateAsync(new ObjectData
                                                          {
                                                            ResultId  = "",
                                                            SessionId = "",
                                                          },
                                                          data.ToAsyncEnumerable())
                                        .ConfigureAwait(false);

    var resultData = resultClient.ImportResultsData(new ImportResultsDataRequest
                                                    {
                                                      SessionId = session_!.SessionId,
                                                      Results =
                                                      {
                                                        new ImportResultsDataRequest.Types.ResultOpaqueId
                                                        {
                                                          OpaqueId = ByteString.CopyFrom(id),
                                                          ResultId = resultId,
                                                        },
                                                      },
                                                    })
                                 .Results.Single();

    Assert.That(resultData.Status,
                Is.EqualTo(ResultStatus.Completed));
    Assert.That(resultData.ResultId,
                Is.EqualTo(resultId));
    Assert.That(resultData.Size,
                Is.EqualTo(size));
    Assert.That(resultData.OpaqueId.ToByteArray(),
                Is.EqualTo(id));

    resultData = resultClient.GetResult(new GetResultRequest
                                        {
                                          ResultId = resultId,
                                        })
                             .Result;

    Assert.That(resultData.Status,
                Is.EqualTo(ResultStatus.Completed));
    Assert.That(resultData.ResultId,
                Is.EqualTo(resultId));
    Assert.That(resultData.Size,
                Is.EqualTo(size));
    Assert.That(resultData.OpaqueId.ToByteArray(),
                Is.EqualTo(id));

    var stream = resultClient.DownloadResultData(new DownloadResultDataRequest
                                                 {
                                                   ResultId  = resultId,
                                                   SessionId = session_!.SessionId,
                                                 });

    var result = new List<byte>();

    while (await stream.ResponseStream.MoveNext()
                       .ConfigureAwait(false))
    {
      result.AddRange(stream.ResponseStream.Current.DataChunk.ToByteArray());
    }

    Assert.That(result.ToArray(),
                Is.EqualTo(data.SelectMany(x => x.ToArray())
                               .ToArray()));
  }

  [Test]
  public async Task ImportDataShouldTriggerTaskSubmission()
  {
    var resultClient = new Results.ResultsClient(channel_);

    var resultId = resultClient.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                      {
                                                        SessionId = session_!.SessionId,
                                                        Results =
                                                        {
                                                          new CreateResultsMetaDataRequest.Types.ResultCreate
                                                          {
                                                            Name = "Result for import",
                                                          },
                                                        },
                                                      })
                               .Results.Single()
                               .ResultId;

    var payloadId = resultClient.CreateResults(new CreateResultsRequest
                                               {
                                                 SessionId = session_!.SessionId,
                                                 Results =
                                                 {
                                                   new CreateResultsRequest.Types.ResultCreate
                                                   {
                                                     Name = "payload",
                                                     Data = ByteString.CopyFromUtf8("Payload data"),
                                                   },
                                                 },
                                               })
                                .Results.Single()
                                .ResultId;

    var taskClient = new Tasks.TasksClient(channel_);

    var taskId = taskClient.SubmitTasks(new SubmitTasksRequest
                                        {
                                          SessionId = session_.SessionId,
                                          TaskCreations =
                                          {
                                            new SubmitTasksRequest.Types.TaskCreation
                                            {
                                              PayloadId = payloadId,
                                              DataDependencies =
                                              {
                                                resultId,
                                              },
                                            },
                                          },
                                        })
                           .TaskInfos.Single()
                           .TaskId;

    var queueStorage = (SimplePushQueueStorage)helper_!.GetRequiredService<IPushQueueStorage>();

    Assert.That(queueStorage.Messages,
                Is.Empty);

    var objectStorage = helper_!.GetRequiredService<IObjectStorage>();

    var data = new List<ReadOnlyMemory<byte>>
               {
                 new(Encoding.ASCII.GetBytes("data for result")),
                 new(Encoding.ASCII.GetBytes("data for result")),
               };

    var (id, size) = await objectStorage.AddOrUpdateAsync(new ObjectData
                                                          {
                                                            ResultId  = "",
                                                            SessionId = "",
                                                          },
                                                          data.ToAsyncEnumerable())
                                        .ConfigureAwait(false);

    var resultData = resultClient.ImportResultsData(new ImportResultsDataRequest
                                                    {
                                                      SessionId = session_!.SessionId,
                                                      Results =
                                                      {
                                                        new ImportResultsDataRequest.Types.ResultOpaqueId
                                                        {
                                                          OpaqueId = ByteString.CopyFrom(id),
                                                          ResultId = resultId,
                                                        },
                                                      },
                                                    })
                                 .Results.Single();

    Assert.That(resultData.Status,
                Is.EqualTo(ResultStatus.Completed));
    Assert.That(resultData.ResultId,
                Is.EqualTo(resultId));
    Assert.That(resultData.Size,
                Is.EqualTo(size));
    Assert.That(resultData.OpaqueId.ToByteArray(),
                Is.EqualTo(id));

    Assert.That(queueStorage.Messages,
                Contains.Item(taskId));
  }

  [Test]
  public async Task PurgeDataShouldSucceed()
  {
    var objectStorage = helper_!.GetRequiredService<IObjectStorage>();

    var resultClient = new Results.ResultsClient(channel_);

    var opaqueId = resultClient.CreateResults(new CreateResultsRequest
                                              {
                                                SessionId = session_!.SessionId,
                                                Results =
                                                {
                                                  new CreateResultsRequest.Types.ResultCreate
                                                  {
                                                    Name = "Result for purge",
                                                    Data = ByteString.CopyFromUtf8("Completed result for purge"),
                                                  },
                                                },
                                              })
                               .Results.Single()
                               .OpaqueId.ToByteArray();

    var sizes = await objectStorage.GetSizesAsync(new[]
                                                  {
                                                    opaqueId,
                                                  })
                                   .ConfigureAwait(false);

    Assert.That(sizes[opaqueId],
                Is.GreaterThan(0));

    var sessionClient = new Sessions.SessionsClient(channel_);


    sessionClient.CloseSession(new CloseSessionRequest
                               {
                                 SessionId = session_.SessionId,
                               });

    sessionClient.PurgeSession(new PurgeSessionRequest
                               {
                                 SessionId = session_.SessionId,
                               });

    sizes = await objectStorage.GetSizesAsync(new[]
                                              {
                                                opaqueId,
                                              })
                               .ConfigureAwait(false);

    Assert.That(sizes[opaqueId],
                Is.Null);
  }
}
