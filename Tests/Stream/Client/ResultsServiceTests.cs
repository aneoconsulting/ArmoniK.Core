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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Submitter;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

using CreateSessionRequest = ArmoniK.Api.gRPC.V1.Submitter.CreateSessionRequest;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

internal class ResultsServiceTests
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
                               .Get<GrpcClient>();

    partition_ = configuration.GetValue<string>("Partition");

    Console.WriteLine($"endpoint : {options.Endpoint}");
    channel_ = GrpcChannelFactory.CreateChannel(options);
  }

  [Test]
  public async Task CreateResultsAndMetadataShouldSucceed()
  {
    var submitterClient = new Submitter.SubmitterClient(channel_);
    var createSessionReply = await submitterClient.CreateSessionAsync(new CreateSessionRequest
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

    var payloadBytes = Encoding.ASCII.GetBytes("Payload");

    await resultsClient.UploadResultData(createSessionReply.SessionId,
                                         results.Results.Single()
                                                .ResultId,
                                         payloadBytes)
                       .ConfigureAwait(false);

    var downloadedPayload = await resultsClient.DownloadResultData(createSessionReply.SessionId,
                                                                   results.Results.Single()
                                                                          .ResultId,
                                                                   CancellationToken.None)
                                               .ConfigureAwait(false);

    Assert.AreEqual(payloadBytes,
                    downloadedPayload);
  }

  [Test]
  public async Task CreateResultsDirectlyShouldSucceed()
  {
    var submitterClient = new Submitter.SubmitterClient(channel_);
    var createSessionReply = await submitterClient.CreateSessionAsync(new CreateSessionRequest
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
    var payloadBytes  = Encoding.ASCII.GetBytes("Payload");

    var results = await resultsClient.CreateResultsAsync(new CreateResultsRequest
                                                         {
                                                           SessionId = createSessionReply.SessionId,
                                                           Results =
                                                           {
                                                             new CreateResultsRequest.Types.ResultCreate
                                                             {
                                                               Name = "MyResult",
                                                               Data = UnsafeByteOperations.UnsafeWrap(payloadBytes),
                                                             },
                                                           },
                                                         });

    var downloadedPayload = await resultsClient.DownloadResultData(createSessionReply.SessionId,
                                                                   results.Results.Single()
                                                                          .ResultId,
                                                                   CancellationToken.None)
                                               .ConfigureAwait(false);

    Assert.AreEqual(payloadBytes,
                    downloadedPayload);

    await resultsClient.DeleteResultsDataAsync(new DeleteResultsDataRequest
                                               {
                                                 SessionId = createSessionReply.SessionId,
                                                 ResultId =
                                                 {
                                                   results.Results.Select(r => r.ResultId),
                                                 },
                                               })
                       .ConfigureAwait(false);
  }

  [Test]
  public async Task DownloadNotExistingResultShouldSucceed()
  {
    var sessionClient = new Sessions.SessionsClient(channel_);
    var createSessionReply = await sessionClient.CreateSessionAsync(new Api.gRPC.V1.Sessions.CreateSessionRequest
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

    RpcException? ex = null;

    Assert.ThrowsAsync<RpcException>(async () =>
                                     {
                                       try
                                       {
                                         await resultsClient.DownloadResultData(createSessionReply.SessionId,
                                                                                "NotExistingResult",
                                                                                CancellationToken.None)
                                                            .ConfigureAwait(false);
                                       }
                                       catch (RpcException e)
                                       {
                                         ex = e;
                                         throw;
                                       }
                                     });
    Assert.NotNull(ex);
    Assert.AreEqual(StatusCode.NotFound,
                    ex!.StatusCode);
  }
}
