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

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

internal class CreateSessionTests
{
  private Submitter.SubmitterClient? client_;
  private string?                    partition_;

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
    var channel = GrpcChannelFactory.CreateChannel(options);
    client_ = new Submitter.SubmitterClient(channel);
    Console.WriteLine("Client created");
  }

  [Test]
  public void NullDefaultTaskOptionShouldThrowException()
  {
    Console.WriteLine("NullDefaultTaskOptionShouldThrowException");

    Assert.Throws(typeof(RpcException),
                  () => client_!.CreateSession(new CreateSessionRequest
                                               {
                                                 DefaultTaskOption = null,
                                                 PartitionIds =
                                                 {
                                                   partition_,
                                                 },
                                               }));
  }

  [Test]
  public void SessionShouldBeCreated()
  {
    Console.WriteLine("SessionShouldBeCreated");

    var createSessionReply = client_!.CreateSession(new CreateSessionRequest
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
    Assert.AreNotEqual("",
                       createSessionReply.SessionId);
  }
}
