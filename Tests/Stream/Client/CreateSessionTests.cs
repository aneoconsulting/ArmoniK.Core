// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using ArmoniK.Api.gRPC.V1;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

internal class CreateSessionTests
{
  private Submitter.SubmitterClient client_;

  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Grpc:Endpoint", "http://localhost:5001" },
    };

    var builder              = new ConfigurationBuilder().AddInMemoryCollection(baseConfig).AddEnvironmentVariables();
    var configuration        = builder.Build();
    var configurationSection = configuration.GetSection(Options.Grpc.SettingSection);
    var endpoint             = configurationSection.GetValue<string>("Endpoint");

    Console.WriteLine($"endpoint : {endpoint}");
    var channel = GrpcChannel.ForAddress(endpoint);
    client_ = new Submitter.SubmitterClient(channel);
    Console.WriteLine("Client created");
  }

  [Test]
  public void NullDefaultTaskOptionShouldThrowException()
  {
    var sessionId = Guid.NewGuid() + "mytestsession";

    Assert.Throws(typeof(RpcException),
                  () => client_.CreateSession(new CreateSessionRequest
                  {
                    DefaultTaskOption = null,
                    Id                = sessionId,
                  }));
  }

  [Test]
  public void EmptyIdTaskOptionShouldThrowException()
  {
    Assert.Throws(typeof(RpcException),
                  () => client_.CreateSession(new CreateSessionRequest
                  {
                    DefaultTaskOption = new TaskOptions
                    {
                      Priority    = 1,
                      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                      MaxRetries  = 2,
                    },
                    Id = "",
                  }));
  }

  [Test]
  public void SessionShouldBeCreated()
  {
    var sessionId = Guid.NewGuid() + "mytestsession";

    var createSessionReply = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = new TaskOptions
      {
        Priority    = 1,
        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
        MaxRetries  = 2,
      },
      Id = sessionId,
    });
    Assert.AreEqual(createSessionReply.ResultCase,
                    CreateSessionReply.ResultOneofCase.Ok);
  }
}