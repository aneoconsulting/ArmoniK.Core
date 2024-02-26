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
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1.HealthChecks;
using ArmoniK.Core.Utils;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

internal class HealthChecksTests
{
  private ChannelBase? channel_;

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
    var options       = configuration.GetRequiredValue<GrpcClient>(GrpcClient.SettingSection);

    Console.WriteLine($"endpoint : {options.Endpoint}");
    channel_ = GrpcChannelFactory.CreateChannel(options);
  }


  [Test]
  public async Task HealthCheckShouldSucceed()
  {
    var client = new HealthChecksService.HealthChecksServiceClient(channel_);
    var response = await client.CheckHealthAsync(new CheckHealthRequest())
                               .ConfigureAwait(false);

    Assert.IsNotNull(response);
    foreach (var health in response.Services)
    {
      Assert.That(health.Healthy,
                  Is.EqualTo(HealthStatusEnum.Healthy));
    }
  }
}
