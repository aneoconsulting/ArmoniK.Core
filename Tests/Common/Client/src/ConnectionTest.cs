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

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Client;

[TestFixture]
internal class ConnectionTest
{
  [TearDown]
  public void TearDown()
  {
    channel_?.ShutdownAsync()
            .GetAwaiter()
            .GetResult();
    channel_ = null;
  }

  private ChannelBase? channel_;

  [TestCase("http://localhost:5001",
            "",
            "",
            TestName = "Direct")]
  [TestCase("http://localhost:5201",
            "",
            "",
            TestName = "Ingress_Insecure")]
  [TestCase("https://localhost:5202",
            "",
            "",
            TestName = "Ingress_TLS_NoValidation")]
  [TestCase("https://localhost:5202",
            "",
            "",
            false,
            TestName = "Ingress_TLS")]
  [TestCase("https://localhost:5203",
            "",
            "",
            TestName = "Ingress_MTLS_NoValidation")]
  [TestCase("https://localhost:5203",
            "",
            "",
            false,
            TestName = "Ingress_MTLS")]
  public void ConnectionShouldSucceed(string endpoint,
                                      string caFile,
                                      string clientCertP12,
                                      bool   allowInsecure = true)
  {
    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 "GrpcClient:Endpoint", endpoint
                                               },
                                               {
                                                 "GrpcClient:CertP12", clientCertP12
                                               },
                                               {
                                                 "GrpcClient:CaCert", caFile
                                               },
                                               {
                                                 "GrpcClient:AllowUnsafeConnection", allowInsecure.ToString()
                                               },
                                             };

    var builder = new ConfigurationBuilder().AddInMemoryCollection(baseConfig)
                                            .AddEnvironmentVariables();
    var configuration = builder.Build();
    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();

    Console.WriteLine($"endpoint : {options.Endpoint}");
    channel_ = GrpcChannelFactory.CreateChannel(options);
    var client = new Submitter.SubmitterClient(channel_);
    Console.WriteLine("Client created");

    Assert.DoesNotThrow(() => client.GetServiceConfiguration(new Empty()));
  }
}
