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
using System.IO;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Injection;

using Grpc.Net.Client;

using Htc.Mock.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Formatting.Compact;

namespace ArmoniK.Samples.HtcMock.Client;

internal class Program
{
  private static int Main()
  {
    Console.WriteLine("Hello Mock V3!");

    var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                            .AddEnvironmentVariables();
    var configuration = builder.Build();
    Log.Logger = new LoggerConfiguration()
                 .ReadFrom.Configuration(configuration)
                 .Enrich.FromLogContext()
                 .WriteTo.Console(new CompactJsonFormatter())
                 .CreateBootstrapLogger();

    var factory = new LoggerFactory().AddSerilog();

    var options = configuration.GetRequiredValue<Options.Grpc>(Options.Grpc.SettingSection);
    var optionsHtcMock = configuration.GetRequiredValue<Options.HtcMock>(Options.HtcMock.SettingSection);
    var channel = GrpcChannel.ForAddress(options.Endpoint);

    var submitterClient = new Submitter.SubmitterClient(channel);

    var gridClient = new GridClient(submitterClient,
                                    factory);

    var client = new HtcMockClient(gridClient,
                                   factory.CreateLogger<Htc.Mock.Client>());

    var runConfiguration = new RunConfiguration(new TimeSpan(0,
                                                             0,
                                                             0,
                                                             0,
                                                             100),
                                                optionsHtcMock.NTasks,
                                                1,
                                                1,
                                                4);
    return client.Start(runConfiguration) ? 0 : 1;
  }
}