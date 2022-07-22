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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Threading;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1.Submitter;

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
    var sleepStr = Environment.GetEnvironmentVariable("SLEEP_BEFORE_LAUNCH");
    if (!string.IsNullOrEmpty(sleepStr))
    {
      Thread.Sleep(int.Parse(sleepStr));
    }

    Console.WriteLine("Hello Mock V3!");

    var builder       = new ConfigurationBuilder().AddEnvironmentVariables();
    var configuration = builder.Build();
    Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console(new CompactJsonFormatter())
                                          .CreateBootstrapLogger();

    var factory = new LoggerFactory().AddSerilog();

    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();
    var optionsHtcMock = new Options.HtcMock();
    configuration.GetSection(Options.HtcMock.SettingSection)
                 .Bind(optionsHtcMock);
    var channel = GrpcChannelFactory.CreateChannel(options);

    var submitterClient = new Submitter.SubmitterClient(channel);

    var gridClient = new GridClient(submitterClient,
                                    factory,
                                    optionsHtcMock);

    using var client = new HtcMockClient(gridClient,
                                         factory.CreateLogger<Htc.Mock.Client>());

    Console.CancelKeyPress += (sender,
                               args) =>
                              {
                                args.Cancel = true;
                                client.Dispose();
                                Environment.Exit(0);
                              };

    var runConfiguration = new RunConfiguration(optionsHtcMock.TotalCalculationTime,
                                                optionsHtcMock.NTasks,
                                                optionsHtcMock.DataSize,
                                                optionsHtcMock.MemorySize,
                                                optionsHtcMock.SubTasksLevels);
    return client.Start(runConfiguration)
             ? 0
             : 1;
  }
}
