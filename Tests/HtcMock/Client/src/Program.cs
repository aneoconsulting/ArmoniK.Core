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
using System.Threading;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;

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
    var channel = GrpcChannelFactory.CreateChannel(options!);

    var gridClient = new GridClient(channel,
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
