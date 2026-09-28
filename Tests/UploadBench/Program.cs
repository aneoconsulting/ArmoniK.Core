// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;

namespace ArmoniK.Core.Tests.UploadBench;

internal class Program
{
  private static async Task<int> Main(string[] args)
  {
    var builder = new ConfigurationBuilder().AddEnvironmentVariables()
                                            .AddCommandLine(args);

    var configuration = builder.Build();
    Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console()
                                          .CreateBootstrapLogger();

    var factory = new LoggerFactory().AddSerilog();
    var logger  = factory.CreateLogger<Program>();

    var optionsGrpc = configuration.GetRequiredSection(GrpcClient.SettingSection)
                                   .Get<GrpcClient>();
    var optionsUploadBench = new Options.UploadBench();
    configuration.GetSection(Options.UploadBench.SettingSection)
                 .Bind(optionsUploadBench);

    logger.LogInformation("Start Upload benchmark with gRPC options {@OptionsGrpc} and bench options {@OptionsBench}",
                          optionsGrpc,
                          optionsUploadBench);

    await using var benchClient = new UploadBenchClient(optionsGrpc!,
                                                        optionsUploadBench,
                                                        factory);

    // Gracefully cancel the benchmark
    var firstCancel = true;
    Console.CancelKeyPress += (_,
                               args) =>
                              {
                                if (firstCancel)
                                {
                                  logger.LogInformation("Cancellation requested");
                                  benchClient.Cancel();
                                  args.Cancel = true;
                                  firstCancel = false;
                                }
                                else
                                {
                                  logger.LogInformation("Force stop");
                                }
                              };

    try
    {
      await benchClient.CreateSessionAsync();
      await benchClient.RunAll();
      benchClient.PrintMeasurements();
    }
    catch (RpcException e) when (e is
                                 {
                                   StatusCode    : StatusCode.Cancelled,
                                   InnerException: OperationCanceledException,
                                 })
    {
      return 1;
    }
    catch (OperationCanceledException)
    {
      return 1;
    }

    return 0;
  }
}
