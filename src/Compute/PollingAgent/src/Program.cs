// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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

using ArmoniK.Adapters.Amqp;
using ArmoniK.Adapters.MongoDB;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;

using Serilog;
using Serilog.Events;
using Serilog.Formatting.Compact;

namespace ArmoniK.Compute.PollingAgent
{
  public static class Program
  {
    public static int Main(string[] args)
    {
      Log.Logger = new LoggerConfiguration()
                  .MinimumLevel.Override("Microsoft",
                                         LogEventLevel.Information)
                  .Enrich.FromLogContext()
                  .WriteTo.Console(new CompactJsonFormatter())
                  .CreateBootstrapLogger();

      try
      {
        Log.Information("Starting host");
        CreateHostBuilder(args).Build().Run();
        return 0;
      }
      catch (Exception ex)
      {
        Log.Fatal(ex,
                  "Host terminated unexpectedly");
        return 1;
      }
      finally
      {
        Log.CloseAndFlush();
      }
    }

    public static IHostBuilder CreateHostBuilder(string[] args)
    {
      var env = new HostingEnvironment
                {
                  ContentRootPath = Directory.GetCurrentDirectory(),
                };
      return Host.CreateDefaultBuilder(args)
                 .ConfigureHostConfiguration(builder => builder.SetBasePath(env.ContentRootPath)
                                                               .AddJsonFile("appsettings.json",
                                                                            true,
                                                                            true)
                                                               .AddJsonFile($"appsettings.{env.EnvironmentName}.json",
                                                                            true)
                                                               .AddEnvironmentVariables()
                                                               .AddCommandLine(args))
                 .UseSerilog((context, services, config) => config
                                                           .ReadFrom.Configuration(context.Configuration)
                                                           .ReadFrom.Services(services)
                                                           .Enrich.FromLogContext())
                 .ConfigureServices((hostContext, services) =>
                                    {
                                      services.AddLogging()
                                              .AddArmoniKCore(hostContext.Configuration)
                                              .AddMongoComponents(hostContext.Configuration)
                                              .AddAmqp(hostContext.Configuration)
                                              .AddHostedService<Worker>()
                                              .AddSingleton<Pollster>()
                                              .AddSingleton<ComputerService.ComputerServiceClient>();
                                    });
    }
  }
}
