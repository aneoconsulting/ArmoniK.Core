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
using System.Net;

using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Formatting.Compact;

namespace ArmoniK.Core.Compute.PollingAgent;

public static class Program
{
  public static int Main(string[] args)
  {
    try
    {
      Log.Information("Starting host");

      var builder = WebApplication.CreateBuilder(args);

      builder.Configuration
             .SetBasePath(Directory.GetCurrentDirectory())
             .AddJsonFile("appsettings.json",
                          true,
                          true)
             .AddEnvironmentVariables()
             .AddCommandLine(args);

      Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(builder.Configuration)
                                            .WriteTo.Console(new CompactJsonFormatter())
                                            .Enrich.FromLogContext()
                                            .CreateLogger();

      var logger = LoggerFactory.Create(loggingBuilder => loggingBuilder.AddSerilog(Log.Logger))
                                .CreateLogger("root");

      builder.Host
             .UseSerilog(Log.Logger);

      builder.Services
             .AddLogging()
             .AddArmoniKCore(builder.Configuration)
             .AddMongoComponents(builder.Configuration,
                                 logger)
             .AddAmqp(builder.Configuration,
                      logger)
             .AddRedis(builder.Configuration,
                       logger)
             .AddHostedService<Worker>()
             .AddSingleton<Pollster>()
             .AddSingleton<PreconditionChecker>()
             .AddSingleton<RequestProcessor>()
             .AddSingleton<Submitter>()
             .AddSingleton<DataPrefetcher>();

      builder.Services.AddHealthChecks();

      builder.WebHost
             .UseConfiguration(builder.Configuration)
             .UseKestrel(options =>
             {
               options.Listen(IPAddress.Loopback,
                              8989,
                              listenOptions => listenOptions.Protocols = HttpProtocols.Http1);
             });

      var app = builder.Build();

      app.UseRouting();

      if (app.Environment.IsDevelopment())
        app.UseDeveloperExceptionPage();

      app.UseEndpoints(endpoints =>
      {
        endpoints.MapHealthChecks("/startup",
                                  new()
                                  {
                                    Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                                  });

        endpoints.MapHealthChecks("/liveness",
                                  new()
                                  {
                                    Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                                  });

        endpoints.MapHealthChecks("/readiness",
                                  new()
                                  {
                                    Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Readiness)),
                                  });
      });
      app.Run();
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
}