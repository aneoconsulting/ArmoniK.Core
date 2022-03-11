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
using System.Diagnostics;
using System.IO;

using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;
using ArmoniK.Core.Control.Submitter.Services;

using OpenTelemetry.Trace;

using Serilog.Formatting.Compact;

namespace ArmoniK.Core.Control.Submitter;

public static class Program
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Control.Submitter");
  public static int Main(string[] args)
  {
    try
    {
      Log.Information("Starting web host");


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
             .ValidateGrpcRequests();

      if (!string.IsNullOrEmpty(builder.Configuration["Zipkin:Uri"]))
      {
        ActivitySource.AddActivityListener(new ActivityListener
        {
          ShouldListenTo = _ => true,
          //Sample         = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
          ActivityStopped = activity =>
          {
            foreach (var (key, value) in activity.Baggage)
              activity.AddTag(key,
                              value);
          },
        });

        builder.Services
               .AddSingleton(ActivitySource)
               .AddOpenTelemetryTracing(b =>
               {
                 b.AddSource(ActivitySource.Name);
                 b.AddAspNetCoreInstrumentation();
                 b.AddZipkinExporter(options => options.Endpoint = new Uri(builder.Configuration["Zipkin:Uri"]));
               });
      }


      var app = builder.Build();

      if (app.Environment.IsDevelopment())
        app.UseDeveloperExceptionPage();

      app.UseSerilogRequestLogging();

      app.UseRouting();


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

        //readiness uses grpc to ensure corresponding features are ok.
        endpoints.MapGrpcService<GrpcHealthCheckService>();

        endpoints.MapGrpcService<GrpcSubmitterService>();

        if (app.Environment.IsDevelopment())
          endpoints.MapGrpcReflectionService();
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