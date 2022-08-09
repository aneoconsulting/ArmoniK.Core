﻿// This file is part of the ArmoniK project
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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Trace;

using Serilog;
using Serilog.Formatting.Compact;

namespace ArmoniK.Core.Compute.PollingAgent;

public static class Program
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Compute.PollingAgent");

  public static int Main(string[] args)
  {
    try
    {
      var builder = WebApplication.CreateBuilder(args);

      builder.Configuration.SetBasePath(Directory.GetCurrentDirectory())
             .AddJsonFile("appsettings.json",
                          true,
                          false)
             .AddEnvironmentVariables()
             .AddCommandLine(args);

      var logger = new LoggerInit(builder.Configuration);

      builder.Host.UseSerilog(logger.GetSerilogConf());

      builder.Services.AddLogging(logger.Configure)
             .AddArmoniKWorkerConnection(builder.Configuration)
             .AddMongoComponents(builder.Configuration,
                                 logger.GetLogger())
             .AddAmqp(builder.Configuration,
                      logger.GetLogger())
             .AddRedis(builder.Configuration,
                       logger.GetLogger())
             .AddHostedService<Worker>()
             .AddSingleton<Pollster>()
             .AddSingleton<ISubmitter, Submitter>()
             .AddSingleton<DataPrefetcher>()
             .AddSingleton<ITaskProcessingChecker, TaskProcessingCheckerClient>()
             .AddHttpClient();

      if (!string.IsNullOrEmpty(builder.Configuration["Zipkin:Uri"]))
      {
        ActivitySource.AddActivityListener(new ActivityListener
                                           {
                                             ShouldListenTo = _ => true,
                                             //Sample         = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                                             ActivityStopped = activity =>
                                                               {
                                                                 foreach (var (key, value) in activity.Baggage)
                                                                 {
                                                                   activity.AddTag(key,
                                                                                   value);
                                                                 }
                                                               },
                                           });

        builder.Services.AddSingleton(ActivitySource)
               .AddOpenTelemetryTracing(b =>
                                        {
                                          b.AddSource(ActivitySource.Name);
                                          b.AddAspNetCoreInstrumentation();
                                          b.AddMongoDBInstrumentation();
                                          b.AddZipkinExporter(options => options.Endpoint = new Uri(builder.Configuration["Zipkin:Uri"]));
                                        });
      }

      builder.Services.AddHealthChecks();

      var app = builder.Build();

      app.UseRouting();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.UseEndpoints(endpoints =>
                       {
                         endpoints.MapHealthChecks("/startup",
                                                   new HealthCheckOptions
                                                   {
                                                     Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                                                   });

                         endpoints.MapHealthChecks("/liveness",
                                                   new HealthCheckOptions
                                                   {
                                                     Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                                                   });

                         endpoints.MapHealthChecks("/readiness",
                                                   new HealthCheckOptions
                                                   {
                                                     Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Readiness)),
                                                   });

                         endpoints.MapGet("/taskprocessing",
                                          () => Task.FromResult(app.Services.GetRequiredService<Pollster>()
                                                                   .TaskProcessing));
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
