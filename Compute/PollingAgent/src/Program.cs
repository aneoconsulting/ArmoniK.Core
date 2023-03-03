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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Adapters.LocalStorage;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.RabbitMQ;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Adapters.S3;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Trace;

using Serilog;

using Pollster = ArmoniK.Core.Common.Injection.Options.Pollster;
using Submitter = ArmoniK.Core.Common.Injection.Options.Submitter;

namespace ArmoniK.Core.Compute.PollingAgent;

public static class Program
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Compute.PollingAgent");

  public static async Task<int> Main(string[] args)
  {
    Console.WriteLine("ArmoniK.Core.Compute.PollingAgent (Main)");
    var builder = WebApplication.CreateBuilder(args);
    Console.WriteLine("ArmoniK.Core.Compute.PollingAgent (Main2)");

    builder.Configuration.SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json",
                        true,
                        false)
           .AddEnvironmentVariables()
           .AddCommandLine(args);
    Console.WriteLine("ArmoniK.Core.Compute.PollingAgent (Main3)");

    var logger = new LoggerInit(builder.Configuration);
    Console.WriteLine("ArmoniK.Core.Compute.PollingAgent (Main4)");

    try
    {
      var pollsterOptions = builder.Configuration.GetSection(Pollster.SettingSection)
                                   .Get<Pollster>() ?? new Pollster();
      Console.WriteLine("1");
      builder.Host.UseSerilog(logger.GetSerilogConf()).ConfigureHostOptions(options => options.ShutdownTimeout = pollsterOptions.ShutdownTimeout);
      Console.WriteLine("2");

      builder.Services.AddLogging(logger.Configure);
      Console.WriteLine("3");
      builder.Services.AddArmoniKWorkerConnection(builder.Configuration);
      Console.WriteLine("4");
      builder.Services.AddMongoComponents(builder.Configuration, logger.GetLogger());
      Console.WriteLine("5");
      builder.Services.AddAmqp(builder.Configuration, logger.GetLogger());
      Console.WriteLine("6");
      builder.Services.AddRabbit(builder.Configuration, logger.GetLogger());
      Console.WriteLine("7");
      builder.Services.AddRedis(builder.Configuration, logger.GetLogger());
      Console.WriteLine("8");
      builder.Services.AddS3(builder.Configuration, logger.GetLogger());
      Console.WriteLine("9");
      builder.Services.AddLocalStorage(builder.Configuration, logger.GetLogger());
      Console.WriteLine("10");
      builder.Services.AddHostedService<Worker>();
      Console.WriteLine("11");
      builder.Services.AddSingletonWithHealthCheck<Common.Pollster.Pollster>(nameof(Common.Pollster.Pollster));
      Console.WriteLine("12");
      builder.Services.AddSingleton(logger);
      Console.WriteLine("13");
      builder.Services.AddSingleton<ISubmitter, Common.gRPC.Services.Submitter>();
      Console.WriteLine("14");
      builder.Services.AddInitializedOption<Submitter>(builder.Configuration, Submitter.SettingSection);
      Console.WriteLine("15");
      builder.Services.AddSingleton(pollsterOptions);
      Console.WriteLine("16");
      builder.Services.AddSingleton<IAgentHandler, AgentHandler>();
      Console.WriteLine("17");
      builder.Services.AddSingleton<DataPrefetcher>();
      Console.WriteLine("18");
      builder.Services.AddSingleton<ITaskProcessingChecker, TaskProcessingCheckerClient>();
      Console.WriteLine("19");
      builder.Services.AddHttpClient();
      Console.WriteLine("20");

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

      app.UseSerilogRequestLogging();
      app.UseRouting();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }
      Console.WriteLine("HealthCheck Init :");
      app.UseEndpoints(endpoints =>
                       {
                         endpoints.MapHealthChecks("/startup",
                                                   new HealthCheckOptions
                                                   {
                                                     Predicate = check =>
                                                     {
                                                       Console.WriteLine("HealthCheck check :");
                                                       return check.Tags.Contains(nameof(HealthCheckTag.Startup));
                                                     },
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
                                          () => Task.FromResult(app.Services.GetRequiredService<Common.Pollster.Pollster>()
                                                                   .TaskProcessing));

                         endpoints.MapGet("/stopcancelledtask",
                                          async () =>
                                          {
                                            var stopCancelledTask = app.Services.GetRequiredService<Common.Pollster.Pollster>()
                                                                       .StopCancelledTask;
                                            if (stopCancelledTask != null)
                                            {
                                              await stopCancelledTask.Invoke()
                                                                     .ConfigureAwait(false);
                                            }
                                          });
                       });

      var pushQueueStorage = app.Services.GetRequiredService<IPushQueueStorage>();
      await pushQueueStorage.Init(CancellationToken.None)
                            .ConfigureAwait(false);

      await app.RunAsync()
               .ConfigureAwait(false);
      return 0;
    }
    catch (Exception ex)
    {
      Console.WriteLine(ex);
      Console.WriteLine(ex.Message);
      Console.WriteLine(ex.InnerException);
      Console.WriteLine(ex.StackTrace);

      logger.GetLogger()
            .LogCritical(ex,
                         "Host terminated unexpectedly");
      return 1;
    }
    finally
    {
      Log.CloseAndFlush();
    }
  }
}
