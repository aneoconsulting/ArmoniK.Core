// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.DynamicLoading;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using Serilog;

using Pollster = ArmoniK.Core.Common.Injection.Options.Pollster;
using Submitter = ArmoniK.Core.Common.Injection.Options.Submitter;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace ArmoniK.Core.Compute.PollingAgent;

public static class Program
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Compute.PollingAgent");

  public static async Task<int> Main(string[] args)
  {
    var builder = WebApplication.CreateBuilder(args);

    builder.Configuration.SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json",
                        true,
                        false)
           .AddEnvironmentVariables()
           .AddCommandLine(args);

    var logger = new LoggerInit(builder.Configuration);

    logger.GetLogger()
          .LogVersion(typeof(Program));
    logger.GetLogger()
          .LogVersion(typeof(Submitter));
    logger.GetLogger()
          .LogVersion(typeof(HealthCheck));

    try
    {
      AppDomain.CurrentDomain.AssemblyResolve += new CollocatedAssemblyResolver(logger.GetLogger()).AssemblyResolve;

      var pollsterOptions = builder.Configuration.GetSection(Pollster.SettingSection)
                                   .Get<Pollster>() ?? new Pollster();

      builder.Host.UseSerilog(logger.GetSerilogConf())
             .ConfigureHostOptions(options => options.ShutdownTimeout = 2 * pollsterOptions.GraceDelay);

      builder.Services.AddLogging(logger.Configure)
             .AddArmoniKWorkerConnection(builder.Configuration)
             .AddMongoComponents(builder.Configuration,
                                 logger.GetLogger())
             .AddAdapter(builder.Configuration,
                         nameof(Components.QueueAdaptorSettings),
                         logger.GetLogger())
             .AddAdapter(builder.Configuration,
                         nameof(Components.ObjectStorageAdaptorSettings),
                         logger.GetLogger())
             .AddHostedService<Worker>()
             .AddHostedService<RunningTaskProcessor>()
             .AddHostedService<PostProcessor>()
             .AddSingleton<RunningTaskQueue>()
             .AddSingleton<PostProcessingTaskQueue>()
             .AddSingletonWithHealthCheck<Common.Pollster.Pollster>(nameof(Common.Pollster.Pollster))
             .AddSingleton(logger)
             .AddSingleton<ISubmitter, Common.gRPC.Services.Submitter>()
             .AddInitializedOption<Submitter>(builder.Configuration,
                                              Submitter.SettingSection)
             .AddSingleton(pollsterOptions)
             .AddSingleton(new ExceptionManager.Options(pollsterOptions.GraceDelay,
                                                        pollsterOptions.MaxErrorAllowed))
             .AddSingleton<ExceptionManager>()
             .AddSingleton<HealthCheckRecord>()
             .AddSingleton<IHealthCheckPublisher, HealthCheckRecord.Publisher>()
             .AddSingleton<IAgentHandler, AgentHandler>()
             .AddSingleton<DataPrefetcher>()
             .AddSingleton<MeterHolder>()
             .AddSingleton<AgentIdentifier>()
             .AddScoped(typeof(FunctionExecutionMetrics<>))
             .AddSingleton<ITaskProcessingChecker, TaskProcessingCheckerClient>()
             .AddHttpClient();

      var otel = builder.Services.AddOpenTelemetry();
      otel.WithMetrics(opts => opts.SetResourceBuilder(ResourceBuilder.CreateDefault()
                                                                      .AddService("ArmoniK.Core.Agent"))
                                   .AddPrometheusExporter()
                                   .AddMeter(MeterHolder.Name));

      var endpoint = builder.Configuration["OTLP:Uri"];
      var token    = builder.Configuration["OTLP:AuthToken"];
      if (!string.IsNullOrEmpty(endpoint))
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

        builder.Services.AddSingleton(ActivitySource);
        otel.WithTracing(b =>
                         {
                           b.AddSource(ActivitySource.Name);
                           b.AddMongoDBInstrumentation();
                           b.AddOtlpExporter(options =>
                                             {
                                               options.HttpClientFactory = () =>
                                                                           {
                                                                             var client = new HttpClient();
                                                                             if (!string.IsNullOrEmpty(token))
                                                                             {
                                                                               client.DefaultRequestHeaders.Add("Authorization",
                                                                                                                $"Bearer {token}");
                                                                             }

                                                                             return client;
                                                                           };
                                               options.Endpoint = new Uri(endpoint);
                                             });
                         });
      }

      builder.Services.AddHealthChecks();

      var app = builder.Build();

      app.UseSerilogRequestLogging();
      app.UseOpenTelemetryPrometheusScrapingEndpoint();
      app.UseRouting();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.MapHealthChecks("/startup",
                          new HealthCheckOptions
                          {
                            Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                          });

      app.MapHealthChecks("/liveness",
                          new HealthCheckOptions
                          {
                            Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                          });

      app.MapHealthChecks("/readiness",
                          new HealthCheckOptions
                          {
                            Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Readiness)),
                          });

      app.MapGet("/taskprocessing",
                 () => string.Join(",",
                                   app.Services.GetRequiredService<Common.Pollster.Pollster>()
                                      .TaskProcessing));

      app.MapGet("/stopcancelledtask",
                 () => app.Services.GetRequiredService<Common.Pollster.Pollster>()
                          .StopCancelledTask());

      var pushQueueStorage = app.Services.GetRequiredService<IPushQueueStorage>();
      await pushQueueStorage.Init(CancellationToken.None)
                            .ConfigureAwait(false);

      await app.RunAsync()
               .ConfigureAwait(false);
      return 0;
    }
    catch (Exception ex)
    {
      logger.GetLogger()
            .LogCritical(ex,
                         "Host terminated unexpectedly");
      return 1;
    }
    finally
    {
      await Log.CloseAndFlushAsync()
               .ConfigureAwait(false);
    }
  }
}
