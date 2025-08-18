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
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.DynamicLoading;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using Serilog;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace ArmoniK.Core.Control.Submitter;

public static class Program
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Control.Submitter");

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
          .LogVersion(typeof(Common.gRPC.Services.Submitter));
    logger.GetLogger()
          .LogVersion(typeof(HealthCheck));

    try
    {
      AppDomain.CurrentDomain.AssemblyResolve += new CollocatedAssemblyResolver(logger.GetLogger()).AssemblyResolve;

      builder.Host.UseSerilog(logger.GetSerilogConf());

      builder.Services.AddLogging(logger.Configure)
             .AddHttpClient()
             .AddMongoComponents(builder.Configuration,
                                 logger.GetLogger())
             .AddAdapter(builder.Configuration,
                         nameof(Components.QueueAdaptorSettings),
                         logger.GetLogger())
             .AddAdapter(builder.Configuration,
                         nameof(Components.ObjectStorageAdaptorSettings),
                         logger.GetLogger())
             .AddSingleton<ISubmitter, Common.gRPC.Services.Submitter>()
             .AddSingletonWithHealthCheck<ExceptionInterceptor>(nameof(ExceptionInterceptor))
             .AddOption<Common.Injection.Options.Submitter>(builder.Configuration,
                                                            Common.Injection.Options.Submitter.SettingSection)
             .AddSingleton(sp => new ExceptionManager.Options(TimeSpan.Zero,
                                                              sp.GetRequiredService<Common.Injection.Options.Submitter>()
                                                                .MaxErrorAllowed))
             .AddSingleton<ExceptionManager>()
             .AddGrpcReflection()
             .AddSingleton<MeterHolder>()
             .AddSingleton<AgentIdentifier>()
             .AddScoped(typeof(FunctionExecutionMetrics<>))
             .ValidateGrpcRequests();

      builder.Services.AddHealthChecks();
      builder.Services.AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>());

      var otel = builder.Services.AddOpenTelemetry();
      otel.WithMetrics(opts => opts.SetResourceBuilder(ResourceBuilder.CreateDefault()
                                                                      .AddService("ArmoniK.Core.Submitter"))
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

      builder.Services.AddClientSubmitterAuthenticationStorage(builder.Configuration);
      builder.Services.AddClientSubmitterAuthServices(builder.Configuration,
                                                      out var authCache);

      builder.WebHost.UseKestrel(options =>
                                 {
                                   options.ListenAnyIP(1080,
                                                       listenOptions =>
                                                       {
                                                         listenOptions.Protocols = HttpProtocols.Http2;
                                                         listenOptions.Use(async (context,
                                                                                  func) =>
                                                                           {
                                                                             await func.Invoke()
                                                                                       .ConfigureAwait(false);
                                                                             authCache.FlushConnection(context.ConnectionId);
                                                                           });
                                                       });
                                   options.ListenAnyIP(1081,
                                                       listenOptions => listenOptions.Protocols = HttpProtocols.Http1);
                                 });

      var app = builder.Build();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.UseAuthentication();

      app.UseRouting();
      app.UseGrpcWeb();
      app.UseOpenTelemetryPrometheusScrapingEndpoint(context => context.Connection.LocalPort == 1081 && context.Request.Path == "/metrics");

      app.UseAuthorization();
      app.UseSerilogRequestLogging();

      app.MapGrpcService<GrpcSubmitterService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcTasksService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcSessionsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcResultsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcApplicationsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcAuthService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcEventsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcPartitionsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcVersionsService>()
         .EnableGrpcWeb();
      app.MapGrpcService<GrpcHealthChecksService>()
         .EnableGrpcWeb();

      app.UseHealthChecks("/startup",
                          1081,
                          new HealthCheckOptions
                          {
                            Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                          });

      app.UseHealthChecks("/liveness",
                          1081,
                          new HealthCheckOptions
                          {
                            Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                          });

      if (app.Environment.IsDevelopment())
      {
        app.MapGrpcReflectionService();
      }

      var objectStorage        = app.Services.GetRequiredService<IObjectStorage>();
      var pushQueueStorage     = app.Services.GetRequiredService<IPushQueueStorage>();
      var taskTable            = app.Services.GetRequiredService<ITaskTable>();
      var resultTable          = app.Services.GetRequiredService<IResultTable>();
      var partitionTable       = app.Services.GetRequiredService<IPartitionTable>();
      var sessionTable         = app.Services.GetRequiredService<ISessionTable>();
      var authTable            = app.Services.GetRequiredService<IAuthenticationTable>();
      var taskObjectFactory    = objectStorage.Init(CancellationToken.None);
      var taskPushQueueStorage = pushQueueStorage.Init(CancellationToken.None);

      await taskTable.Init(CancellationToken.None)
                     .ConfigureAwait(false);
      await resultTable.Init(CancellationToken.None)
                       .ConfigureAwait(false);
      await partitionTable.Init(CancellationToken.None)
                          .ConfigureAwait(false);
      await sessionTable.Init(CancellationToken.None)
                        .ConfigureAwait(false);
      await authTable.Init(CancellationToken.None)
                     .ConfigureAwait(false);

      await taskObjectFactory.ConfigureAwait(false);
      await taskPushQueueStorage.ConfigureAwait(false);

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
      Log.CloseAndFlush();
    }
  }
}
