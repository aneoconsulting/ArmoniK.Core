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

using ArmoniK.Core.Adapters.LocalStorage;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Adapters.S3;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
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

using OpenTelemetry.Trace;

using Serilog;

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

    try
    {
      builder.Host.UseSerilog(logger.GetSerilogConf());

      builder.Services.AddLogging(logger.Configure)
             .AddMongoComponents(builder.Configuration,
                                 logger.GetLogger())
             .AddQueue(builder.Configuration,
                       logger.GetLogger())
             .AddRedis(builder.Configuration,
                       logger.GetLogger())
             .AddLocalStorage(builder.Configuration,
                              logger.GetLogger())
             .AddS3(builder.Configuration,
                    logger.GetLogger())
             .AddSingleton<ISubmitter, Common.gRPC.Services.Submitter>()
             .AddSingletonWithHealthCheck<ExceptionInterceptor>(nameof(ExceptionInterceptor))
             .AddOption<Common.Injection.Options.Submitter>(builder.Configuration,
                                                            Common.Injection.Options.Submitter.SettingSection)
             .AddGrpcReflection()
             .ValidateGrpcRequests();

      builder.Services.AddHealthChecks();
      builder.Services.AddGrpc(options => options.Interceptors.Add<ExceptionInterceptor>());

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
               .AddOpenTelemetry()
               .WithTracing(b =>
                            {
                              b.AddSource(ActivitySource.Name);
                              b.AddAspNetCoreInstrumentation();
                              b.AddZipkinExporter(options => options.Endpoint =
                                                               new Uri(builder.Configuration["Zipkin:Uri"] ??
                                                                       throw new InvalidOperationException("Zipkin uri should not be null")));
                            });
      }

      builder.Services.AddClientSubmitterAuthenticationStorage(builder.Configuration,
                                                               logger.GetLogger());
      builder.Services.AddClientSubmitterAuthServices(builder.Configuration,
                                                      logger.GetLogger(),
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

      var sessionProvider             = app.Services.GetRequiredService<SessionProvider>();
      var objectFactory               = app.Services.GetRequiredService<IObjectStorageFactory>();
      var pushQueueStorage            = app.Services.GetRequiredService<IPushQueueStorage>();
      var partitionCollectionProvider = app.Services.GetRequiredService<MongoCollectionProvider<PartitionData, PartitionDataModelMapping>>();
      var taskCollectionProvider      = app.Services.GetRequiredService<MongoCollectionProvider<TaskData, TaskDataModelMapping>>();
      var sessionCollectionProvider   = app.Services.GetRequiredService<MongoCollectionProvider<SessionData, SessionDataModelMapping>>();
      var resultCollectionProvider    = app.Services.GetRequiredService<MongoCollectionProvider<Result, ResultDataModelMapping>>();
      var taskObjectFactory           = objectFactory.Init(CancellationToken.None);
      var taskPushQueueStorage        = pushQueueStorage.Init(CancellationToken.None);

      await sessionProvider.Init(CancellationToken.None)
                           .ConfigureAwait(false);
      await partitionCollectionProvider.Init(CancellationToken.None)
                                       .ConfigureAwait(false);
      await taskCollectionProvider.Init(CancellationToken.None)
                                  .ConfigureAwait(false);
      await sessionCollectionProvider.Init(CancellationToken.None)
                                     .ConfigureAwait(false);
      await resultCollectionProvider.Init(CancellationToken.None)
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
