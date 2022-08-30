// This file is part of the ArmoniK project
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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
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
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.Redis;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
             .AddAmqp(builder.Configuration,
                      logger.GetLogger())
             .AddRedis(builder.Configuration,
                       logger.GetLogger())
             .AddSingleton<ISubmitter, Common.gRPC.Services.Submitter>()
             .AddOption<Common.Injection.Options.Submitter>(builder.Configuration,
                                                            Common.Injection.Options.Submitter.SettingSection)
             .AddGrpcReflection()
             .ValidateGrpcRequests();

      builder.Services.AddHealthChecks();

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
                                          b.AddZipkinExporter(options => options.Endpoint = new Uri(builder.Configuration["Zipkin:Uri"]));
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
                                 });

      var app = builder.Build();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.UseAuthentication();

      app.UseRouting();

      app.UseAuthorization();
      app.UseSerilogRequestLogging();

      app.MapGrpcService<GrpcSubmitterService>();
      app.MapGrpcService<GrpcTasksService>();
      app.MapGrpcService<GrpcSessionsService>();

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


      if (app.Environment.IsDevelopment())
      {
        app.MapGrpcReflectionService();
      }

      var sessionProvider = app.Services.GetRequiredService<SessionProvider>();
      sessionProvider.Get();

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
