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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Control.Metrics.Options;
using ArmoniK.Core.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

using Serilog;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace ArmoniK.Core.Control.Metrics;

public static class Program
{
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
             .AddSingleton(builder.Configuration.GetInitializedValue<MetricsExporter>(MetricsExporter.SettingSection))
             .AddHostedService<ArmoniKMeter>()
             .AddControllers();

      builder.Services.AddOpenTelemetry()
             .WithMetrics(b =>
                          {
                            b.AddPrometheusExporter(options => options.ScrapeResponseCacheDurationMilliseconds = 2000);
                            b.SetResourceBuilder(ResourceBuilder.CreateDefault()
                                                                .AddService("armonik-service"));
                            b.AddMeter(nameof(ArmoniKMeter));
                          });

      var app = builder.Build();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.UseSerilogRequestLogging();
      app.UseOpenTelemetryPrometheusScrapingEndpoint();
      app.UseRouting();
      app.UseHttpsRedirection();
      app.UseAuthorization();

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
      app.MapControllers();


      var taskTable      = app.Services.GetRequiredService<ITaskTable>();
      var partitionTable = app.Services.GetRequiredService<IPartitionTable>();
      await taskTable.Init(CancellationToken.None)
                     .ConfigureAwait(false);
      await partitionTable.Init(CancellationToken.None)
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
