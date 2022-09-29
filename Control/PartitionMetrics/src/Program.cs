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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Control.PartitionMetrics.Options;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

using Serilog;

namespace ArmoniK.Core.Control.PartitionMetrics;

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
             .AddOpenTelemetryMetrics(b =>
                                      {
                                        b.AddPrometheusExporter(options => options.ScrapeResponseCacheDurationMilliseconds = 2000);
                                        b.SetResourceBuilder(ResourceBuilder.CreateDefault()
                                                                            .AddService("armonik-service"));
                                        b.AddMeter(nameof(ArmoniKMeter));
                                      })
             .AddOption<MetricsExporter>(builder.Configuration,
                                         MetricsExporter.SettingSection)
             .AddHostedService<ArmoniKMeter>()
             .AddHttpClient()
             .AddControllers();

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
                         endpoints.MapControllers();
                       });

      var sessionProvider = app.Services.GetRequiredService<SessionProvider>();
      await sessionProvider.Init(CancellationToken.None)
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
      Log.CloseAndFlush();
    }
  }
}
