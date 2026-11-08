// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;

using LocalStorageOptions = ArmoniK.Core.Adapters.LocalStorage.Options.LocalStorage;
using ObjectBuilder = ArmoniK.Core.Adapters.LocalStorage.ObjectBuilder;

namespace ArmoniK.Core.Standalone;

/// <summary>
///   ArmoniK in a single process: control plane, compute slots, queue, database and object storage.
///   Workers run in separate processes and connect to the slots.
/// </summary>
public static class Program
{
  /// <summary>
  ///   Name of the activity source of the standalone host
  /// </summary>
  public const string ActivitySourceName = "ArmoniK.Core.Standalone";

  /// <summary>
  ///   Entry point
  /// </summary>
  /// <param name="args">Command line arguments, which override the configuration</param>
  /// <returns>
  ///   The exit code
  /// </returns>
  public static async Task<int> Main(string[] args)
  {
    var builder = WebApplication.CreateBuilder(args);

    var options = builder.Configuration.GetSection(StandaloneOptions.SettingSection)
                         .Get<StandaloneOptions>() ?? new StandaloneOptions();
    var dataFolder = Path.GetFullPath(options.DataFolder);

    // Defaults that depend on the data folder; inserted first, so that any other source overrides them
    builder.Configuration.Sources.Insert(0,
                                         new MemoryConfigurationSource
                                         {
                                           InitialData = new Dictionary<string, string?>
                                                         {
                                                           [$"{LocalStorageOptions.SettingSection}:{nameof(LocalStorageOptions.Path)}"] = Path.Combine(dataFolder,
                                                                                                                                                       "objects"),
                                                         },
                                         });

    var logger = new LoggerInit(builder.Configuration);

    try
    {
      builder.Host.UseSerilog(logger.GetSerilogConf());

      builder.Services.AddLogging(logger.Configure)
             .AddSingleton(logger)
             .AddSingleton(new ActivitySource(ActivitySourceName))
             .AddSingleton<InProcessAgents>()
             .AddMemoryTables()
             .AddSingleton<QueueStorage>()
             .AddSingleton<IPushQueueStorage>(sp => sp.GetRequiredService<QueueStorage>())
             .AddSingleton<IPullQueueStorage>(sp => sp.GetRequiredService<QueueStorage>())
             .AddArmoniKControlPlane(builder.Configuration)
             // Registered after the control plane, so that it overrides the default notification over HTTP
             .AddSingleton<ICancelledTaskNotifier>(sp => sp.GetRequiredService<InProcessAgents>())
             .AddClientSubmitterAuthServices(builder.Configuration,
                                             out var authCache);

      new ObjectBuilder().Build(builder.Services,
                                builder.Configuration,
                                logger.GetLogger());

      for (var slot = 0; slot < options.Slots; slot++)
      {
        var index = slot;
        builder.Services.AddSingleton<IHostedService>(sp => new ComputeSlotSupervisor(index,
                                                                                      options,
                                                                                      dataFolder,
                                                                                      builder.Configuration,
                                                                                      sp));
      }

      // The endpoints are configured in code: a Kestrel section in the configuration would also be applied to the
      // servers that the agents of the slots start for their workers.
      builder.WebHost.UseKestrel(kestrel =>
                                 {
                                   void Listen(int                   port,
                                               Action<ListenOptions> configure)
                                   {
                                     if (options.ListenAnyIP)
                                     {
                                       kestrel.ListenAnyIP(port,
                                                           configure);
                                     }
                                     else
                                     {
                                       kestrel.ListenLocalhost(port,
                                                               configure);
                                     }
                                   }

                                   Listen(options.GrpcPort,
                                          listenOptions =>
                                          {
                                            listenOptions.Protocols = HttpProtocols.Http2;
                                            listenOptions.Use(async (context,
                                                                     next) =>
                                                              {
                                                                await next()
                                                                  .ConfigureAwait(false);
                                                                authCache.FlushConnection(context.ConnectionId);
                                                              });
                                          });
                                   Listen(options.HttpPort,
                                          listenOptions => listenOptions.Protocols = HttpProtocols.Http1);
                                 });

      var app = builder.Build();

      app.UseRouting();
      app.UseGrpcWeb();
      app.UseAuthentication();
      app.UseAuthorization();
      app.MapArmoniKControlPlane(endpoint => endpoint.EnableGrpcWeb());

      foreach (var tag in Enum.GetValues<HealthCheckTag>())
      {
        app.MapHealthChecks($"/{tag.ToString().ToLowerInvariant()}",
                            new HealthCheckOptions
                            {
                              Predicate = check => check.Tags.Contains(tag.ToString()),
                            });
      }

      await InitStorage(app.Services)
        .ConfigureAwait(false);

      var appLogger = app.Services.GetRequiredService<ILogger<StandaloneOptions>>();
      appLogger.LogInformation("ArmoniK standalone: gRPC on port {GrpcPort}, gRPC-Web and health checks on port {HttpPort}, {Slots} compute slots, data in {DataFolder}",
                               options.GrpcPort,
                               options.HttpPort,
                               options.Slots,
                               dataFolder);

      // The compute slots are started by their supervisors, hosted by the root host, and stopped before the storage they
      // share is disposed with the root host.
      await app.RunAsync()
               .ConfigureAwait(false);

      return 0;
    }
    catch (Exception e)
    {
      logger.GetLogger()
            .LogCritical(e,
                         "Host terminated unexpectedly");
      return 1;
    }
    finally
    {
      await Log.CloseAndFlushAsync()
               .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Initialize the shared storage: tables, watchers, queue and object storage
  /// </summary>
  /// <param name="services">Services of the root host</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  private static async Task InitStorage(IServiceProvider services)
  {
    IInitializable[] components =
    [
      services.GetRequiredService<ITaskTable>(), services.GetRequiredService<ISessionTable>(), services.GetRequiredService<IResultTable>(),
      services.GetRequiredService<IPartitionTable>(), services.GetRequiredService<IAuthenticationTable>(), services.GetRequiredService<ITaskWatcher>(),
      services.GetRequiredService<IResultWatcher>(), services.GetRequiredService<IPushQueueStorage>(), services.GetRequiredService<IObjectStorage>(),
    ];

    foreach (var component in components)
    {
      await component.Init(CancellationToken.None)
                     .ConfigureAwait(false);
    }
  }
}
