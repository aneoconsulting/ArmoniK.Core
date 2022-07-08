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

using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Formatting.Compact;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Server;

public static class Program
{
  private static readonly string SocketPath = "/cache/armonik_worker.sock";

  public static int Main(string[] args)
  {
    try
    {
      Log.Information("Starting web host");

      var builder = WebApplication.CreateBuilder(args);

      builder.Configuration.SetBasePath(Directory.GetCurrentDirectory())
             .AddJsonFile("appsettings.json",
                          true,
                          false)
             .AddEnvironmentVariables()
             .AddCommandLine(args);

      Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(builder.Configuration)
                                            .WriteTo.Console(new CompactJsonFormatter())
                                            .Enrich.FromLogContext()
                                            .CreateLogger();

      var loggerFactory = LoggerFactory.Create(loggingBuilder => loggingBuilder.AddSerilog(Log.Logger));
      var logger        = loggerFactory.CreateLogger("root");

      builder.Host.UseSerilog(Log.Logger);

      builder.WebHost.ConfigureKestrel(options =>
                                       {
                                         if (File.Exists(SocketPath))
                                         {
                                           File.Delete(SocketPath);
                                         }

                                         options.ListenUnixSocket(SocketPath,
                                                                  listenOptions =>
                                                                  {
                                                                    listenOptions.Protocols = HttpProtocols.Http2;
                                                                  });
                                       });

      builder.Services.AddSingleton<ApplicationLifeTimeManager>()
             .AddSingleton(sp => loggerFactory)
             .AddLogging()
             .AddGrpc(options => options.MaxReceiveMessageSize = null);


      var app = builder.Build();

      if (app.Environment.IsDevelopment())
      {
        app.UseDeveloperExceptionPage();
      }

      app.UseSerilogRequestLogging();

      app.UseRouting();


      app.UseEndpoints(endpoints =>
                       {
                         endpoints.MapGrpcService<WorkerService>();

                         if (app.Environment.IsDevelopment())
                         {
                           endpoints.MapGrpcReflectionService();
                           logger.LogInformation("Grpc Reflection Activated");
                         }
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
