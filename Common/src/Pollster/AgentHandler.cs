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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Worker.Options;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;

using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
/// Represents the handler that will provide servers to process requests from worker
/// </summary>
public class AgentHandler : IAgentHandler, IAsyncDisposable
{
  private readonly ISubmitter            submitter_;
  private readonly IObjectStorageFactory objectStorageFactory_;
  private readonly ILogger<AgentHandler> logger_;
  private readonly WebApplication        app_;
  private readonly GrpcAgentService      service_;

  /// <summary>
  /// Initializes a new instance
  /// </summary>
  /// <param name="loggerInit">Logger initializer used to configure the loggers needed by the worker</param>
  /// <param name="computePlanOptions">Options needed for the creation of the servers</param>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorageFactory">Interface class to create object storage</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public AgentHandler(LoggerInit            loggerInit,
                      ComputePlan           computePlanOptions,
                      ISubmitter            submitter,
                      IObjectStorageFactory objectStorageFactory,
                      ILogger<AgentHandler> logger)
  {
    submitter_            = submitter;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;

    try
    {
      if (computePlanOptions.AgentChannel?.Address == null)
      {
        throw new ArgumentNullException(nameof(computePlanOptions.AgentChannel));
      }

      logger.LogDebug("Agent address is {address}",
                      computePlanOptions.AgentChannel.Address);

      var builder = WebApplication.CreateBuilder();

      builder.Host.UseSerilog(loggerInit.GetSerilogConf());

      builder.Services.AddLogging(loggerInit.Configure)
             .AddSingleton<GrpcAgentService>()
             .AddGrpc();

      builder.WebHost.ConfigureKestrel(options =>
                                       {
                                         if (File.Exists(computePlanOptions.AgentChannel.Address))
                                         {
                                           File.Delete(computePlanOptions.AgentChannel.Address);
                                         }

                                         options.ListenUnixSocket(computePlanOptions.AgentChannel.Address,
                                                                  listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
                                       });

      app_ = builder.Build();

      app_.UseRouting();
      app_.MapGrpcService<GrpcAgentService>();

      service_ = app_.Services.GetRequiredService<GrpcAgentService>();
      app_.Start();
    }
    catch (Exception e)
    {
      logger.LogError(e,
                      "Error while initializing agent server");
      throw;
    }
  }

  public async Task<IAgent> Start(string            token,
                                  ILogger           logger,
                                  string            sessionId,
                                  string            taskId,
                                  CancellationToken cancellationToken)
  {
    try
    {
      var agent = new Agent(submitter_,
                            objectStorageFactory_,
                            sessionId,
                            taskId,
                            token,
                            logger);

      await service_.Start(agent)
                    .ConfigureAwait(false);

      return agent;
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Error while starting agent server");
      throw;
    }
  }

  public async Task Stop(CancellationToken cancellationToken)
  {
    try
    {
      await service_.Stop()
                    .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Error while stopping agent server");
      throw;
    }
  }

  public async ValueTask DisposeAsync()
  {
    await app_.DisposeAsync()
              .ConfigureAwait(false);
  }
}
