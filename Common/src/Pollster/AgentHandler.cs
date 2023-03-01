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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Common.gRPC.Services;
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
///   Represents the handler that will provide servers to process requests from worker
/// </summary>
public class AgentHandler : IAgentHandler, IAsyncDisposable
{
  private readonly WebApplication                       app_;
  private readonly ComputePlane                         computePlaneOptions_;
  private readonly Injection.Options.DependencyResolver dependencyResolverOptions_;
  private readonly ILogger<AgentHandler>                logger_;
  private readonly IObjectStorageFactory                objectStorageFactory_;
  private readonly IPushQueueStorage                    pushQueueStorage_;
  private readonly ITaskTable                           taskTable_;
  private readonly GrpcAgentService                     service_;
  private readonly ISubmitter                           submitter_;

  /// <summary>
  ///   Initializes a new instance
  /// </summary>
  /// <param name="loggerInit">Logger initializer used to configure the loggers needed by the worker</param>
  /// <param name="computePlaneOptions">Options needed for the creation of the servers</param>
  /// <param name="dependencyResolverOptions">Configuration for DependencyResolver</param>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorageFactory">Interface class to create object storage</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public AgentHandler(LoggerInit                           loggerInit,
                      ComputePlane                         computePlaneOptions,
                      Injection.Options.DependencyResolver dependencyResolverOptions,
                      ISubmitter                           submitter,
                      IObjectStorageFactory                objectStorageFactory,
                      IPushQueueStorage                    pushQueueStorage,
                      ITaskTable                           taskTable,
                      ILogger<AgentHandler>                logger)
  {
    computePlaneOptions_       = computePlaneOptions;
    dependencyResolverOptions_ = dependencyResolverOptions;
    submitter_                 = submitter;
    objectStorageFactory_      = objectStorageFactory;
    pushQueueStorage_          = pushQueueStorage;
    taskTable_                 = taskTable;
    logger_                    = logger;

    try
    {
      if (computePlaneOptions.AgentChannel?.Address == null)
      {
        throw new ArgumentNullException(nameof(computePlaneOptions.AgentChannel));
      }

      logger.LogDebug("Agent address is {address}",
                      computePlaneOptions.AgentChannel.Address);

      var builder = WebApplication.CreateBuilder();

      builder.Host.UseSerilog(loggerInit.GetSerilogConf());

      builder.Services.AddLogging(loggerInit.Configure)
             .AddSingleton<GrpcAgentService>()
             .AddGrpc();

      builder.WebHost.ConfigureKestrel(options =>
                                       {
                                         if (File.Exists(computePlaneOptions.AgentChannel.Address))
                                         {
                                           File.Delete(computePlaneOptions.AgentChannel.Address);
                                         }

                                         options.ListenUnixSocket(computePlaneOptions.AgentChannel.Address,
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

  /// <inheritdoc />
  public async Task<IAgent> Start(string            token,
                                  ILogger           logger,
                                  SessionData       sessionData,
                                  TaskData          taskData,
                                  CancellationToken cancellationToken)
  {
    try
    {
      var agent = new Agent(submitter_,
                            objectStorageFactory_,
                            pushQueueStorage_,
                            taskTable_,
                            dependencyResolverOptions_,
                            sessionData,
                            taskData,
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

  /// <inheritdoc />
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

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    await app_.DisposeAsync()
              .ConfigureAwait(false);

    if (File.Exists(computePlaneOptions_.AgentChannel.Address))
    {
      File.Delete(computePlaneOptions_.AgentChannel.Address);
    }
  }
}
