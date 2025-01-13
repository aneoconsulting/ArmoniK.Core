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

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Base;
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
public sealed class AgentHandler : IAgentHandler, IAsyncDisposable
{
  private readonly WebApplication        app_;
  private readonly ComputePlane          computePlaneOptions_;
  private readonly ILogger<AgentHandler> logger_;
  private readonly IObjectStorage        objectStorage_;
  private readonly IPushQueueStorage     pushQueueStorage_;
  private readonly IResultTable          resultTable_;
  private readonly GrpcAgentService      service_;
  private readonly ISubmitter            submitter_;
  private readonly ITaskTable            taskTable_;

  /// <summary>
  ///   Initializes a new instance
  /// </summary>
  /// <param name="loggerInit">Logger initializer used to configure the loggers needed by the worker</param>
  /// <param name="computePlaneOptions">Options needed for the creation of the servers</param>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorage">Interface class to create object storage</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public AgentHandler(LoggerInit            loggerInit,
                      ComputePlane          computePlaneOptions,
                      ISubmitter            submitter,
                      IObjectStorage        objectStorage,
                      IPushQueueStorage     pushQueueStorage,
                      IResultTable          resultTable,
                      ITaskTable            taskTable,
                      ILogger<AgentHandler> logger)
  {
    computePlaneOptions_ = computePlaneOptions;
    submitter_           = submitter;
    objectStorage_       = objectStorage;
    pushQueueStorage_    = pushQueueStorage;
    resultTable_         = resultTable;
    taskTable_           = taskTable;
    logger_              = logger;

    try
    {
      if (computePlaneOptions.AgentChannel?.Address is null)
      {
        throw new ArgumentNullException(nameof(computePlaneOptions),
                                        $"{nameof(computePlaneOptions.AgentChannel)} is null");
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
                                  string            folder,
                                  CancellationToken cancellationToken)
  {
    try
    {
      var agent = new Agent(submitter_,
                            objectStorage_,
                            pushQueueStorage_,
                            resultTable_,
                            taskTable_,
                            sessionData,
                            taskData,
                            folder,
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
