// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
// but WITHOUT ANY WARRANTY

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Utils;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;

using Serilog;

namespace ArmoniK.Core.Common.Pollster;

public class AgentHandler : IAgentHandler, IAsyncDisposable
{
  private readonly IAgent          agent_;
  private readonly LoggerInit      loggerInit_;
  private          WebApplication? app_;

  public AgentHandler(IAgent  agent,
                      LoggerInit loggerInit)
  {
    agent_           = agent;
    loggerInit_ = loggerInit;
  }

  public Task Start(string sessionId,
                    string taskId,
                    string socketPath)
  {
    agent_.Init(sessionId,
                taskId);

    var builder = WebApplication.CreateBuilder();

    builder.Logging.AddSerilog(loggerInit_.GetSerilogConf());

    builder.Services.AddSingleton(agent_)
           .AddLogging(loggerInit_.Configure)
           .AddGrpc();

    builder.WebHost.ConfigureKestrel(options =>
                                     {
                                       if (File.Exists(socketPath))
                                       {
                                         File.Delete(socketPath);
                                       }

                                       options.ListenUnixSocket(socketPath,
                                                                listenOptions =>
                                                                {
                                                                  listenOptions.Protocols = HttpProtocols.Http2;
                                                                });
                                     });

    app_ = builder.Build();

    app_.UseRouting();

    app_.MapGrpcService<GrpcAgentService>();

    app_.RunAsync();
    return Task.CompletedTask;
  }

  public async Task Stop(CancellationToken cancellationToken)
  {
    if (app_ != null)
    {
      await app_.StopAsync(cancellationToken)
                .ConfigureAwait(false);
    }
  }

  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
    => await agent_.FinalizeTaskCreation(cancellationToken)
                   .ConfigureAwait(false);

  public async ValueTask DisposeAsync()
  {
    if (app_ != null)
    {
      await app_.DisposeAsync()
                .ConfigureAwait(false);
    }
  }
}