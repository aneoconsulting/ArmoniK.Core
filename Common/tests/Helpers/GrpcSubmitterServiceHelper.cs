// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Auth;
using ArmoniK.Utils;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class GrpcSubmitterServiceHelper : IDisposable
{
  private readonly WebApplication      app_;
  private readonly Mutex               channelMutex_ = new();
  private readonly ILoggerFactory      loggerFactory_;
  private          ChannelBase?        channel_;
  private          HttpMessageHandler? handler_;
  private          TestServer?         server_;

  public GrpcSubmitterServiceHelper(ISubmitter                  submitter,
                                    List<MockIdentity>          authIdentities,
                                    AuthenticatorOptions        authOptions,
                                    LogLevel                    logLevel,
                                    Action<IServiceCollection>? serviceConfigurator  = null,
                                    bool                        validateGrpcRequests = true)
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider(logLevel));

    var builder = WebApplication.CreateBuilder();

    builder.Services.AddSingleton(loggerFactory_)
           .AddSingleton(submitter);
    builder.Services.AddSingleton(loggerFactory_.CreateLogger<GrpcSubmitterService>())
           .AddTransient<IAuthenticationTable, MockAuthenticationTable>(_ => new MockAuthenticationTable(authIdentities))
           .AddSingleton(new AuthenticationCache())
           .AddSingleton<ITaskTable, SimpleTaskTable>()
           .AddSingleton<IResultTable, SimpleResultTable>()
           .AddSingleton<ISessionTable, SimpleSessionTable>()
           .Configure<AuthenticatorOptions>(o => o.CopyFrom(authOptions))
           .AddLogging(build => build.SetMinimumLevel(logLevel)
                                     .AddConsole())
           .AddHttpClient()
           .AddSingleton<FunctionExecutionMetricsFactory>()
           .AddSingleton<AgentIdentifier>()
           .AddAuthentication()
           .AddScheme<AuthenticatorOptions, Authenticator>(Authenticator.SchemeName,
                                                           _ =>
                                                           {
                                                           });
    builder.Services.AddSingleton<IAuthorizationPolicyProvider, AuthorizationPolicyProvider>()
           .AddAuthorization();
    if (validateGrpcRequests)
    {
      builder.Services.ValidateGrpcRequests();
    }

    builder.Services.AddGrpc();

    serviceConfigurator?.Invoke(builder.Services);

    builder.WebHost.UseTestServer(options => options.PreserveExecutionContext = true);
    app_ = builder.Build();
    app_.UseRouting();
    app_.UseAuthentication();
    app_.UseAuthorization();
    app_.MapGrpcService<GrpcSubmitterService>();
    app_.MapGrpcService<GrpcResultsService>();
    app_.MapGrpcService<GrpcSessionsService>();
    app_.MapGrpcService<GrpcTasksService>();
    app_.MapGrpcService<GrpcApplicationsService>();
    app_.MapGrpcService<GrpcAuthService>();
    app_.MapGrpcService<GrpcEventsService>();
    app_.MapGrpcService<GrpcPartitionsService>();
    app_.MapGrpcService<GrpcVersionsService>();
  }

  public GrpcSubmitterServiceHelper(ISubmitter                  submitter,
                                    Action<IServiceCollection>? serviceConfigurator = null)
    : this(submitter,
           new List<MockIdentity>(),
           AuthenticatorOptions.DefaultNoAuth,
           LogLevel.Trace,
           serviceConfigurator)
  {
  }

  public void Dispose()
  {
    app_.DisposeAsync()
        .WaitSync();
    server_?.Dispose();
    server_ = null;
    handler_?.Dispose();
    handler_ = null;
    channel_?.ShutdownAsync()
            .Wait();
    channel_ = null;
    loggerFactory_.Dispose();
    channelMutex_?.Dispose();
    GC.SuppressFinalize(this);
  }

  public async Task StartServer()
  {
    await app_.StartAsync()
              .ConfigureAwait(false);

    server_  =   app_.GetTestServer();
    handler_ ??= server_.CreateHandler();
  }

  public async Task<ChannelBase> CreateChannel()
  {
    using (channelMutex_)
    {
      if (handler_ is null)
      {
        await StartServer()
          .ConfigureAwait(false);
      }
    }

    return GrpcChannel.ForAddress("http://localhost:9999",
                                  new GrpcChannelOptions
                                  {
                                    LoggerFactory = loggerFactory_,
                                    HttpHandler   = handler_,
                                  });
    ;
  }

  public static async Task DeleteChannel(ChannelBase channel)
    => await channel.ShutdownAsync()
                    .ConfigureAwait(false);

  public async Task StopServer()
  {
    server_?.Dispose();
    await app_.StopAsync()
              .ConfigureAwait(false);
    await app_.DisposeAsync()
              .ConfigureAwait(false);
    handler_?.Dispose();
    handler_ = null;
  }
}
