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
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Tests.Auth;

using Grpc.Net.Client;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

using Moq;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class GrpcSubmitterServiceHelper : IDisposable
{
  private readonly WebApplication     app_;
  [CanBeNull]
  private          TestServer         server_;
  [CanBeNull]
  private          HttpMessageHandler handler_;
  private readonly ILoggerFactory     loggerFactory_;
  [CanBeNull]
  private          GrpcChannel        channel_;

  public GrpcSubmitterServiceHelper(ISubmitter submitter, List<MockIdentity> authIdentities, AuthenticatorOptions authOptions)
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    var builder = WebApplication.CreateBuilder();

    builder.Services.AddSingleton(loggerFactory_)
           .AddSingleton(submitter)
           .AddSingleton(loggerFactory_.CreateLogger<GrpcSubmitterService>())
           .AddTransient<IAuthenticationTable, MockAuthenticationTable>(_ => new MockAuthenticationTable(authIdentities))
           .Configure<AuthenticatorOptions>(o=> o.CopyFrom(authOptions))
           .AddAuthentication()
           .AddScheme<AuthenticatorOptions, Authenticator>(Authenticator.SchemeName, _ => {});
    builder.Services.AddLogging()
           .AddSingleton<IAuthorizationPolicyProvider, AuthorizationPolicyProvider>()
           .AddAuthorization()
           .ValidateGrpcRequests()
           .AddGrpc();

    builder.WebHost.UseTestServer();

    app_ = builder.Build();
    app_.UseRouting();
    app_.UseAuthentication();
    app_.UseAuthorization();
    app_.MapGrpcService<GrpcSubmitterService>();
  }

  public GrpcSubmitterServiceHelper(ISubmitter submitter)
    : this(submitter, new List<MockIdentity>(),
           AuthenticatorOptions.DefaultNoAuth)
  {

  }

  public async Task StartServer()
  {
    await app_.StartAsync()
              .ConfigureAwait(false);

    server_  = app_.GetTestServer();
    handler_ ??= server_.CreateHandler();
   
  }

  public async Task<GrpcChannel> CreateChannel()
  {

    if (handler_ == null)
    {
      await StartServer()
        .ConfigureAwait(false);
    }

    channel_ = GrpcChannel.ForAddress("http://localhost",
                                      new GrpcChannelOptions
                                      {
                                        LoggerFactory = loggerFactory_,
                                        HttpHandler   = handler_,
                                      });

    return channel_;
  }

  public async Task DeleteChannel()
  {
    if (channel_ == null)
      return;
    await channel_.ShutdownAsync()
                  .ConfigureAwait(false);
    channel_.Dispose();
    channel_ = null;
  }

  public async Task StopServer()
  {
    server_?.Dispose();
    await app_.StopAsync()
              .ConfigureAwait(false);
    await app_.DisposeAsync().ConfigureAwait(false);
    handler_?.Dispose();
    handler_ = null;
  }

  public void Dispose()
  {
    app_.DisposeAsync().GetAwaiter().GetResult();
    server_?.Dispose();
    server_ = null;
    handler_?.Dispose();
    handler_ = null;
    channel_?.Dispose();
    channel_ = null;
    GC.SuppressFinalize(this);
  }
}
