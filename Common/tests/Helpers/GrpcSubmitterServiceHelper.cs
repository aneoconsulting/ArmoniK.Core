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
  private          TestServer         server_;
  private          HttpMessageHandler handler_;
  private readonly ILoggerFactory     loggerFactory_;
  private          GrpcChannel        channel_;

  public GrpcSubmitterServiceHelper(ISubmitter submitter)
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    var builder = WebApplication.CreateBuilder();

    builder.Services.AddSingleton(loggerFactory_)
           .AddSingleton(submitter)
           .AddSingleton(loggerFactory_.CreateLogger<GrpcSubmitterService>())
           .AddTransient<IAuthenticationTable, MockAuthenticationTable>(_ => new MockAuthenticationTable(new List<MockIdentity>()))
           .Configure<AuthenticatorOptions>(o =>
                                            {
                                              o.Bypass            = true;
                                            })
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

  public async Task<GrpcChannel> CreateChannel()
  {    

    
    await app_.StartAsync()
             .ConfigureAwait(false);

    server_  = app_.GetTestServer();
    handler_ = server_.CreateHandler();

    channel_ = GrpcChannel.ForAddress("http://localhost",
                                         new GrpcChannelOptions
                                         {
                                           LoggerFactory = loggerFactory_,
                                           HttpHandler   = handler_,
                                         });

    return channel_;
  }

  public async Task StopServer()
  {
    await app_.StopAsync()
              .ConfigureAwait(false);
  }

  public void Dispose()
  {
    app_.DisposeAsync().GetAwaiter().GetResult();
    server_.Dispose();
    handler_.Dispose();
    channel_.Dispose();
    GC.SuppressFinalize(this);
  }
}