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

using System.Net;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.HealthChecks;

[TestFixture]
public class HowHealthCheckWorkTest
{
  [SetUp]
  public void SetUp()
  {
    var loggerFactory = new LoggerFactory();
    loggerFactory.AddProvider(new ConsoleForwardingLoggerProvider(LogLevel.Information));

    var builder = WebApplication.CreateBuilder();
    builder.Services.AddSingleton(loggerFactory)
           .AddHealthChecks()
           .Add(new HealthCheckRegistration(nameof(AlwaysTrueHealthCheck),
                                            _ => new HealthCheck(new AlwaysTrueHealthCheck(),
                                                                 HealthCheckTag.Startup),
                                            HealthStatus.Unhealthy,
                                            new[]
                                            {
                                              nameof(HealthCheckTag.Startup),
                                            }))
           .Add(new HealthCheckRegistration(nameof(AlwaysFalseHealthCheck),
                                            _ => new HealthCheck(new AlwaysFalseHealthCheck(),
                                                                 HealthCheckTag.Liveness),
                                            HealthStatus.Unhealthy,
                                            new[]
                                            {
                                              nameof(HealthCheckTag.Liveness),
                                            }));

    builder.WebHost.UseTestServer();
    app_ = builder.Build();
    app_.UseRouting();

    app_.MapHealthChecks("/startup",
                         new HealthCheckOptions
                         {
                           Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Startup)),
                         });

    app_.MapHealthChecks("/liveness",
                         new HealthCheckOptions
                         {
                           Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Liveness)),
                         });
  }

  [TearDown]
  public virtual async Task TearDown()
  {
    server_?.Dispose();
    server_ = null;

    if (app_ != null)
    {
      await app_.DisposeAsync()
                .ConfigureAwait(false);
    }

    app_ = null;
  }

  private WebApplication? app_;
  private TestServer?     server_;

  private class AlwaysTrueHealthCheck : IHealthCheckProvider
  {
    public ValueTask<bool> Check(HealthCheckTag tag)
      => new(true);
  }

  private class AlwaysFalseHealthCheck : IHealthCheckProvider
  {
    public ValueTask<bool> Check(HealthCheckTag tag)
      => new(false);
  }


  [Test]
  public async Task CallHealthChecks()
  {
    await app_!.StartAsync()
               .ConfigureAwait(false);

    server_ = app_.GetTestServer();
    var client = server_.CreateClient();

    Assert.AreEqual("Healthy",
                    await client.GetStringAsync("/startup")
                                .ConfigureAwait(false));

    Assert.AreEqual(HttpStatusCode.ServiceUnavailable,
                    (await client.GetAsync("/liveness")
                                 .ConfigureAwait(false)).StatusCode);
  }
}
