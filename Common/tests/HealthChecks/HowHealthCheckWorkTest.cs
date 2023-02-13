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
                                            }))
           .AddCheck("Check from add check",
                     () => HealthCheckResult.Healthy(),
                     new[]
                     {
                       nameof(HealthCheckTag.Readiness),
                     })
           .AddCheck("Check from add check2",
                     () => HealthCheckResult.Unhealthy("test"),
                     new[]
                     {
                       nameof(HealthCheckTag.Readiness),
                     });

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

    app_.MapHealthChecks("/readiness",
                         new HealthCheckOptions
                         {
                           Predicate = check => check.Tags.Contains(nameof(HealthCheckTag.Readiness)),
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
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Healthy());
  }

  private class AlwaysFalseHealthCheck : IHealthCheckProvider
  {
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Unhealthy());
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

  [Test]
  public async Task CallHealthChecksFromAddCheck()
  {
    await app_!.StartAsync()
               .ConfigureAwait(false);

    server_ = app_.GetTestServer();
    var client = server_.CreateClient();

    Assert.AreEqual(HttpStatusCode.ServiceUnavailable,
                    (await client.GetAsync("/readiness")
                                 .ConfigureAwait(false)).StatusCode);
  }
}
