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
using System.Collections;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Injection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.HealthChecks;

[TestFixture]
public class HealthCheckTest
{
  private class AlwaysFalseHealthCheck : IHealthCheckProvider
  {
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Unhealthy());
  }

  private class ReturnHealthCheck : IHealthCheckProvider
  {
    private readonly HealthCheckResult healthCheckResult_;

    public ReturnHealthCheck(HealthCheckResult healthCheckResult)
      => healthCheckResult_ = healthCheckResult;

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(healthCheckResult_);
  }

  [Test]
  [TestCase(HealthStatus.Unhealthy)]
  [TestCase(HealthStatus.Degraded)]
  [TestCase(HealthStatus.Healthy)]
  public async Task FailureStatusShouldSucceed(HealthStatus healthStatus)
  {
    var hc = new HealthCheck(new AlwaysFalseHealthCheck(),
                             HealthCheckTag.Liveness);

    var hcr = new HealthCheckRegistration(nameof(AlwaysFalseHealthCheck),
                                          _ => hc,
                                          healthStatus,
                                          new[]
                                          {
                                            nameof(HealthCheckTag.Liveness),
                                          });

    var checkResult = await hc.CheckHealthAsync(new HealthCheckContext
                                                {
                                                  Registration = hcr,
                                                },
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    Assert.AreEqual(healthStatus,
                    checkResult.Status);
  }

  public static IEnumerable TestCases
  {
    get
    {
      yield return new TestCaseData(HealthCheckResult.Healthy()).SetArgDisplayNames("Healthy");
      yield return new TestCaseData(HealthCheckResult.Degraded()).SetArgDisplayNames("Degraded");
    }
  }


  [Test]
  [TestCaseSource(nameof(TestCases))]
  public async Task ReturnSameShouldSucceed(HealthCheckResult healthCheckResultInput)
  {
    var hc = new HealthCheck(new ReturnHealthCheck(healthCheckResultInput),
                             HealthCheckTag.Liveness);

    var hcr = new HealthCheckRegistration(nameof(AlwaysFalseHealthCheck),
                                          _ => hc,
                                          HealthStatus.Healthy,
                                          new[]
                                          {
                                            nameof(HealthCheckTag.Liveness),
                                          });

    var checkResult = await hc.CheckHealthAsync(new HealthCheckContext
                                                {
                                                  Registration = hcr,
                                                },
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    Assert.AreEqual(healthCheckResultInput,
                    checkResult);
  }

  private class TestHealthCheck : IInitializable
  {
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => tag switch
         {
           HealthCheckTag.Startup   => Task.FromResult(HealthCheckResult.Healthy()),
           HealthCheckTag.Liveness  => Task.FromResult(HealthCheckResult.Degraded()),
           HealthCheckTag.Readiness => Task.FromResult(HealthCheckResult.Unhealthy()),
           _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                      tag,
                                                      null),
         };

    public Task Init(CancellationToken cancellationToken)
      => throw new NotImplementedException();
  }

  public static IEnumerable TestAddWithHealthCheck
  {
    get
    {
      yield return new TestCaseData(new Action<IServiceCollection>(collection => collection.AddTransientWithHealthCheck<TestHealthCheck>(nameof(TestHealthCheck))))
        .SetArgDisplayNames("Transient");
      yield return new TestCaseData(new Action<IServiceCollection>(collection
                                                                     => collection
                                                                       .AddTransientWithHealthCheck<IInitializable, TestHealthCheck>(nameof(TestHealthCheck))))
        .SetArgDisplayNames("ITransient");

      yield return new TestCaseData(new Action<IServiceCollection>(collection => collection.AddSingletonWithHealthCheck<TestHealthCheck>(nameof(TestHealthCheck))))
        .SetArgDisplayNames("Singleton");
      yield return new TestCaseData(new Action<IServiceCollection>(collection
                                                                     => collection
                                                                       .AddSingletonWithHealthCheck<IInitializable, TestHealthCheck>(nameof(TestHealthCheck))))
        .SetArgDisplayNames("ISingleton");
    }
  }


  [Test]
  [TestCaseSource(nameof(TestAddWithHealthCheck))]
  public async Task AddWithHealthCheckShouldSucceed(Action<IServiceCollection> configurator)
  {
    var serviceBuilder = new ServiceCollection();

    serviceBuilder.AddLogging(builder => builder.AddConsole()
                                                .SetMinimumLevel(LogLevel.Debug))
                  .AddHealthChecks();

    configurator.Invoke(serviceBuilder);

    var provider = serviceBuilder.BuildServiceProvider();

    var healthCheckService = provider.GetRequiredService<HealthCheckService>();

    Assert.AreEqual(HealthStatus.Degraded,
                    (await healthCheckService.CheckHealthAsync(registration => registration.Tags.Contains(HealthCheckTag.Liveness.ToString()))
                                             .ConfigureAwait(false)).Status);

    Assert.AreEqual(HealthStatus.Healthy,
                    (await healthCheckService.CheckHealthAsync(registration => registration.Tags.Contains(HealthCheckTag.Startup.ToString()))
                                             .ConfigureAwait(false)).Status);

    Assert.AreEqual(HealthStatus.Unhealthy,
                    (await healthCheckService.CheckHealthAsync(registration => registration.Tags.Contains(HealthCheckTag.Readiness.ToString()))
                                             .ConfigureAwait(false)).Status);
  }
}
