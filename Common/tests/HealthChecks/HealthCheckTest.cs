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

using System.Collections;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Diagnostics.HealthChecks;

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
}
