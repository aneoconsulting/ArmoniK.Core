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

using System.Diagnostics.Metrics;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.HealthChecks;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Tests.Helpers;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class GrpcHealthChecksServiceTest
{
  private class MyFactory : IMeterFactory
  {
    public void Dispose()
    {
    }

    public System.Diagnostics.Metrics.Meter Create(MeterOptions options)
      => new("");
  }


  [Test]
  public async Task CheckHealthShouldSucceed()
  {
    var taskTable     = new SimpleTaskTable();
    var objectStorage = new SimpleObjectStorage();
    var queueStorage  = new SimplePushQueueStorage();
    var service = new GrpcHealthChecksService(taskTable,
                                              objectStorage,
                                              queueStorage,
                                              new FunctionExecutionMetricsFactory(new MyFactory(),
                                                                                  new AgentIdentifier()));
    Assert.IsNotNull(service);
    var response = await service.CheckHealth(new CheckHealthRequest(),
                                             TestServerCallContext.Create())
                                .ConfigureAwait(false);
    Assert.IsNotNull(response);
    foreach (var serviceHealth in response.Services)
    {
      Assert.AreEqual(HealthStatusEnum.Healthy,
                      serviceHealth.Healthy);
    }
  }
}
