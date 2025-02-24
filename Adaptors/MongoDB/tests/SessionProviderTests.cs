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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class SessionProviderTests
{
  [SetUp]
  public void SetUp()
  {
    dbProvider_ = new MongoDatabaseProvider();
    provider_   = dbProvider_.GetServiceProvider();
  }

  [TearDown]
  public void TearDown()
    => dbProvider_?.Dispose();

  private IServiceProvider?      provider_;
  private MongoDatabaseProvider? dbProvider_;


  [Test]
  public async Task SessionProviderInitShouldSucceed()
  {
    var sessionProvider = provider_!.GetRequiredService<SessionProvider>();

    Assert.NotNull(sessionProvider);

    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider!.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);

    await sessionProvider.Init(CancellationToken.None)
                         .ConfigureAwait(false);

    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Readiness)
                                          .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Startup)
                                          .ConfigureAwait(false)).Status);

    Assert.NotNull(sessionProvider.Get());
  }

  [Test]
  public void SessionProviderGetBeforeInitShouldThrow()
  {
    var sessionProvider = provider_!.GetRequiredService<SessionProvider>();

    Assert.NotNull(sessionProvider);

    Assert.Throws<NullReferenceException>(() => sessionProvider.Get());
  }
}
