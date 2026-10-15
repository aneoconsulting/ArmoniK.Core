// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Authorization;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

public class AuthorizationTableTests : AuthorizationTableTestBase
{
  private MongoDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  public override void GetAuthorizationTableInstance()
  {
    tableProvider_ = new MongoDatabaseProvider();
    AuthorizationTable = tableProvider_.GetServiceProvider()
                                       .GetRequiredService<IAuthorizationTable>();
    RunTests = true;
  }

  private AuthorizationTable Table(TimeSpan lockDuration)
  {
    var services = tableProvider_!.GetServiceProvider();
    return new AuthorizationTable(services.GetRequiredService<MongoCollectionProvider<GrantData, GrantDataModelMapping>>(),
                                  services.GetRequiredService<MongoCollectionProvider<PermissionSetData, PermissionSetDataModelMapping>>(),
                                  services.GetRequiredService<MongoCollectionProvider<PermissionSetVersionData, PermissionSetVersionDataModelMapping>>(),
                                  services.GetRequiredService<MongoCollectionProvider<AuthorizationStateData, AuthorizationStateDataModelMapping>>(),
                                  services.GetRequiredService<ActivitySource>())
           {
             LockDuration = lockDuration,
           };
  }

  [Test]
  public async Task ALeaseIsRenewedWhileTheLockIsHeld()
  {
    var table = Table(TimeSpan.FromSeconds(1.5));
    await table.Init(CancellationToken.None)
               .ConfigureAwait(false);

    var first = await table.LockAsync()
                           .ConfigureAwait(false);
    var second = table.LockAsync();

    await Task.Delay(TimeSpan.FromSeconds(4))
              .ConfigureAwait(false);
    Assert.That(second.IsCompleted,
                Is.False);
    Assert.That(first.Lost.IsCancellationRequested,
                Is.False);

    await first.DisposeAsync()
               .ConfigureAwait(false);
    await (await second.WaitAsync(TimeSpan.FromSeconds(10))
                       .ConfigureAwait(false)).DisposeAsync()
                                              .ConfigureAwait(false);
  }

  [Test]
  public async Task ALeaseTakenOverIsReportedLost()
  {
    var table = Table(TimeSpan.FromSeconds(1.5));
    await table.Init(CancellationToken.None)
               .ConfigureAwait(false);

    await using var held = await table.LockAsync()
                                      .ConfigureAwait(false);
    await tableProvider_!.GetServiceProvider()
                         .GetRequiredService<MongoCollectionProvider<AuthorizationStateData, AuthorizationStateDataModelMapping>>()
                         .Get()
                         .UpdateOneAsync(data => data.Id == "lock",
                                         Builders<AuthorizationStateData>.Update.Set(data => data.Owner,
                                                                                     "another replica"))
                         .ConfigureAwait(false);

    Assert.That(() => held.Lost.IsCancellationRequested,
                Is.True.After(5000,
                              50));
  }
}
