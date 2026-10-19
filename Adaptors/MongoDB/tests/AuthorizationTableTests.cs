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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Authorization;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Bson;
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
                                  services.GetRequiredService<ActivitySource>(),
                                  NullLogger<AuthorizationTable>.Instance)
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

  [Test]
  public async Task ALeaseThatCannotBeRenewedIsReportedLostBeforeItEnds()
  {
    var table = Table(TimeSpan.FromSeconds(6));
    await table.Init(CancellationToken.None)
               .ConfigureAwait(false);
    var state = tableProvider_!.GetServiceProvider()
                               .GetRequiredService<MongoCollectionProvider<AuthorizationStateData, AuthorizationStateDataModelMapping>>()
                               .Get();

    await using var held = await table.LockAsync()
                                      .ConfigureAwait(false);
    // Every renewal is refused, while no other replica takes the lock
    await state.Database.RunCommandAsync<BsonDocument>(new BsonDocument
                                                       {
                                                         {
                                                           "collMod", state.CollectionNamespace.CollectionName
                                                         },
                                                         {
                                                           "validator", new BsonDocument("Owner",
                                                                                         "nobody")
                                                         },
                                                       })
               .ConfigureAwait(false);
    try
    {
      Assert.That(() => held.Lost.IsCancellationRequested,
                  Is.True.After(10000,
                                20));

      // Reported lost while the lease still holds on the server, so that no write starts once another replica can take
      // the lock over
      Assert.That(await state.CountDocumentsAsync(new BsonDocument
                                                  {
                                                    {
                                                      "_id", "lock"
                                                    },
                                                    {
                                                      "$expr", new BsonDocument("$gt",
                                                                                new BsonArray
                                                                                {
                                                                                  "$" + nameof(AuthorizationStateData.ExpiresAt),
                                                                                  "$$NOW",
                                                                                })
                                                    },
                                                  })
                             .ConfigureAwait(false),
                  Is.EqualTo(1));
    }
    finally
    {
      await state.Database.RunCommandAsync<BsonDocument>(new BsonDocument
                                                         {
                                                           {
                                                             "collMod", state.CollectionNamespace.CollectionName
                                                           },
                                                           {
                                                             "validator", new BsonDocument()
                                                           },
                                                         })
                 .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task IncompleteDocumentsAreReadAndRefusedAlone()
  {
    var services = tableProvider_!.GetServiceProvider();
    var grants = services.GetRequiredService<MongoCollectionProvider<GrantData, GrantDataModelMapping>>()
                         .Get();
    var sets = services.GetRequiredService<MongoCollectionProvider<PermissionSetData, PermissionSetDataModelMapping>>()
                       .Get();

    // Written by hand or by a faulty writer: no creation date, no origin, no permission list
    await grants.Database.GetCollection<BsonDocument>(grants.CollectionNamespace.CollectionName)
                .InsertManyAsync([
                  new BsonDocument
                  {
                    {
                      "_id", "undated"
                    },
                    {
                      "Principal", "role:ops"
                    },
                    {
                      "PermissionSet", "ops"
                    },
                    {
                      "Origin", (int)AuthorizationOrigin.Api
                    },
                  },
                  new BsonDocument
                  {
                    {
                      "_id", "unowned"
                    },
                    {
                      "Principal", "role:ops"
                    },
                    {
                      "PermissionSet", "viewer"
                    },
                    {
                      "CreationDate", DateTime.UtcNow
                    },
                  },
                ])
                .ConfigureAwait(false);
    await sets.Database.GetCollection<BsonDocument>(sets.CollectionNamespace.CollectionName)
              .InsertOneAsync(new BsonDocument
                              {
                                {
                                  "_id", "ops"
                                },
                                {
                                  "Version", 1
                                },
                                {
                                  "Origin", (int)AuthorizationOrigin.Api
                                },
                              })
              .ConfigureAwait(false);

    var listed = await AuthorizationTable!.ListGrantsAsync()
                                          .ToDictionaryAsync(grant => grant.GrantId)
                                          .ConfigureAwait(false);
    Assert.That(listed.Keys,
                Is.EquivalentTo(new[]
                                {
                                  "undated",
                                  "unowned",
                                }));
    // Never deleted nor rewritten by the reconciliation
    Assert.That(listed["unowned"].Origin,
                Is.EqualTo(AuthorizationOrigin.Api));
    Assert.That((await AuthorizationTable.ListPermissionSetsAsync()
                                         .SingleAsync()
                                         .ConfigureAwait(false)).Permissions,
                Is.Null);

    // The grant naming the incomplete set is refused alone: the configuration is applied
    Assert.That(await new AuthorizationReconciler(AuthorizationTable,
                                                  new AuthorizationOptions
                                                  {
                                                    Enabled        = true,
                                                    ConfigRevision = 1,
                                                    PermissionSets =
                                                    [
                                                      new PermissionSetOption
                                                      {
                                                        Name        = "viewer",
                                                        Permissions = ["session:read"],
                                                      },
                                                    ],
                                                    Grants =
                                                    [
                                                      new GrantOption
                                                      {
                                                        Id            = "reader",
                                                        Principal     = "role:team",
                                                        PermissionSet = "viewer",
                                                      },
                                                    ],
                                                  },
                                                  new AuthorizationAudit(NullLoggerFactory.Instance),
                                                  TimeProvider.System,
                                                  NullLogger<AuthorizationReconciler>.Instance).ReconcileAsync()
                                                                                               .ConfigureAwait(false),
                Is.True);
    Assert.That(await AuthorizationTable.ListGrantsAsync()
                                        .Select(grant => grant.GrantId)
                                        .ToListAsync()
                                        .ConfigureAwait(false),
                Is.EquivalentTo(new[]
                                {
                                  "undated",
                                  "unowned",
                                  "reader",
                                }));
  }
}
