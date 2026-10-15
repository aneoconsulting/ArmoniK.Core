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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class AuthorizationTableTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetAuthorizationTableInstance();

    if (!RunTests)
    {
      return;
    }

    await AuthorizationTable!.Init(CancellationToken.None)
                             .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    AuthorizationTable = null;
    RunTests           = false;
  }

  protected IAuthorizationTable? AuthorizationTable;

  protected bool RunTests;

  public virtual void GetAuthorizationTableInstance()
  {
  }

  private static readonly DateTime Date = new(2026,
                                              10,
                                              8,
                                              12,
                                              0,
                                              0,
                                              DateTimeKind.Utc);

  [Test]
  public async Task GrantsRoundTrip()
  {
    if (RunTests)
    {
      var own = new GrantData("own",
                              "role:team",
                              "session-own",
                              "session.owner == request.caller && grant.principal in session.createdUnder",
                              AuthorizationOrigin.Config,
                              Date);
      var admin = new GrantData("admin",
                                "role:platform",
                                "cluster-admin",
                                null,
                                AuthorizationOrigin.Api,
                                Date);

      await AuthorizationTable!.CreateGrantAsync(own)
                               .ConfigureAwait(false);
      await AuthorizationTable.CreateGrantAsync(admin)
                              .ConfigureAwait(false);

      var grants = await AuthorizationTable.ListGrantsAsync()
                                           .ToListAsync()
                                           .ConfigureAwait(false);

      Assert.That(grants,
                  Is.EquivalentTo(new[]
                                  {
                                    own,
                                    admin,
                                  }));
    }
  }

  [Test]
  public async Task AGrantIdentifierIsUnique()
  {
    if (RunTests)
    {
      var grant = new GrantData("g",
                                "role:team",
                                "session-view",
                                null,
                                AuthorizationOrigin.Config,
                                Date);
      await AuthorizationTable!.CreateGrantAsync(grant)
                               .ConfigureAwait(false);

      Assert.That(() => AuthorizationTable.CreateGrantAsync(grant with
                                                            {
                                                              Principal = "role:other",
                                                            }),
                  Throws.InstanceOf<GrantAlreadyExistsException>());
    }
  }

  [Test]
  public async Task DeletingAGrantSaysWhetherItExisted()
  {
    if (RunTests)
    {
      await AuthorizationTable!.CreateGrantAsync(new GrantData("g",
                                                               "role:team",
                                                               "session-view",
                                                               null,
                                                               AuthorizationOrigin.Config,
                                                               Date))
                               .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.DeleteGrantAsync("g")
                                          .ConfigureAwait(false),
                  Is.True);
      Assert.That(await AuthorizationTable.DeleteGrantAsync("g")
                                          .ConfigureAwait(false),
                  Is.False);
      Assert.That(await AuthorizationTable.ListGrantsAsync()
                                          .ToListAsync()
                                          .ConfigureAwait(false),
                  Is.Empty);
    }
  }

  [Test]
  public async Task PermissionSetVersionsAreKept()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("reader",
                                                                            1,
                                                                            new[]
                                                                            {
                                                                              "session:read",
                                                                            },
                                                                            AuthorizationOrigin.Config))
                               .ConfigureAwait(false);
      await AuthorizationTable.SetPermissionSetAsync(new PermissionSetData("reader",
                                                                           2,
                                                                           new[]
                                                                           {
                                                                             "session:read",
                                                                             "data:read",
                                                                           },
                                                                           AuthorizationOrigin.Config))
                              .ConfigureAwait(false);

      var current = await AuthorizationTable.ListPermissionSetsAsync()
                                            .ToListAsync()
                                            .ConfigureAwait(false);
      var first = await AuthorizationTable.GetPermissionSetVersionAsync("reader",
                                                                        1)
                                          .ConfigureAwait(false);

      Assert.That(current.Select(set => (set.Name, set.Version)),
                  Is.EqualTo(new[]
                             {
                               ("reader", 2),
                             }));
      Assert.That(current.Single()
                         .Permissions,
                  Is.EqualTo(new[]
                             {
                               "session:read",
                               "data:read",
                             }));
      Assert.That(first!.Permissions,
                  Is.EqualTo(new[]
                             {
                               "session:read",
                             }));
      Assert.That(await AuthorizationTable.GetPermissionSetVersionAsync("reader",
                                                                        3)
                                          .ConfigureAwait(false),
                  Is.Null);
    }
  }

  [Test]
  public async Task DeletingAPermissionSetKeepsItsHistory()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("reader",
                                                                            1,
                                                                            new[]
                                                                            {
                                                                              "session:read",
                                                                            },
                                                                            AuthorizationOrigin.Api))
                               .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.DeletePermissionSetAsync("reader")
                                          .ConfigureAwait(false),
                  Is.True);
      Assert.That(await AuthorizationTable.DeletePermissionSetAsync("reader")
                                          .ConfigureAwait(false),
                  Is.False);
      Assert.That(await AuthorizationTable.ListPermissionSetsAsync()
                                          .ToListAsync()
                                          .ConfigureAwait(false),
                  Is.Empty);
      Assert.That((await AuthorizationTable.GetPermissionSetVersionAsync("reader",
                                                                         1)
                                           .ConfigureAwait(false))?.Origin,
                  Is.EqualTo(AuthorizationOrigin.Api));
    }
  }

  [Test]
  public async Task TheConfigRevisionIsStored()
  {
    if (RunTests)
    {
      Assert.That(await AuthorizationTable!.GetConfigRevisionAsync()
                                           .ConfigureAwait(false),
                  Is.Zero);

      await AuthorizationTable.SetConfigRevisionAsync(3)
                              .ConfigureAwait(false);
      await AuthorizationTable.SetConfigRevisionAsync(5)
                              .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetConfigRevisionAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(5));
    }
  }

  [Test]
  public async Task AChangeIsUnstableUntilItEnds()
  {
    if (RunTests)
    {
      Assert.That(await AuthorizationTable!.GetGenerationAsync()
                                           .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(0,
                                                         0)));

      var generation = await AuthorizationTable.BeginChangeAsync()
                                               .ConfigureAwait(false);

      Assert.That(generation,
                  Is.EqualTo(1));
      Assert.That((await AuthorizationTable.GetGenerationAsync()
                                           .ConfigureAwait(false)).Stable,
                  Is.False);

      await AuthorizationTable.EndChangeAsync(generation)
                              .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetGenerationAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(1,
                                                         1)));
    }
  }

  [Test]
  public async Task AChangeDoesNotEndOneThatBeganAfterIt()
  {
    if (RunTests)
    {
      var first = await AuthorizationTable!.BeginChangeAsync()
                                           .ConfigureAwait(false);
      var second = await AuthorizationTable.BeginChangeAsync()
                                           .ConfigureAwait(false);

      await AuthorizationTable.EndChangeAsync(first)
                              .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetGenerationAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(2,
                                                         0)));

      await AuthorizationTable.EndChangeAsync(second)
                              .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetGenerationAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(2,
                                                         2)));
    }
  }

  [Test]
  public async Task TheConfigRevisionNeverGoesBack()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetConfigRevisionAsync(5)
                               .ConfigureAwait(false);
      await AuthorizationTable.SetConfigRevisionAsync(3)
                              .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetConfigRevisionAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(5));
    }
  }

  [Test]
  public async Task TheLockIsExclusive()
  {
    if (RunTests)
    {
      var first = await AuthorizationTable!.LockAsync()
                                           .ConfigureAwait(false);
      var second = AuthorizationTable.LockAsync();

      await Task.Delay(TimeSpan.FromMilliseconds(500))
                .ConfigureAwait(false);
      Assert.That(second.IsCompleted,
                  Is.False);

      await first.DisposeAsync()
                 .ConfigureAwait(false);

      var acquired = await second.WaitAsync(TimeSpan.FromSeconds(10))
                                 .ConfigureAwait(false);
      await acquired.DisposeAsync()
                    .ConfigureAwait(false);

      // Released, so it can be taken again
      await (await AuthorizationTable.LockAsync()
                                     .WaitAsync(TimeSpan.FromSeconds(10))
                                     .ConfigureAwait(false)).DisposeAsync()
                                                            .ConfigureAwait(false);
    }
  }
}
