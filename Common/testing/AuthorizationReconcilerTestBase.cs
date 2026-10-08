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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

/// <summary>
///   Reconciliation of the stored grants with the configuration, on each storage adaptor
/// </summary>
[TestFixture]
public class AuthorizationReconcilerTestBase
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

  private const string Ownership = "session.owner == request.caller && grant.principal in session.createdUnder";

  private static AuthorizationOptions Options(int                          revision,
                                              IEnumerable<GrantOption>?   grants = null,
                                              IEnumerable<PermissionSetOption>? sets   = null)
    => new()
       {
         Enabled        = true,
         ConfigRevision = revision,
         Grants         = (grants ?? []).ToList(),
         PermissionSets = (sets   ?? []).ToList(),
       };

  private static GrantOption Grant(string  id,
                                   string  principal,
                                   string  set,
                                   string? condition = null)
    => new()
       {
         Id            = id,
         Principal     = principal,
         PermissionSet = set,
         Condition     = condition,
       };

  private Task<bool> ReconcileAsync(AuthorizationOptions options)
    => new AuthorizationReconciler(AuthorizationTable!,
                                   options,
                                   new AuthorizationAudit(NullLoggerFactory.Instance),
                                   TimeProvider.System,
                                   NullLogger<AuthorizationReconciler>.Instance).ReconcileAsync();

  private async Task<Dictionary<string, GrantData>> GrantsAsync()
    => await AuthorizationTable!.ListGrantsAsync()
                                .ToDictionaryAsync(grant => grant.GrantId)
                                .ConfigureAwait(false);

  private async Task<Dictionary<string, PermissionSetData>> SetsAsync()
    => await AuthorizationTable!.ListPermissionSetsAsync()
                                .ToDictionaryAsync(set => set.Name)
                                .ConfigureAwait(false);

  [Test]
  public async Task TheConfigurationIsStored()
  {
    if (RunTests)
    {
      Assert.That(await ReconcileAsync(Options(1,
                                               [Grant("own", "role:team", "session-own", Ownership)],
                                               [
                                                 new PermissionSetOption
                                                 {
                                                   Name        = "team-reader",
                                                   Permissions = ["data:read", "data:download"],
                                                 },
                                               ]))
                    .ConfigureAwait(false),
                  Is.True);

      var grants = await GrantsAsync()
                     .ConfigureAwait(false);
      var sets = await SetsAsync()
                   .ConfigureAwait(false);

      Assert.That(grants["own"].Condition,
                  Is.EqualTo(Ownership));
      Assert.That(grants["own"].Origin,
                  Is.EqualTo(AuthorizationOrigin.Config));
      Assert.That(sets.Keys,
                  Is.SupersetOf(new[]
                                {
                                  "session-view",
                                  "session-create",
                                  "session-own",
                                  "session-supervise",
                                  "access-admin",
                                  "cluster-admin",
                                  "team-reader",
                                }));
      Assert.That(sets.Values.Select(set => set.Version),
                  Is.All.EqualTo(1));
      Assert.That(await AuthorizationTable!.GetConfigRevisionAsync()
                                           .ConfigureAwait(false),
                  Is.EqualTo(1));
    }
  }

  [Test]
  public async Task ReconcilingTwiceChangesNothing()
  {
    if (RunTests)
    {
      var options = Options(1,
                            [Grant("own", "role:team", "session-own", Ownership)]);
      await ReconcileAsync(options)
        .ConfigureAwait(false);
      var grants = await GrantsAsync()
                     .ConfigureAwait(false);

      await ReconcileAsync(options)
        .ConfigureAwait(false);

      Assert.That(await GrantsAsync()
                    .ConfigureAwait(false),
                  Is.EqualTo(grants));
      Assert.That((await SetsAsync()
                     .ConfigureAwait(false)).Values.Select(set => set.Version),
                  Is.All.EqualTo(1));
    }
  }

  [Test]
  public async Task AChangedSetGetsANewVersionAndKeepsTheOldOne()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(1))
        .ConfigureAwait(false);
      await ReconcileAsync(Options(1,
                                   sets:
                                   [
                                     new PermissionSetOption
                                     {
                                       Name        = "session-view",
                                       Permissions = ["session:read"],
                                     },
                                   ]))
        .ConfigureAwait(false);

      var sets = await SetsAsync()
                   .ConfigureAwait(false);
      var first = await AuthorizationTable!.GetPermissionSetVersionAsync("session-view",
                                                                         1)
                                           .ConfigureAwait(false);

      Assert.That(sets["session-view"].Version,
                  Is.EqualTo(2));
      Assert.That(sets["session-view"].Permissions,
                  Is.EqualTo(new[]
                             {
                               "session:read",
                             }));
      Assert.That(first!.Permissions,
                  Has.Count.EqualTo(5));
    }
  }

  [Test]
  public async Task GrantsRemovedFromTheConfigurationAreRevokedAndApiGrantsAreKept()
  {
    if (RunTests)
    {
      await AuthorizationTable!.CreateGrantAsync(new GrantData("api",
                                                               "role:ops",
                                                               "session-view",
                                                               null,
                                                               AuthorizationOrigin.Api,
                                                               DateTime.UtcNow))
                               .ConfigureAwait(false);
      await ReconcileAsync(Options(1,
                                   [
                                     Grant("own", "role:team", "session-own", Ownership),
                                     Grant("view", "role:team", "session-view"),
                                   ]))
        .ConfigureAwait(false);

      await ReconcileAsync(Options(2,
                                   [Grant("own", "role:team", "session-own", "session.owner == request.caller")]))
        .ConfigureAwait(false);

      var grants = await GrantsAsync()
                     .ConfigureAwait(false);

      Assert.That(grants.Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "api",
                                    "own",
                                  }));
      Assert.That(grants["own"].Condition,
                  Is.EqualTo("session.owner == request.caller"));
    }
  }

  [Test]
  public async Task AnOlderConfigurationChangesNothing()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(2,
                                   [Grant("own", "role:team", "session-own", Ownership)]))
        .ConfigureAwait(false);

      Assert.That(await ReconcileAsync(Options(1,
                                               [
                                                 Grant("own", "role:team", "session-own", Ownership),
                                                 Grant("revoked", "role:team", "cluster-admin"),
                                               ]))
                    .ConfigureAwait(false),
                  Is.False);
      Assert.That((await GrantsAsync()
                     .ConfigureAwait(false)).Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "own",
                                  }));
    }
  }

  [Test]
  public async Task ARefusedConfigurationChangesNothing()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(1,
                                   [Grant("own", "role:team", "session-own", Ownership)]))
        .ConfigureAwait(false);

      Assert.That(() => ReconcileAsync(Options(2,
                                               [
                                                 Grant("own2", "role:team", "session-own", Ownership),
                                                 Grant("broken", "role:team", "session-create", "session.owner == request.caller"),
                                               ])),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("broken"));
      Assert.That((await GrantsAsync()
                     .ConfigureAwait(false)).Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "own",
                                  }));
      Assert.That(await AuthorizationTable!.GetConfigRevisionAsync()
                                           .ConfigureAwait(false),
                  Is.EqualTo(1));
    }
  }

  [Test]
  public async Task ASetStillNamedByAnApiGrantCannotBeRemoved()
  {
    if (RunTests)
    {
      var custom = new PermissionSetOption
                   {
                     Name        = "team-reader",
                     Permissions = ["data:read"],
                   };
      await ReconcileAsync(Options(1,
                                   sets: [custom]))
        .ConfigureAwait(false);
      await AuthorizationTable!.CreateGrantAsync(new GrantData("api",
                                                               "role:lead",
                                                               "team-reader",
                                                               null,
                                                               AuthorizationOrigin.Api,
                                                               DateTime.UtcNow))
                               .ConfigureAwait(false);

      Assert.That(() => ReconcileAsync(Options(2)),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("api"));
      Assert.That((await SetsAsync()
                     .ConfigureAwait(false)).Keys,
                  Does.Contain("team-reader"));
    }
  }

  [Test]
  public async Task ConcurrentReconciliationsAgree()
  {
    if (RunTests)
    {
      var options = Options(1,
                            [
                              Grant("own", "role:team", "session-own", Ownership),
                              Grant("create", "role:team", "session-create", "request.partition == \"q\""),
                            ]);

      await Task.WhenAll(Enumerable.Range(0,
                                          5)
                                   .Select(_ => ReconcileAsync(options)))
                .ConfigureAwait(false);

      Assert.That((await GrantsAsync()
                     .ConfigureAwait(false)).Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "own",
                                    "create",
                                  }));
      Assert.That((await SetsAsync()
                     .ConfigureAwait(false)).Values.Select(set => set.Version),
                  Is.All.EqualTo(1));
    }
  }
}
