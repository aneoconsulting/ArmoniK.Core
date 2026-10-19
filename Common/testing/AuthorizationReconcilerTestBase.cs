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

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
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

  private static AuthorizationOptions Options(int                               revision,
                                              IEnumerable<GrantOption>?         grants = null,
                                              IEnumerable<PermissionSetOption>? sets   = null)
    => new()
       {
         Authentication = CallerAuthentication.Xfcc,
         ConfigRevision = revision,
         Grants         = (grants ?? []).ToList(),
         // The sets of the access model, as the deployment defines them, some redefined by the test
         PermissionSets = ExamplePermissionSets.Options.Where(set => (sets ?? []).All(redefined => redefined.Name != set.Name))
                                               .Concat(sets ?? [])
                                               .ToList(),
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

  private Task<bool> ReconcileAsync(AuthorizationOptions options,
                                    ILoggerFactory?      audit = null)
    => new AuthorizationReconciler(AuthorizationTable!,
                                   options,
                                   new AuthorizationAudit(audit ?? NullLoggerFactory.Instance),
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

  // Whether a change that fails under the lock of the table leaves nothing written
  private async Task<bool> AtomicAsync()
  {
    await using var held = await AuthorizationTable!.LockAsync()
                                                    .ConfigureAwait(false);
    return held.Atomic;
  }

  // A failed change of a table whose lock is atomic left the stored state as it was
  private async Task AssertNothingWrittenAsync(int             revision,
                                               params string[] grants)
  {
    Assert.That(await AuthorizationTable!.GetConfigRevisionAsync()
                                         .ConfigureAwait(false),
                Is.EqualTo(revision));
    Assert.That((await AuthorizationTable.GetGenerationAsync()
                                         .ConfigureAwait(false)).Stable,
                Is.True);
    Assert.That((await GrantsAsync()
                   .ConfigureAwait(false)).Keys,
                Is.EquivalentTo(grants));
  }

  [Test]
  public async Task TheConfigurationIsStored()
  {
    if (RunTests)
    {
      Assert.That(await ReconcileAsync(Options(1,
                                               [
                                                 Grant("own",
                                                       "role:team",
                                                       "session-own",
                                                       Ownership),
                                               ],
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
      [
        Grant("own",
              "role:team",
              "session-own",
              Ownership),
      ]);
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
  public async Task AReconciliationIsOneChange()
  {
    if (RunTests)
    {
      var options = Options(1,
      [
        Grant("own",
              "role:team",
              "session-own",
              Ownership),
      ]);
      await ReconcileAsync(options)
        .ConfigureAwait(false);

      Assert.That(await AuthorizationTable!.GetGenerationAsync()
                                           .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(1,
                                                         1)));

      // Nothing to write, no change
      await ReconcileAsync(options)
        .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetGenerationAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(1,
                                                         1)));
    }
  }

  [Test]
  public async Task AChangeLeftUnendedIsEndedByTheNextReconciliation()
  {
    if (RunTests)
    {
      var options = Options(1,
      [
        Grant("own",
              "role:team",
              "session-own",
              Ownership),
      ]);
      await ReconcileAsync(options)
        .ConfigureAwait(false);

      // A writer that failed between its writes
      await AuthorizationTable!.BeginChangeAsync()
                               .ConfigureAwait(false);

      await ReconcileAsync(options)
        .ConfigureAwait(false);

      Assert.That(await AuthorizationTable.GetGenerationAsync()
                                          .ConfigureAwait(false),
                  Is.EqualTo(new AuthorizationGeneration(3,
                                                         3)));
    }
  }

  [Test]
  public async Task AnOlderConfigurationCannotEndAChangeLeftUnended()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(2,
        [
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
        ]))
        .ConfigureAwait(false);

      // A writer that failed between its writes
      await AuthorizationTable!.BeginChangeAsync()
                               .ConfigureAwait(false);

      Assert.That(() => ReconcileAsync(Options(1)),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("revision 2 or later"));
      Assert.That((await AuthorizationTable.GetGenerationAsync()
                                           .ConfigureAwait(false)).Stable,
                  Is.False);
    }
  }

  [Test]
  public async Task ASetAddedBackAfterItsDeletionGetsTheNextVersion()
  {
    if (RunTests)
    {
      PermissionSetOption Reader(params string[] permissions)
        => new()
           {
             Name        = "team-reader",
             Permissions = permissions.ToList(),
           };

      await ReconcileAsync(Options(1,
                                   sets: [Reader("data:read")]))
        .ConfigureAwait(false);
      await ReconcileAsync(Options(2))
        .ConfigureAwait(false);
      await ReconcileAsync(Options(3,
                                   sets:
                                   [
                                     Reader("data:read",
                                            "data:delete"),
                                   ]))
        .ConfigureAwait(false);

      Assert.That((await SetsAsync()
                     .ConfigureAwait(false))["team-reader"].Version,
                  Is.EqualTo(2));
      Assert.That((await AuthorizationTable!.GetPermissionSetVersionAsync("team-reader",
                                                                          1)
                                            .ConfigureAwait(false))!.Permissions,
                  Is.EquivalentTo(new[]
                                  {
                                    "data:read",
                                  }));
    }
  }

  [Test]
  public async Task AVersionAFailedChangeLeftBehindDoesNotBlockTheNextChange()
  {
    if (RunTests)
    {
      PermissionSetOption Reader(params string[] permissions)
        => new()
           {
             Name        = "team-reader",
             Permissions = permissions.ToList(),
           };

      await ReconcileAsync(Options(1,
                                   sets: [Reader("data:read")]))
        .ConfigureAwait(false);

      // A change wrote version 2 and failed before making it current
      var current = (await SetsAsync()
                       .ConfigureAwait(false))["team-reader"];
      await AuthorizationTable!.SetPermissionSetAsync(current with
                                                      {
                                                        Version = 2,
                                                        Permissions = ["data:read", "data:delete"],
                                                      })
                               .ConfigureAwait(false);
      await AuthorizationTable.SetPermissionSetAsync(current)
                              .ConfigureAwait(false);

      await ReconcileAsync(Options(2,
                                   sets:
                                   [
                                     Reader("data:read",
                                            "data:download"),
                                   ]))
        .ConfigureAwait(false);

      var stored = (await SetsAsync()
                      .ConfigureAwait(false))["team-reader"];
      Assert.That(stored.Version,
                  Is.EqualTo(3));
      Assert.That(stored.Permissions,
                  Is.EquivalentTo(new[]
                                  {
                                    "data:read",
                                    "data:download",
                                  }));
    }
  }

  [Test]
  public async Task AnOlderConfigurationCannotWriteAgainWhatALaterOneLeftUnended()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(1,
        [
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
          Grant("revoked",
                "role:team",
                "session-view"),
        ]))
        .ConfigureAwait(false);

      // Revision 2 revokes a grant, then fails before it ends its change
      Assert.That(() => new AuthorizationReconciler(new FailingEnd(AuthorizationTable!),
                                                    Options(2,
                                                    [
                                                      Grant("own",
                                                            "role:team",
                                                            "session-own",
                                                            Ownership),
                                                    ]),
                                                    new AuthorizationAudit(NullLoggerFactory.Instance),
                                                    TimeProvider.System,
                                                    NullLogger<AuthorizationReconciler>.Instance).ReconcileAsync(),
                  Throws.InstanceOf<TimeoutException>());

      // Nothing of revision 2 is kept, so there is nothing left for revision 1 to complete
      if (await AtomicAsync()
            .ConfigureAwait(false))
      {
        await AssertNothingWrittenAsync(1,
                                        "own",
                                        "revoked")
          .ConfigureAwait(false);
        return;
      }

      Assert.That(() => ReconcileAsync(Options(1,
                  [
                    Grant("own",
                          "role:team",
                          "session-own",
                          Ownership),
                    Grant("revoked",
                          "role:team",
                          "session-view"),
                  ])),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("revision 2 or later"));
      Assert.That((await GrantsAsync()
                     .ConfigureAwait(false)).Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "own",
                                  }));
    }
  }

  [Test]
  public async Task AnOlderConfigurationCannotCompleteALaterOneThatFailedWhileWriting()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(1,
        [
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
          Grant("revoked",
                "role:team",
                "session-view"),
        ]))
        .ConfigureAwait(false);

      // Revision 2 revokes a grant, and fails while writing
      Assert.That(() => ReconcileAsync(new FailingWrite(AuthorizationTable!),
                                       Options(2,
                                       [
                                         Grant("own",
                                               "role:team",
                                               "session-own",
                                               Ownership),
                                       ])),
                  Throws.InstanceOf<TimeoutException>());

      if (await AtomicAsync()
            .ConfigureAwait(false))
      {
        await AssertNothingWrittenAsync(1,
                                        "own",
                                        "revoked")
          .ConfigureAwait(false);
        return;
      }

      // Revision 1 does not complete the change of revision 2 with what revision 2 revoked
      Assert.That(() => ReconcileAsync(Options(1,
                  [
                    Grant("own",
                          "role:team",
                          "session-own",
                          Ownership),
                    Grant("revoked",
                          "role:team",
                          "session-view"),
                  ])),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("revision 2 or later"));
      Assert.That((await AuthorizationTable!.GetGenerationAsync()
                                            .ConfigureAwait(false)).Stable,
                  Is.False);
    }
  }

  /// <summary>A writer failing on its first write</summary>
  private sealed class FailingWrite(IAuthorizationTable inner) : FailingEnd(inner)
  {
    public override Task<bool> DeleteGrantAsync(string            grantId,
                                                CancellationToken cancellationToken = default)
      => throw new TimeoutException("the storage did not answer");
  }

  /// <summary>Another writer that took the lock over changes the grants while this one changes them</summary>
  private sealed class TakenOver(IAuthorizationTable inner) : FailingEnd(inner)
  {
    private bool takenOver_;

    public override async Task<long> BeginChangeAsync(CancellationToken cancellationToken = default)
    {
      var generation = await Inner.BeginChangeAsync(cancellationToken)
                                  .ConfigureAwait(false);
      if (takenOver_)
      {
        return generation;
      }

      takenOver_ = true;
      var other = await Inner.BeginChangeAsync(cancellationToken)
                             .ConfigureAwait(false);
      await Inner.EndChangeAsync(other,
                                 cancellationToken)
                 .ConfigureAwait(false);
      return generation;
    }

    public override Task EndChangeAsync(long              generation,
                                        CancellationToken cancellationToken = default)
      => Inner.EndChangeAsync(generation,
                              cancellationToken);
  }

  /// <summary>A lock already lost when the reconciliation starts</summary>
  private sealed class LostLock(IAuthorizationTable inner) : FailingEnd(inner)
  {
    protected override CancellationToken Lost(IAuthorizationLock held)
      => new(true);
  }

  [Test]
  public async Task AWriterLosingItsLockAfterAnotherEndedItsChangeLeavesTheGrantsUnended()
  {
    if (RunTests)
    {
      Assert.That(() => ReconcileAsync(new LostWhileWriting(AuthorizationTable!),
                                       Options(1,
                                       [
                                         Grant("own",
                                               "role:team",
                                               "session-own",
                                               Ownership),
                                       ])),
                  Throws.InstanceOf<OperationCanceledException>());
      if (await AtomicAsync()
            .ConfigureAwait(false))
      {
        await AssertNothingWrittenAsync(0)
          .ConfigureAwait(false);
        return;
      }

      // Its writes may land after the change of the other writer: none of them is served until a reconciliation
      Assert.That((await AuthorizationTable!.GetGenerationAsync()
                                            .ConfigureAwait(false)).Stable,
                  Is.False);
    }
  }

  // The lock is lost once the change began, while another replica, which took it over, began and ended its own change
  private sealed class LostWhileWriting(IAuthorizationTable inner) : FailingEnd(inner)
  {
    private readonly CancellationTokenSource lost_ = new();

    protected override CancellationToken Lost(IAuthorizationLock held)
      => lost_.Token;

    public override async Task<long> BeginChangeAsync(CancellationToken cancellationToken = default)
    {
      var generation = await Inner.BeginChangeAsync(cancellationToken)
                                  .ConfigureAwait(false);
      if (!lost_.IsCancellationRequested)
      {
        await Inner.EndChangeAsync(await Inner.BeginChangeAsync(cancellationToken)
                                              .ConfigureAwait(false),
                                   cancellationToken)
                   .ConfigureAwait(false);
        await lost_.CancelAsync()
                   .ConfigureAwait(false);
      }

      return generation;
    }
  }

  private Task<bool> ReconcileAsync(IAuthorizationTable  table,
                                    AuthorizationOptions options,
                                    ILoggerFactory?      audit = null)
    => new AuthorizationReconciler(table,
                                   options,
                                   new AuthorizationAudit(audit ?? NullLoggerFactory.Instance),
                                   TimeProvider.System,
                                   NullLogger<AuthorizationReconciler>.Instance).ReconcileAsync();

  [Test]
  public async Task AChangeMixedWithAnotherWriterIsLeftUnended()
  {
    if (RunTests)
    {
      var changes = new List<string>();
      using var audit = LoggerFactory.Create(builder => builder.AddProvider(new ForwardingLoggerProvider((_,
                                                                                                          _,
                                                                                                          _,
                                                                                                          message,
                                                                                                          _,
                                                                                                          _) =>
                                                                                                         {
                                                                                                           if (message.Contains("Authorization change"))
                                                                                                           {
                                                                                                             changes.Add(message);
                                                                                                           }
                                                                                                         })));

      Assert.That(() => ReconcileAsync(new TakenOver(AuthorizationTable!),
                                       Options(1,
                                       [
                                         Grant("own",
                                               "role:team",
                                               "session-own",
                                               Ownership),
                                       ]),
                                       audit),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("lock was lost"));
      if (await AtomicAsync()
            .ConfigureAwait(false))
      {
        // Discarded with the other change, and not recorded as made
        await AssertNothingWrittenAsync(0)
          .ConfigureAwait(false);
        Assert.That(changes,
                    Is.Empty);
        return;
      }

      Assert.That((await AuthorizationTable!.GetGenerationAsync()
                                            .ConfigureAwait(false)).Stable,
                  Is.False);
    }
  }

  [Test]
  public async Task ALostLockStopsTheReconciliation()
  {
    if (RunTests)
    {
      Assert.That(() => ReconcileAsync(new LostLock(AuthorizationTable!),
                                       Options(1,
                                       [
                                         Grant("own",
                                               "role:team",
                                               "session-own",
                                               Ownership),
                                       ])),
                  Throws.InstanceOf<OperationCanceledException>());
      Assert.That(await GrantsAsync()
                    .ConfigureAwait(false),
                  Is.Empty);
    }
  }

  /// <summary>A table whose changes cannot be ended, as when the writer fails just before</summary>
  private class FailingEnd(IAuthorizationTable inner) : IAuthorizationTable
  {
    // The table the lock gives while it is held, written through as the reconciler would
    private IAuthorizationTable? bound_;

    protected IAuthorizationTable Inner
      => bound_ ?? inner;

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Inner.Check(tag);

    public Task Init(CancellationToken cancellationToken)
      => Inner.Init(cancellationToken);

    public IAsyncEnumerable<GrantData> ListGrantsAsync(CancellationToken cancellationToken = default)
      => Inner.ListGrantsAsync(cancellationToken);

    public Task CreateGrantAsync(GrantData         grant,
                                 CancellationToken cancellationToken = default)
      => Inner.CreateGrantAsync(grant,
                                cancellationToken);

    public virtual Task<bool> DeleteGrantAsync(string            grantId,
                                               CancellationToken cancellationToken = default)
      => Inner.DeleteGrantAsync(grantId,
                                cancellationToken);

    public IAsyncEnumerable<PermissionSetData> ListPermissionSetsAsync(CancellationToken cancellationToken = default)
      => Inner.ListPermissionSetsAsync(cancellationToken);

    public Task<PermissionSetData?> GetPermissionSetVersionAsync(string            name,
                                                                 int               version,
                                                                 CancellationToken cancellationToken = default)
      => Inner.GetPermissionSetVersionAsync(name,
                                            version,
                                            cancellationToken);

    public Task<int> GetLatestPermissionSetVersionAsync(string            name,
                                                        CancellationToken cancellationToken = default)
      => Inner.GetLatestPermissionSetVersionAsync(name,
                                                  cancellationToken);

    public Task SetPermissionSetAsync(PermissionSetData permissionSet,
                                      CancellationToken cancellationToken = default)
      => Inner.SetPermissionSetAsync(permissionSet,
                                     cancellationToken);

    public Task<bool> DeletePermissionSetAsync(string            name,
                                               CancellationToken cancellationToken = default)
      => Inner.DeletePermissionSetAsync(name,
                                        cancellationToken);

    public Task<int> GetConfigRevisionAsync(CancellationToken cancellationToken = default)
      => Inner.GetConfigRevisionAsync(cancellationToken);

    public Task SetConfigRevisionAsync(int               revision,
                                       CancellationToken cancellationToken = default)
      => Inner.SetConfigRevisionAsync(revision,
                                      cancellationToken);

    public async Task<IAuthorizationLock> LockAsync(CancellationToken cancellationToken = default)
    {
      var held = await inner.LockAsync(cancellationToken)
                            .ConfigureAwait(false);
      bound_ = held.Table;
      return new Bound(this,
                       held);
    }

    public Task<AuthorizationGeneration> GetGenerationAsync(CancellationToken cancellationToken = default)
      => Inner.GetGenerationAsync(cancellationToken);

    public virtual Task<long> BeginChangeAsync(CancellationToken cancellationToken = default)
      => Inner.BeginChangeAsync(cancellationToken);

    public virtual Task EndChangeAsync(long              generation,
                                       CancellationToken cancellationToken = default)
      => throw new TimeoutException("the storage did not answer");

    // When the lock is lost
    protected virtual CancellationToken Lost(IAuthorizationLock held)
      => held.Lost;

    // The lock, whose table is this one
    private sealed class Bound(FailingEnd         table,
                               IAuthorizationLock held) : IAuthorizationLock
    {
      public CancellationToken Lost
        => table.Lost(held);

      public bool Atomic
        => held.Atomic;

      public Task CommitAsync(CancellationToken cancellationToken = default)
        => held.CommitAsync(cancellationToken);

      public ValueTask DisposeAsync()
      {
        table.bound_ = null;
        return held.DisposeAsync();
      }
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
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
          Grant("view",
                "role:team",
                "session-view"),
        ]))
        .ConfigureAwait(false);

      await ReconcileAsync(Options(2,
        [
          Grant("own",
                "role:team",
                "session-own",
                "session.owner == request.caller"),
        ]))
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
        [
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
        ]))
        .ConfigureAwait(false);

      Assert.That(await ReconcileAsync(Options(1,
                    [
                      Grant("own",
                            "role:team",
                            "session-own",
                            Ownership),
                      Grant("revoked",
                            "role:team",
                            "cluster-admin"),
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
        [
          Grant("own",
                "role:team",
                "session-own",
                Ownership),
        ]))
        .ConfigureAwait(false);

      Assert.That(() => ReconcileAsync(Options(2,
                  [
                    Grant("own2",
                          "role:team",
                          "session-own",
                          Ownership),
                    Grant("broken",
                          "role:team",
                          "session-create",
                          "session.owner == request.caller"),
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
  public async Task ASetRedefinedByTheApiIsKept()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("session-own",
                                                                            1,
                                                                            ["session:read"],
                                                                            AuthorizationOrigin.Api))
                               .ConfigureAwait(false);

      await ReconcileAsync(Options(1))
        .ConfigureAwait(false);

      var set = (await SetsAsync()
                   .ConfigureAwait(false))["session-own"];
      Assert.That(set.Origin,
                  Is.EqualTo(AuthorizationOrigin.Api));
      Assert.That(set.Version,
                  Is.EqualTo(1));
      Assert.That(set.Permissions,
                  Is.EqualTo(new[]
                             {
                               "session:read",
                             }));
    }
  }

  [Test]
  public async Task AStoredSetKeepsThePermissionsOfTheCatalogueOnly()
  {
    if (RunTests)
    {
      // Written by a later version, naming a permission this one does not know
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("later",
                                                                            1,
                                                                            ["session:read", "session:archive"],
                                                                            AuthorizationOrigin.Api))
                               .ConfigureAwait(false);

      // The set is kept without the unknown permission: the grant naming it is accepted
      Assert.That(await ReconcileAsync(Options(1,
                    [
                      Grant("reader",
                            "role:team",
                            "later"),
                    ]))
                    .ConfigureAwait(false),
                  Is.True);
    }
  }

  [Test]
  public async Task AMalformedSetOfTheApiDoesNotBlockTheConfiguration()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("ops",
                                                                            1,
                                                                            ["session:read", null!],
                                                                            AuthorizationOrigin.Api))
                               .ConfigureAwait(false);
      await AuthorizationTable.CreateGrantAsync(new GrantData("api",
                                                              "role:ops",
                                                              "ops",
                                                              null,
                                                              AuthorizationOrigin.Api,
                                                              DateTime.UtcNow))
                              .ConfigureAwait(false);

      var faults = new List<string>();
      using var audit = LoggerFactory.Create(builder => builder.AddProvider(new ForwardingLoggerProvider((_,
                                                                                                          _,
                                                                                                          _,
                                                                                                          message,
                                                                                                          _,
                                                                                                          _) =>
                                                                                                         {
                                                                                                           if (message.Contains("Fault"))
                                                                                                           {
                                                                                                             faults.Add(message);
                                                                                                           }
                                                                                                         })));

      // The grant of the API was already refused: it stays refused, and the configuration is applied
      Assert.That(await ReconcileAsync(Options(1,
                                       [
                                         Grant("own",
                                               "role:team",
                                               "session-own",
                                               Ownership),
                                       ]),
                                       audit)
                    .ConfigureAwait(false),
                  Is.True);
      Assert.That((await GrantsAsync()
                     .ConfigureAwait(false)).Keys,
                  Is.EquivalentTo(new[]
                                  {
                                    "api",
                                    "own",
                                  }));
      // Recorded once, though the sets are checked as stored and as they become
      Assert.That(faults,
                  Has.One.Contains("Fault on permission set ops"));
    }
  }

  [Test]
  public async Task AGrantOfTheConfigurationNamingAMalformedSetIsRefused()
  {
    if (RunTests)
    {
      await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("ops",
                                                                            1,
                                                                            ["session:read", null!],
                                                                            AuthorizationOrigin.Api))
                               .ConfigureAwait(false);

      Assert.That(() => ReconcileAsync(Options(1,
                  [
                    Grant("reader",
                          "role:team",
                          "ops"),
                  ])),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("reader"));
    }
  }

  private static IEnumerable<TestCaseData> MalformedPermissions()
  {
    yield return new TestCaseData(new List<string>
                                  {
                                    "data:read",
                                    null!,
                                  }).SetArgDisplayNames("NullPermission");
    yield return new TestCaseData(null).SetArgDisplayNames("NoPermissionList");
  }

  [TestCaseSource(nameof(MalformedPermissions))]
  public async Task AMalformedSetOfTheConfigurationStillConfiguredIsRewritten(List<string>? permissions)
  {
    if (RunTests)
    {
      try
      {
        await AuthorizationTable!.SetPermissionSetAsync(new PermissionSetData("team-reader",
                                                                              1,
                                                                              permissions!,
                                                                              AuthorizationOrigin.Config))
                                 .ConfigureAwait(false);
      }
      catch (ArgumentNullException)
      {
        Assert.Ignore("This storage cannot hold a set without a permission list");
      }

      Assert.That(await ReconcileAsync(Options(1,
                                               [
                                                 Grant("reader",
                                                       "role:team",
                                                       "team-reader"),
                                               ],
                                               [
                                                 new PermissionSetOption
                                                 {
                                                   Name        = "team-reader",
                                                   Permissions = ["data:read"],
                                                 },
                                               ]))
                    .ConfigureAwait(false),
                  Is.True);

      var set = (await SetsAsync()
                   .ConfigureAwait(false))["team-reader"];
      Assert.That(set.Version,
                  Is.EqualTo(2));
      Assert.That(set.Permissions,
                  Is.EqualTo(new[]
                             {
                               "data:read",
                             }));
    }
  }

  [Test]
  public async Task EverythingIsStoredAsWritten()
  {
    if (RunTests)
    {
      await ReconcileAsync(Options(1))
        .ConfigureAwait(false);

      // Not expanded, so that versions with other catalogues store the same set and never rewrite each other's
      Assert.That((await SetsAsync()
                     .ConfigureAwait(false))["cluster-admin"].Permissions,
                  Is.EqualTo(new[]
                             {
                               "*",
                             }));
    }
  }

  [Test]
  public async Task ConcurrentReconciliationsAgree()
  {
    if (RunTests)
    {
      var options = Options(1,
      [
        Grant("own",
              "role:team",
              "session-own",
              Ownership),
        Grant("create",
              "role:team",
              "session-create",
              "request.partition == \"q\""),
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
