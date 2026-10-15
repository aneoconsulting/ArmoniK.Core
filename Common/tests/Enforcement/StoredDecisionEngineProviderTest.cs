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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(StoredDecisionEngineProvider))]
public class StoredDecisionEngineProviderTest
{
  // The provider measures time with the system clock
  private static readonly TimeSpan Ttl = TimeSpan.FromMilliseconds(200);

  private readonly List<GrantData>           grants_ = new();
  private          bool                      changeWhileReading_;
  private          bool                      fail_;
  private          AuthorizationGeneration   generation_;
  private          TaskCompletionSource?     hold_;
  private          ExceptionManager          exceptionManager_ = null!;
  private          ApplicationLifetime       lifetime_         = null!;
  private          int                       reads_;
  private          Mock<IAuthorizationTable> table_ = null!;

  [SetUp]
  public void SetUp()
  {
    grants_.Clear();
    reads_              = 0;
    fail_               = false;
    hold_               = null;
    generation_         = default;
    changeWhileReading_ = false;
    lifetime_           = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);
    // A single error is allowed: a second one marks the control plane as failed
    exceptionManager_ = new ExceptionManager(lifetime_,
                                             new OptionsWrapper<ConsoleLifetimeOptions>(new ConsoleLifetimeOptions()),
                                             new HostingEnvironment(),
                                             new OptionsWrapper<HostOptions>(new HostOptions()),
                                             NullLogger<ExceptionManager>.Instance,
                                             new ExceptionManager.Options(TimeSpan.Zero,
                                                                          TimeSpan.FromHours(1),
                                                                          1));
    table_              = new Mock<IAuthorizationTable>();
    table_.Setup(table => table.GetGenerationAsync(It.IsAny<CancellationToken>()))
          .Returns(() => Task.FromResult(generation_));
    table_.Setup(table => table.ListPermissionSetsAsync(It.IsAny<CancellationToken>()))
          .Returns(() => new[]
                         {
                           new PermissionSetData("session-view",
                                                 3,
                                                 ["session:read"],
                                                 AuthorizationOrigin.Config),
                         }.ToAsyncEnumerable());
    table_.Setup(table => table.ListGrantsAsync(It.IsAny<CancellationToken>()))
          .Returns((CancellationToken cancellationToken) =>
                   {
                     Interlocked.Increment(ref reads_);
                     return Read(hold_?.Task,
                                 cancellationToken);
                   });
  }

  [TearDown]
  public void TearDown()
  {
    exceptionManager_.Dispose();
    lifetime_.StopApplication();
  }

  private async IAsyncEnumerable<GrantData> Read(Task?             hold,
                                                 CancellationToken cancellationToken)
  {
    if (hold is not null)
    {
      await hold.WaitAsync(cancellationToken)
                .ConfigureAwait(false);
    }

    cancellationToken.ThrowIfCancellationRequested();

    if (fail_)
    {
      throw new InvalidOperationException("storage is down");
    }

    if (changeWhileReading_)
    {
      generation_ = new AuthorizationGeneration(generation_.Begun + 1,
                                                generation_.Begun + 1);
    }

    foreach (var grant in grants_.ToList())
    {
      yield return grant;
    }
  }

  private StoredDecisionEngineProvider Provider()
    => new(table_.Object,
           new AuthorizationOptions
           {
             GrantCacheTtl = Ttl,
           },
           new AuthorizationAudit(NullLoggerFactory.Instance),
           exceptionManager_,
           NullLogger<StoredDecisionEngineProvider>.Instance);

  private void AddGrant(string id)
    => grants_.Add(new GrantData(id,
                                 "role:team",
                                 "session-view",
                                 null,
                                 AuthorizationOrigin.Config,
                                 DateTime.UtcNow));

  private static IEnumerable<string> Ids(StoredDecisionEngineProvider provider)
    => provider.GetEngineAsync()
               .AsTask()
               .Result.Grants.Select(grant => grant.Id)
               .ToList();

  [Test]
  public async Task AChangeIsSeenOnceTheCacheExpires()
  {
    using var provider = Provider();
    AddGrant("a");

    var first = await provider.GetEngineAsync()
                              .ConfigureAwait(false);
    AddGrant("b");
    var cached = await provider.GetEngineAsync()
                               .ConfigureAwait(false);

    Assert.That(first.Grants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
    Assert.That(cached,
                Is.SameAs(first));
    Assert.That(() => Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                             "b",
                           })
                  .After(5000,
                         20));
  }

  [Test]
  public async Task TheStoredVersionOfASetIsUsed()
  {
    using var provider = Provider();
    AddGrant("a");

    var engine = await provider.GetEngineAsync()
                               .ConfigureAwait(false);

    Assert.That(engine.Grants.Single()
                      .PermissionSet.Version,
                Is.EqualTo(3));
  }

  [Test]
  public async Task ConcurrentCallersReadOnce()
  {
    using var provider = Provider();
    AddGrant("a");

    await Task.WhenAll(Enumerable.Range(0,
                                        20)
                                 .Select(_ => provider.GetEngineAsync()
                                                      .AsTask()))
              .ConfigureAwait(false);

    Assert.That(reads_,
                Is.EqualTo(1));
  }

  [Test]
  public async Task ACallerCancellingTheReadDoesNotFailTheCallersAfterIt()
  {
    using var provider = Provider();
    AddGrant("a");
    hold_ = new TaskCompletionSource();

    using var cts = new CancellationTokenSource();
    var cancelled = provider.GetEngineAsync(cts.Token)
                            .AsTask();
    cts.Cancel();
    // Cancelling, the caller would cancel the read with it; else it waits for the read
    await Task.WhenAny(cancelled,
                       Task.Delay(100))
              .ConfigureAwait(false);
    hold_.SetResult();

    Assert.That(Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
  }

  [Test]
  public async Task ACallerWaitingForTheReadStopsWithItsCall()
  {
    using var provider = Provider();
    AddGrant("a");
    hold_ = new TaskCompletionSource();

    using var cts = new CancellationTokenSource();
    var waiting = provider.GetEngineAsync(cts.Token)
                          .AsTask();
    await cts.CancelAsync()
             .ConfigureAwait(false);

    // Stopped while the read is still held
    Assert.That(() => waiting.WaitAsync(TimeSpan.FromSeconds(1)),
                Throws.InstanceOf<OperationCanceledException>());

    hold_.SetResult();
    Assert.That(Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
    Assert.That(reads_,
                Is.EqualTo(1));
  }

  [Test]
  public async Task AnExpiredEngineIsServedWhileItIsReadAgain()
  {
    using var provider = Provider();
    AddGrant("a");
    var first = await provider.GetEngineAsync()
                              .ConfigureAwait(false);

    hold_ = new TaskCompletionSource();
    AddGrant("b");
    await Task.Delay(Ttl * 1.25)
              .ConfigureAwait(false);
    var during    = provider.GetEngineAsync();
    var completed = during.IsCompleted;
    hold_.SetResult();

    Assert.That(completed,
                Is.True);
    Assert.That(await during.ConfigureAwait(false),
                Is.SameAs(first));
    Assert.That(() => Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                             "b",
                           })
                  .After(5000,
                         20));
  }

  [Test]
  public async Task AFailedReadServesTheLastEngineForOneMorePeriodThenFails()
  {
    using var provider = Provider();
    AddGrant("a");
    var first = await provider.GetEngineAsync()
                              .ConfigureAwait(false);

    fail_ = true;
    await Task.Delay(Ttl * 1.25)
              .ConfigureAwait(false);
    Assert.That(await provider.GetEngineAsync()
                              .ConfigureAwait(false),
                Is.SameAs(first));

    await Task.Delay(Ttl)
              .ConfigureAwait(false);
    Assert.That(() => provider.GetEngineAsync()
                              .AsTask(),
                Throws.InstanceOf<GrantsUnavailableException>());
  }

  [Test]
  public async Task PastTwoPeriodsCallersShareOneFailedRead()
  {
    using var provider = Provider();
    AddGrant("a");
    await provider.GetEngineAsync()
                  .ConfigureAwait(false);

    fail_ = true;
    hold_ = new TaskCompletionSource();
    await Task.Delay(Ttl * 2.25)
              .ConfigureAwait(false);
    var waiting = Enumerable.Range(0,
                                   20)
                            .Select(_ => provider.GetEngineAsync()
                                                 .AsTask())
                            .ToList();
    hold_.SetResult();

    foreach (var task in waiting)
    {
      Assert.That(() => task,
                  Throws.InstanceOf<GrantsUnavailableException>());
    }

    Assert.That(reads_,
                Is.EqualTo(2));
  }

  [Test]
  public async Task ARefusedStoredGrantIsLeftOut()
  {
    using var provider = Provider();
    AddGrant("a");
    grants_.Add(new GrantData("broken",
                              "role:team",
                              "session-view",
                              "session.owner = request.caller",
                              AuthorizationOrigin.Api,
                              DateTime.UtcNow));

    var engine = await provider.GetEngineAsync()
                               .ConfigureAwait(false);

    Assert.That(engine.Grants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
  }

  [Test]
  public async Task AReadDuringAChangeFails()
  {
    using var provider = Provider();
    AddGrant("a");
    generation_ = new AuthorizationGeneration(1,
                                              0);

    Assert.That(() => provider.GetEngineAsync()
                              .AsTask(),
                Throws.InstanceOf<GrantsUnavailableException>());

    generation_ = new AuthorizationGeneration(1,
                                              1);
    // The failed read is not tried again before a period
    await Task.Delay(2 * Ttl)
              .ConfigureAwait(false);

    Assert.That(Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
  }

  [Test]
  public void AReadAcrossAChangeFails()
  {
    using var provider = Provider();
    AddGrant("a");
    changeWhileReading_ = true;

    Assert.That(() => provider.GetEngineAsync()
                              .AsTask(),
                Throws.InstanceOf<GrantsUnavailableException>());
  }

  [Test]
  public async Task AChangeUnderWayIsNeverServed()
  {
    using var provider = Provider();
    AddGrant("a");
    await provider.GetEngineAsync()
                  .ConfigureAwait(false);

    // Half written: b is stored but the change has not ended
    generation_ = new AuthorizationGeneration(1,
                                              0);
    AddGrant("b");

    var until = DateTime.UtcNow + 4 * Ttl;
    while (DateTime.UtcNow < until)
    {
      try
      {
        Assert.That(Ids(provider),
                    Is.EqualTo(new[]
                               {
                                 "a",
                               }));
      }
      catch (AggregateException e) when (e.InnerException is GrantsUnavailableException)
      {
        // Past two periods without a read kept, refused rather than served half written
      }

      await Task.Delay(10)
                .ConfigureAwait(false);
    }

    generation_ = new AuthorizationGeneration(1,
                                              1);
    await Task.Delay(2 * Ttl)
              .ConfigureAwait(false);

    Assert.That(Ids(provider),
                Is.EqualTo(new[]
                           {
                             "a",
                             "b",
                           }));
  }

  [Test]
  public async Task AFailedReadIsCountedOnceForAllTheCallersItRefuses()
  {
    using var provider = Provider();
    fail_ = true;

    var refused = Enumerable.Range(0,
                                   20)
                            .Select(_ => provider.GetEngineAsync()
                                                 .AsTask())
                            .ToList();
    foreach (var task in refused)
    {
      Assert.That(() => task,
                  Throws.InstanceOf<GrantsUnavailableException>());
    }

    Assert.That(reads_,
                Is.EqualTo(1));
    Assert.That(exceptionManager_.Failed,
                Is.False);

    // The next read fails too: counted again
    await Task.Delay(Ttl * 1.25)
              .ConfigureAwait(false);
    Assert.That(() => provider.GetEngineAsync()
                              .AsTask(),
                Throws.InstanceOf<GrantsUnavailableException>());
    Assert.That(reads_,
                Is.EqualTo(2));
    Assert.That(exceptionManager_.Failed,
                Is.True);
  }

  [Test]
  public async Task AReadDuringAChangeIsNotCounted()
  {
    using var provider = Provider();
    AddGrant("a");
    generation_ = new AuthorizationGeneration(1,
                                              0);

    for (var read = 0; read < 3; read++)
    {
      Assert.That(() => provider.GetEngineAsync()
                                .AsTask(),
                  Throws.InstanceOf<GrantsUnavailableException>());
      await Task.Delay(Ttl * 1.25)
                .ConfigureAwait(false);
    }

    Assert.That(exceptionManager_.Failed,
                Is.False);
  }
}
