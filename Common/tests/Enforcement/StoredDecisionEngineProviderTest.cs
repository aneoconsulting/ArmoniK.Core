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

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(StoredDecisionEngineProvider))]
public class StoredDecisionEngineProviderTest
{
  private sealed class ManualTime : TimeProvider
  {
    public DateTimeOffset Now { get; set; } = new(2026,
                                                  10,
                                                  8,
                                                  12,
                                                  0,
                                                  0,
                                                  TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow()
      => Now;
  }

  private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(5);

  private readonly List<GrantData>     grants_ = new();
  private          int                 reads_;
  private          Mock<IAuthorizationTable> table_ = null!;
  private          ManualTime          time_   = null!;
  private          bool                fail_;

  [SetUp]
  public void SetUp()
  {
    grants_.Clear();
    reads_ = 0;
    fail_  = false;
    time_  = new ManualTime();
    table_ = new Mock<IAuthorizationTable>();
    table_.Setup(table => table.ListPermissionSetsAsync(It.IsAny<CancellationToken>()))
          .Returns(() => new[]
                         {
                           new PermissionSetData("session-view",
                                                 3,
                                                 ["session:read"],
                                                 AuthorizationOrigin.Config),
                         }.ToAsyncEnumerable());
    table_.Setup(table => table.ListGrantsAsync(It.IsAny<CancellationToken>()))
          .Returns(() =>
                   {
                     Interlocked.Increment(ref reads_);
                     return fail_
                              ? throw new InvalidOperationException("storage is down")
                              : grants_.ToList()
                                       .ToAsyncEnumerable();
                   });
  }

  private StoredDecisionEngineProvider Provider()
    => new(table_.Object,
           new AuthorizationOptions
           {
             GrantCacheTtl = Ttl,
           },
           new AuthorizationAudit(NullLoggerFactory.Instance),
           time_,
           NullLogger<StoredDecisionEngineProvider>.Instance);

  private void AddGrant(string id)
    => grants_.Add(new GrantData(id,
                                 "role:team",
                                 "session-view",
                                 null,
                                 AuthorizationOrigin.Config,
                                 DateTime.UtcNow));

  [Test]
  public async Task AChangeIsSeenOnceTheCacheExpires()
  {
    using var provider = Provider();
    AddGrant("a");

    var first = await provider.GetEngineAsync()
                              .ConfigureAwait(false);
    AddGrant("b");
    time_.Now += Ttl - TimeSpan.FromMilliseconds(1);
    var cached = await provider.GetEngineAsync()
                               .ConfigureAwait(false);
    time_.Now += TimeSpan.FromMilliseconds(1);
    var refreshed = await provider.GetEngineAsync()
                                  .ConfigureAwait(false);

    Assert.That(first.Grants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "a",
                           }));
    Assert.That(cached,
                Is.SameAs(first));
    Assert.That(refreshed.Grants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "a",
                             "b",
                           }));
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
  public async Task AFailedReadServesTheLastEngineForOneMorePeriodThenFails()
  {
    using var provider = Provider();
    AddGrant("a");
    var first = await provider.GetEngineAsync()
                              .ConfigureAwait(false);

    fail_    =  true;
    time_.Now += Ttl;
    Assert.That(await provider.GetEngineAsync()
                              .ConfigureAwait(false),
                Is.SameAs(first));

    time_.Now += Ttl;
    Assert.That(() => provider.GetEngineAsync()
                              .AsTask(),
                Throws.InstanceOf<InvalidOperationException>());
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
}
