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

using System.Security.Claims;

using ArmoniK.Core.Common.Auth.Authentication;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture]
public class AuthenticationCacheTest
{
  [SetUp]
  public void Setup()
  {
    cache_ = new AuthenticationCache();
    cache_.Set(BaseKey,
               BaseIdentity);
  }

  private const string ConnectionId = "TestConnectionId";
  private const string Cn           = "CN1";
  private const string Fingerprint  = "Fingerprint1";

  private static readonly AuthenticationCacheKey BaseKey = new(ConnectionId,
                                                               Cn,
                                                               Fingerprint);

  private static readonly ClaimsPrincipal BaseIdentity = new(new UserIdentity(new UserAuthenticationResult(),
                                                                              string.Empty));

  private AuthenticationCache? cache_;

  [Test]
  public void CacheShouldHit()
  {
    var result = cache_!.Get(BaseKey);
    Assert.IsNotNull(result);
    Assert.AreEqual(BaseIdentity,
                    result);
  }

  [Test]
  [TestCase(ConnectionId,
            Cn,
            Fingerprint,
            "ImpId",
            null)]
  [TestCase(ConnectionId,
            Cn,
            Fingerprint,
            null,
            "ImpName")]
  [TestCase(ConnectionId,
            Cn          + "0",
            Fingerprint + "0",
            null,
            null)]
  [TestCase(ConnectionId,
            Cn + "0",
            null,
            null,
            null)]
  [TestCase(ConnectionId + "0",
            Cn,
            Fingerprint,
            null,
            null)]
  [TestCase(ConnectionId + "0",
            Cn           + "0",
            Fingerprint  + "0",
            null,
            null)]
  public void CacheShouldMiss(string  connectionId,
                              string? cn,
                              string? fingerprint,
                              string? impersonationId,
                              string? impersonationUsername)
  {
    var result = cache_!.Get(new AuthenticationCacheKey(connectionId,
                                                        cn,
                                                        fingerprint,
                                                        impersonationId,
                                                        impersonationUsername));
    Assert.IsTrue(result is null || BaseIdentity != result);
  }

  [Test]
  public void CacheShouldMissOnConnectionReset()
  {
    Assert.AreEqual(BaseIdentity,
                    cache_!.Get(BaseKey));
    cache_.FlushConnection(ConnectionId);
    Assert.IsNull(cache_!.Get(BaseKey));
  }

  [Test]
  public void CacheShouldMissOnClear()
  {
    Assert.AreEqual(BaseIdentity,
                    cache_!.Get(BaseKey));
    cache_.Clear();
    Assert.IsNull(cache_!.Get(BaseKey));
  }

  [Test]
  public void CacheKeyEquatableShouldMatch()
  {
    Assert.IsTrue(BaseKey.Equals(BaseKey));
    Assert.IsTrue(BaseKey.Equals(new AuthenticationCacheKey(ConnectionId,
                                                            Cn,
                                                            Fingerprint)));
    Assert.IsFalse(BaseKey.Equals(null));
    Assert.IsFalse(BaseKey!.Equals(new AuthenticationCacheKey(ConnectionId,
                                                              Cn + "0",
                                                              Fingerprint)));
    Assert.IsFalse(BaseKey.Equals(new object()));
  }
}
