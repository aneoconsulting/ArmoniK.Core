// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture]
public class AuthenticationTableTestBase
{
  [OneTimeSetUp]
  public void SetUp()
  {
    GetAuthSource();
    if (RunTests)
    {
      AuthenticationTable!.AddRoles(Roles);
      AuthenticationTable!.AddUsers(Users);
      AuthenticationTable!.AddCertificates(Auths);
    }
  }

  [OneTimeTearDown]
  public virtual void TearDown()
    => RunTests = false;

  static AuthenticationTableTestBase()
  {
    Roles = new List<RoleData>
            {
              new("RoleId1",
                  "Role1",
                  new[]
                  {
                    "category1:name1",
                    "category1:name2",
                    "category2:name3",
                  }),
              new("RoleId2",
                  "Role2",
                  new[]
                  {
                    "category1:name1",
                    "category1:name2:" + Permissions.AllUsersScope,
                    "category2:name4",
                  }),
              new("RoleId3",
                  "Role3",
                  new[]
                  {
                    "category3:name1",
                    "category4:name2",
                    "category5:name3",
                  }),
              new("RoleId4",
                  "Role4",
                  Array.Empty<string>()),
            };
    Users = new List<UserData>
            {
              new("UserId1",
                  "User1",
                  new[]
                  {
                    Roles[0]
                      .RoleId,
                  }),
              new("UserId2",
                  "User2",
                  new[]
                  {
                    Roles[0]
                      .RoleId,
                    Roles[1]
                      .RoleId,
                  }),
              new("UserId3",
                  "User3",
                  new[]
                  {
                    Roles[0]
                      .RoleId,
                    Roles[2]
                      .RoleId,
                  }),
              new("UserId4",
                  "User4",
                  new[]
                  {
                    Roles[0]
                      .RoleId,
                    "RoleIdDontExist",
                  }),
              new("UserId5",
                  "User5",
                  Array.Empty<string>()),
            };
    Auths = new List<AuthData>
            {
              new("AuthId1",
                  Users[0]
                    .UserId,
                  "CNUser1",
                  "Fingerprint1"),
              new("AuthId2",
                  Users[1]
                    .UserId,
                  "CNUser2",
                  "Fingerprint2"),
              new("AuthId3",
                  Users[1]
                    .UserId,
                  "CNUser3",
                  "Fingerprint3"),
              new("AuthId4",
                  Users[2]
                    .UserId,
                  "CNUser4",
                  "Fingerprint4"),
              new("AuthId5",
                  Users[3]
                    .UserId,
                  "CNUser5",
                  "Fingerprint5"),
              new("AuthId6",
                  "UserIdDontExist",
                  "CNUser6",
                  "Fingerprint6"),
              new("AuthId7",
                  Users[1]
                    .UserId,
                  "CNUser2",
                  "Fingerprint7"),
              new("AuthId8",
                  Users[2]
                    .UserId,
                  "CNUserCommon",
                  null),
              new("AuthId9",
                  Users[3]
                    .UserId,
                  "CNUser2",
                  null),
            };
  }

  private static readonly List<RoleData> Roles;
  private static readonly List<AuthData> Auths;
  private static readonly List<UserData> Users;


  /* Interface to test */
  protected IAuthenticationTable? AuthenticationTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
 * of AuthenticationTable to the corresponding interface implementation */
  public virtual void GetAuthSource()
  {
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await AuthenticationTable!.Check(HealthCheckTag.Liveness)
                                                    .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await AuthenticationTable.Check(HealthCheckTag.Readiness)
                                                   .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await AuthenticationTable.Check(HealthCheckTag.Startup)
                                                   .ConfigureAwait(false)).Status);

      await AuthenticationTable.Init(CancellationToken.None)
                               .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await AuthenticationTable.Check(HealthCheckTag.Liveness)
                                                .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await AuthenticationTable.Check(HealthCheckTag.Readiness)
                                                .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await AuthenticationTable.Check(HealthCheckTag.Startup)
                                                .ConfigureAwait(false)).Status);
    }
  }

  [TestCase("CNUser1",
            "Fingerprint1",
            0)]
  [TestCase("CNUser2",
            "Fingerprint2",
            1)]
  [TestCase("CNUser3",
            "Fingerprint3",
            1)]
  [TestCase("CNUser4",
            "Fingerprint4",
            2)]
  [TestCase("CNUser5",
            "Fingerprint5",
            3)]
  [TestCase("CNUser2",
            "Fingerprint7",
            1)]
  [TestCase("CNUserCommon",
            "FingerprintDontCare",
            2)]
  [TestCase("CNUser2",
            "FingerprintDontCare",
            3)]
  [TestCase("CNUser2",
            "Fingerprint3",
            3)]
  public void GetIdentityFromCnAndFingerprintShouldSucceed(string cn,
                                                           string fingerprint,
                                                           int    userid)
  {
    if (!RunTests)
    {
      return;
    }

    var ident = AuthenticationTable!.GetIdentityFromCertificateAsync(cn,
                                                                     fingerprint,
                                                                     CancellationToken.None)
                                    .Result;
    Assert.NotNull(ident);
    Assert.AreEqual(Users[userid]
                      .UserId,
                    ident!.Id);
  }

  [TestCase("CNUser6",
            "Fingerprint6")]
  [TestCase("CNUser1",
            "Fingerprint2")]
  public void GetIdentityFromCnAndFingerprintShouldFail(string cn,
                                                        string fingerprint)
  {
    if (!RunTests)
    {
      return;
    }

    Assert.IsNull(AuthenticationTable!.GetIdentityFromCertificateAsync(cn,
                                                                       fingerprint,
                                                                       CancellationToken.None)
                                      .Result);
  }

  [TestCase(0,
            "User1")]
  [TestCase(1,
            "User2")]
  public void GetIdentityFromIdShouldSucceed(int    id,
                                             string username)
  {
    if (!RunTests)
    {
      return;
    }

    var ident = AuthenticationTable!.GetIdentityFromUserAsync(Users[id]
                                                                .UserId,
                                                              null)
                                    .Result;
    Assert.NotNull(ident);
    Assert.AreEqual(Users[id]
                      .UserId,
                    ident!.Id);
    Assert.AreEqual(username,
                    ident.Username);
  }

  [TestCase("UserIdDontExist")]
  public void GetIdentityFromIdShouldFail(string id)
  {
    if (!RunTests)
    {
      return;
    }

    Assert.IsNull(AuthenticationTable!.GetIdentityFromUserAsync(id,
                                                                null)
                                      .Result);
  }

  [TestCase("User1",
            0)]
  [TestCase("User2",
            1)]
  public void GetIdentityFromNameShouldSucceed(string name,
                                               int    id)
  {
    if (!RunTests)
    {
      return;
    }

    var identity = AuthenticationTable!.GetIdentityFromUserAsync(null,
                                                                 name)
                                       .Result;
    Assert.NotNull(identity);
    Assert.AreEqual(name,
                    identity!.Username);
    Assert.AreEqual(Users[id]
                      .UserId,
                    identity.Id);
  }

  [TestCase("UserDontExist")]
  public void GetIdentityFromNameShouldFail(string name)
  {
    if (!RunTests)
    {
      return;
    }

    Assert.IsNull(AuthenticationTable!.GetIdentityFromUserAsync(null,
                                                                name)
                                      .Result);
  }

  [TestCase("User1",
            "Role1",
            true)]
  [TestCase("User1",
            "Role2",
            false)]
  [TestCase("User2",
            "Role1",
            true)]
  [TestCase("User2",
            "RoleDontExist",
            false)]
  public void UserHasRoleShouldMatch(string username,
                                     string rolename,
                                     bool   hasRole)
  {
    if (!RunTests)
    {
      return;
    }

    var identity = AuthenticationTable!.GetIdentityFromUserAsync(null,
                                                                 username)
                                       .Result;
    Assert.NotNull(identity);
    Assert.AreEqual(identity!.Roles.Contains(rolename),
                    hasRole);
  }

  [TestCase("User1",
            "category1:name1",
            true)]
  [TestCase("User1",
            "category1:name2",
            true)]
  [TestCase("User1",
            "category1:name3",
            false)]
  [TestCase("User2",
            "category1:name2",
            true)]
  [TestCase("User2",
            "category1:name2:" + Permissions.AllUsersScope,
            true)]
  public void UserHasClaimShouldMatch(string username,
                                      string claim,
                                      bool   hasClaim)
  {
    if (!RunTests)
    {
      return;
    }

    var identity = AuthenticationTable!.GetIdentityFromUserAsync(null,
                                                                 username,
                                                                 CancellationToken.None)
                                       .Result;
    var expected = new Permissions.Permission(claim).Claim;
    Assert.NotNull(identity);
    Assert.AreEqual(identity!.Permissions.Select(perm => new Permissions.Permission(perm).Claim)
                             .Any(c => c.Type == expected.Type && (expected.Value == Permissions.Default || c.Value == expected.Value)),
                    hasClaim);
  }

  [TestCaseSource(nameof(Users))]
  public void UserHasAllClaimsOfItsRoles(UserData user)
  {
    if (!RunTests)
    {
      return;
    }

    var identity = AuthenticationTable!.GetIdentityFromUserAsync(user.UserId,
                                                                 null)
                                       .Result;
    Assert.NotNull(identity);
    Assert.IsTrue(user.Roles.SelectMany(id => Roles.Find(r => r.RoleId == id)
                                                   ?.Permissions ?? Array.Empty<string>())
                      .All(p =>
                           {
                             var expected = new Permissions.Permission(p).Claim;
                             return identity!.Permissions.Select(perm => new Permissions.Permission(perm).Claim)
                                             .Any(c => c.Type == expected.Type && c.Value == expected.Value);
                           }));
  }
}
