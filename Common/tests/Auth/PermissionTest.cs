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

using ArmoniK.Core.Common.Auth.Authorization.Permissions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture]
public class PermissionTest
{
  [TestCase("testprefix:testname:testsuffix",
            "testprefix",
            "testname",
            "testsuffix")]
  [TestCase("prefix:name",
            "prefix",
            "name",
            PermissionScope.Default)]
  public void PermissionFromStringShouldMatch(string  actionName,
                                              string? prefix,
                                              string? name,
                                              string? suffix)
  {
    var perm = new Permission(actionName);
    Assert.That(prefix,
                Is.EqualTo(perm.Service));
    Assert.That(name,
                Is.EqualTo(perm.Name));
    Assert.That(suffix,
                Is.EqualTo(perm.Target));
  }

  [TestCase("prefix:name:suffix",
            "prefix:name",
            "suffix")]
  [TestCase("prefix:name",
            "prefix:name",
            PermissionScope.Default)]
  public void PermissionToClaimShouldMatch(string actionName,
                                           string claimType,
                                           string claimValue)
  {
    var perm = new Permission(actionName);
    Assert.That(claimType,
                Is.EqualTo(perm.Claim.Type));
    Assert.That(claimValue,
                Is.EqualTo(perm.Claim.Value));
  }

  [TestCase("")]
  [TestCase("testprefix")]
  [TestCase("testprefix:")]
  [TestCase("testprefix::")]
  [TestCase(":testprefix:")]
  [TestCase("::testprefix")]
  public void PermissionCreationShouldThrow(string actionstring)
    => Assert.That(Assert.Catch(() =>
                                {
                                  var _ = new Permission(actionstring);
                                }),
                   Is.Not.Null);

  [Test]
  public void PrintPermissions()
  {
    TestContext.Progress.WriteLine(string.Join("\n",
                                               ServicesPermissions.PermissionsLists[ServicesPermissions.All]));
    Assert.Ignore();
  }
}
