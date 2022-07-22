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

using ArmoniK.Core.Common.Auth.Authorization;

using JetBrains.Annotations;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture]
public class PermissionTest
{
  [TestCase("testprefix:testname:testsuffix",
            "testprefix",
            "testname",
            "testsuffix"), TestCase("prefix:name",
                                    "prefix",
                                    "name",
                                    null)]
  public void PermissionFromStringShouldMatch(string             actionName,
                                              [CanBeNull] string prefix,
                                              [CanBeNull] string name,
                                              [CanBeNull] string suffix)
  {
    var perm = new Permissions.Permission(actionName);
    Assert.AreEqual(perm.Prefix,
                    prefix);
    Assert.AreEqual(perm.Name,
                    name);
    Assert.AreEqual(perm.Suffix,
                    suffix);
  }

  [TestCase("prefix:name:suffix",
            "prefix:name",
            "suffix"), TestCase("prefix:name",
                                "prefix:name",
                                Permissions.Default)]
  public void PermissionToClaimShouldMatch(string actionName,
                                           string claimType,
                                           string claimValue)
  {
    var perm = new Permissions.Permission(actionName);
    Assert.AreEqual(perm.Claim.Type,
                    claimType);
    Assert.AreEqual(perm.Claim.Value,
                    claimValue);
  }

  [Test]
  public void PermissionCreationShouldThrow()
  {
    Assert.NotNull(Assert.Catch(() =>
                                {
                                  var _ = new Permissions.Permission("testprefix");
                                }));
  }
}
