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

using System.Linq;

using NUnit.Framework;

namespace ArmoniK.Core.Authorization.Tests;

[TestFixture(TestOf = typeof(Permission))]
public class CatalogueTests
{
  // Access model v0.98, section 4, in order
  private static readonly string[] SpecCatalogue =
  {
    "session:create",
    "session:read",
    "session:pause",
    "session:resume",
    "session:stop-submission",
    "session:cancel",
    "session:close",
    "session:share",
    "session:delegate",
    "session:transfer",
    "session:purge",
    "session:delete",
    "task:read",
    "task:submit",
    "task:cancel",
    "data:read",
    "data:download",
    "data:declare",
    "data:provide",
    "data:import",
    "data:delete",
    "partition:read",
    "partition:create",
    "partition:delete",
    "partition:modify",
    "event:watch",
    "grant:read",
    "grant:manage",
    "permission-set:manage",
  };

  [Test]
  public void CatalogueIsTheOneOfTheAccessModel()
    => Assert.That(Permission.All.Select(permission => permission.Name),
                   Is.EqualTo(SpecCatalogue));

  [Test]
  public void EveryNameParsesBackToItsPermission()
  {
    foreach (var permission in Permission.All)
    {
      Assert.That(Permission.TryParse(permission.Name,
                                      out var parsed),
                  Is.True);
      Assert.That(parsed,
                  Is.SameAs(permission));
    }
  }

  [TestCase("Session:Read")]
  [TestCase("session:control")]
  [TestCase("session:read ")]
  [TestCase("")]
  public void UnknownNamesDoNotParse(string name)
    => Assert.That(Permission.TryParse(name,
                                       out _),
                   Is.False);

  [Test]
  public void OnlySessionCreationAndPartitionReadingPutAPartitionInPlay()
    => Assert.That(Permission.All.Where(permission => permission.PutsPartitionInPlay),
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.SessionCreate,
                                     Permission.PartitionRead,
                                   }));

  [Test]
  public void SessionCreationPartitionsAndAdministrationDoNotMakeASessionVisible()
    => Assert.That(Permission.All.Where(permission => !permission.MakesSessionVisible),
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.SessionCreate,
                                     Permission.PartitionRead,
                                     Permission.PartitionCreate,
                                     Permission.PartitionDelete,
                                     Permission.PartitionModify,
                                     Permission.GrantRead,
                                     Permission.GrantManage,
                                     Permission.PermissionSetManage,
                                   }));

  // Access model v0.98, section 5: the sets are written in the configuration, none is shipped
  [TestCase("session-view",
            "session:read,task:read,data:read,data:download,event:watch")]
  [TestCase("session-create",
            "session:create,partition:read")]
  [TestCase("session-own",
            "session:read,task:read,data:read,data:download,event:watch,task:submit,session:share,session:delegate,session:transfer,session:pause,session:resume,session:stop-submission,session:cancel,session:close,session:purge,session:delete,task:cancel,data:declare,data:provide,data:delete")]
  [TestCase("session-supervise",
            "session:read,session:pause,session:resume,session:stop-submission,session:cancel,task:read,task:cancel")]
  [TestCase("access-admin",
            "grant:read,grant:manage,permission-set:manage")]
  public void ExampleSetsHoldWhatTheAccessModelSays(string name,
                                                    string permissions)
    => Assert.That(ExampleSets.All.Single(s => s.Name == name)
                              .Permissions.Select(permission => permission.Name),
                   Is.EquivalentTo(permissions.Split(',')));

  [Test]
  public void EverythingIsTheWholeCatalogueAndIsKeptAsWritten()
  {
    var set = PermissionSet.FromNames("cluster-admin",
                                      1,
                                      [PermissionSet.Everything]);

    Assert.That(set.Permissions,
                Is.EquivalentTo(Permission.All));
    // Stored as written, so that versions with other catalogues store the same set
    Assert.That(set.Names,
                Is.EqualTo(new[]
                           {
                             PermissionSet.Everything,
                           }));
  }

  [Test]
  public void ASetAsStoredLeavesOutOnlyThePermissionsNotInTheCatalogue()
  {
    var set = PermissionSet.FromNames("written-by-a-later-version",
                                      2,
                                      ["session:read", "session:archive"],
                                      out var unknown);

    Assert.That(set.Permissions,
                Is.EquivalentTo(new[]
                                {
                                  Permission.SessionRead,
                                }));
    Assert.That(unknown,
                Is.EqualTo(new[]
                           {
                             "session:archive",
                           }));
    Assert.That(set.Names,
                Does.Contain("session:archive"));
  }

  [Test]
  public void ASetNamingAnUnknownPermissionIsRefused()
    => Assert.That(() => PermissionSet.FromNames("custom",
                                                 1,
                                                 new[]
                                                 {
                                                   "session:read",
                                                   "session:control",
                                                 }),
                   Throws.InstanceOf<InvalidPermissionSetException>()
                         .With.Message.Contains("'session:control'"));
}

[TestFixture(TestOf = typeof(Principal))]
public class PrincipalTests
{
  [TestCase("oid:8f2c1e94",
            PrincipalKind.DirectoryIdentity)]
  [TestCase("cn:client",
            PrincipalKind.CertificateIdentity)]
  [TestCase("role:modelling-team",
            PrincipalKind.Role)]
  [TestCase("role:a:b",
            PrincipalKind.Role)]
  [TestCase("*",
            PrincipalKind.Everyone)]
  public void PrefixedIdentifiersParse(string        value,
                                       PrincipalKind kind)
  {
    Assert.That(Principal.TryParse(value,
                                   out var principal),
                Is.True);
    Assert.That(principal!.Kind,
                Is.EqualTo(kind));
    Assert.That(principal.Value,
                Is.EqualTo(value));
  }

  [TestCase("alice")]
  [TestCase("oid:")]
  [TestCase("ou:supervisors")]
  [TestCase("OID:alice")]
  [TestCase("Role:team")]
  [TestCase("**")]
  [TestCase("role:te\nam")]
  [TestCase("")]
  public void IdentifiersWithoutAValidPrefixDoNotParse(string value)
    => Assert.That(Principal.TryParse(value,
                                      out _),
                   Is.False);

  [Test]
  public void ComparisonIsOrdinal()
    => Assert.That(Principal.Parse("role:Team"),
                   Is.Not.EqualTo(Principal.Parse("role:team")));

  [Test]
  public void ARoleIsNotAnIdentity()
  {
    Assert.That(Principal.Parse("role:team")
                         .IsIdentity,
                Is.False);
    Assert.That(Principal.Parse("cn:client")
                         .IsIdentity,
                Is.True);
    Assert.That(Principal.Everyone.IsIdentity,
                Is.False);
  }
}
