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

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

namespace ArmoniK.Core.Authorization.Tests;

[TestFixture(TestOf = typeof(GrantValidator))]
public class GrantValidatorTests
{
  private const string Ownership = "session.owner == request.caller && grant.principal in session.createdUnder";

  internal static readonly IReadOnlyDictionary<string, PermissionSet> Sets = ExampleSets.All.Concat(new[]
                                                                                                              {
                                                                                                                PermissionSet.FromNames("team-reader",
                                                                                                                                        1,
                                                                                                                                        new[]
                                                                                                                                        {
                                                                                                                                          "data:read",
                                                                                                                                          "data:download",
                                                                                                                                        }),
                                                                                                                PermissionSet.FromNames("compliance-reader",
                                                                                                                                        1,
                                                                                                                                        new[]
                                                                                                                                        {
                                                                                                                                          "session:read",
                                                                                                                                          "task:read",
                                                                                                                                          "data:read",
                                                                                                                                          "partition:read",
                                                                                                                                        }),
                                                                                                                PermissionSet.FromNames("partition-admin",
                                                                                                                                        1,
                                                                                                                                        new[]
                                                                                                                                        {
                                                                                                                                          "partition:create",
                                                                                                                                          "partition:delete",
                                                                                                                                          "partition:modify",
                                                                                                                                        }),
                                                                                                                PermissionSet.FromNames("empty",
                                                                                                                                        1,
                                                                                                                                        []),
                                                                                                                PermissionSet.FromNames("partition-and-access",
                                                                                                                                        1,
                                                                                                                                        new[]
                                                                                                                                        {
                                                                                                                                          "partition:modify",
                                                                                                                                          "grant:read",
                                                                                                                                        }),
                                                                                                              })
                                                                                                  .ToDictionary(set => set.Name);

  // Access model v0.98, section 9
  internal static readonly GrantDefinition[] WorkedExample =
  {
    new("submit",
        "role:modelling-team",
        "session-create",
        "request.partition == \"research\""),
    new("own",
        "role:modelling-team",
        "session-own",
        Ownership),
    new("lead-supervise",
        "role:lead",
        "session-supervise",
        "\"role:modelling-team\" in session.createdUnder"),
    new("lead-read",
        "role:lead",
        "team-reader",
        "\"role:modelling-team\" in session.createdUnder"),
    new("share",
        "*",
        "session-view",
        "request.caller in session.sharedWith"),
    new("delegate",
        "*",
        "session-own",
        "request.caller in session.delegatedTo"),
    new("auditor",
        "role:reviewer",
        "compliance-reader",
        null),
    new("support",
        "role:support",
        "session-supervise",
        "\"role:modelling-team\" in session.createdUnder"),
    new("platform",
        "role:platform-team",
        "cluster-admin",
        null),
  };

  private static Grant Validate(string  principal,
                                string  set,
                                string? condition)
    => GrantValidator.Validate(new GrantDefinition("g",
                                                   principal,
                                                   set,
                                                   condition),
                               Sets);

  private static void AssertRefused(string           principal,
                                    string           set,
                                    string?          condition,
                                    InvalidGrantKind kind)
    => Assert.That(() => Validate(principal,
                                  set,
                                  condition),
                   Throws.InstanceOf<InvalidGrantException>()
                         .With.Property(nameof(InvalidGrantException.Kind))
                         .EqualTo(kind));

  [Test]
  public void EveryGrantOfTheWorkedExampleIsAccepted()
  {
    foreach (var definition in WorkedExample)
    {
      Assert.That(() => GrantValidator.Validate(definition,
                                                Sets),
                  Throws.Nothing,
                  definition.Id);
    }
  }

  [Test]
  public void AValidGrantKeepsWhatWasWritten()
  {
    var grant = Validate("role:modelling-team",
                         "session-own",
                         Ownership);

    Assert.That(grant.Principal,
                Is.EqualTo(Principal.Parse("role:modelling-team")));
    Assert.That(grant.PermissionSet,
                Is.SameAs(ExampleSets.SessionOwn));
    Assert.That(grant.Condition!.ToString(),
                Is.EqualTo(Ownership));
  }

  [TestCase("session.owner = request.caller")]
  [TestCase("")]
  [TestCase(" ")]
  [TestCase("session.createdUnder == request.caller")]
  public void AConditionOutsideTheGrammarIsMalformed(string condition)
    => AssertRefused("role:team",
                     "session-own",
                     condition,
                     InvalidGrantKind.Malformed);

  [Test]
  public void AnUnknownSetIsMalformed()
    => AssertRefused("role:team",
                     "session-owner",
                     null,
                     InvalidGrantKind.Malformed);

  [Test]
  public void MalformedIsReportedBeforeAMissingPrefix()
    => AssertRefused("team",
                     "session-owner",
                     null,
                     InvalidGrantKind.Malformed);

  [TestCase("team")]
  [TestCase("ou:supervisors")]
  [TestCase("oid:")]
  public void APrincipalWithoutPrefixIsRefused(string principal)
    => AssertRefused(principal,
                     "session-view",
                     null,
                     InvalidGrantKind.MissingPrefix);

  [Test]
  public void ALiteralWithoutPrefixIsRefused()
    => AssertRefused("role:lead",
                     "session-supervise",
                     "\"modelling-team\" in session.createdUnder",
                     InvalidGrantKind.MissingPrefix);

  [Test]
  public void TheEveryoneLiteralIsAccepted()
    => Assert.That(() => Validate("role:lead",
                                  "session-supervise",
                                  "\"*\" in session.createdUnder"),
                   Throws.Nothing);

  [Test]
  public void PartitionPermissionsUnderASessionConditionAreUnreachable()
  {
    AssertRefused("role:ops",
                  "partition-admin",
                  "\"role:team\" in session.createdUnder",
                  InvalidGrantKind.Unreachable);
    Assert.That(() => Validate("role:ops",
                               "partition-admin",
                               null),
                Throws.Nothing);
  }

  [Test]
  public void PartitionAndAdministrativePermissionsUnderASessionConditionAreUnreachable()
    => AssertRefused("role:ops",
                     "partition-and-access",
                     "session.owner == request.caller",
                     InvalidGrantKind.Unreachable);

  [Test]
  public void APartitionConditionWithoutPermissionPuttingOneInPlayIsUnreachable()
  {
    AssertRefused("role:team",
                  "session-own",
                  "request.partition == \"research\"",
                  InvalidGrantKind.Unreachable);
    AssertRefused("role:ops",
                  "partition-admin",
                  "request.partition == \"research\"",
                  InvalidGrantKind.Unreachable);
    Assert.That(() => Validate("role:team",
                               "compliance-reader",
                               "request.partition == \"research\""),
                Throws.Nothing);
  }

  [Test]
  public void AdministrativePermissionsUnderAConditionAreUnreachable()
  {
    AssertRefused("role:admins",
                  "access-admin",
                  "\"role:team\" in session.createdUnder",
                  InvalidGrantKind.Unreachable);
    AssertRefused("role:admins",
                  "access-admin",
                  "request.partition == \"research\"",
                  InvalidGrantKind.Unreachable);
    Assert.That(() => Validate("role:admins",
                               "access-admin",
                               null),
                Throws.Nothing);
  }

  [Test]
  public void SessionCreationUnderASessionConditionIsUnreachable()
  {
    AssertRefused("role:team",
                  "session-create",
                  "session.owner == request.caller",
                  InvalidGrantKind.Unreachable);
    AssertRefused("role:team",
                  "cluster-admin",
                  Ownership,
                  InvalidGrantKind.Unreachable);
  }

  [Test]
  public void TwoPartitionsInOneConditionAreUnreachable()
    => AssertRefused("role:team",
                     "session-create",
                     "request.partition == \"a\" && request.partition == \"b\"",
                     InvalidGrantKind.Unreachable);

  [Test]
  public void TheSamePartitionTwiceIsAccepted()
    => Assert.That(() => Validate("role:team",
                                  "session-create",
                                  "request.partition == \"a\" && request.partition == \"a\""),
                   Throws.Nothing);

  [Test]
  public void PartitionAndSessionInOneConditionAreUnreachable()
    => AssertRefused("role:team",
                     "compliance-reader",
                     "request.partition == \"a\" && \"role:team\" in session.createdUnder",
                     InvalidGrantKind.Unreachable);

  [Test]
  public void AnEmptySetIsUnreachable()
    => AssertRefused("role:team",
                     "empty",
                     null,
                     InvalidGrantKind.Unreachable);

  [Test]
  public void EveryoneTestingItsOwnPrincipalAloneIsOverreaching()
  {
    AssertRefused("*",
                  "session-view",
                  "grant.principal in session.createdUnder",
                  InvalidGrantKind.Overreaching);
    Assert.That(() => Validate("*",
                               "session-own",
                               Ownership),
                Throws.Nothing);
    Assert.That(() => Validate("role:team",
                               "session-view",
                               "grant.principal in session.createdUnder"),
                Throws.Nothing);
  }
}
