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

/// <summary>
///   Decisions over the grants of the worked example of the access model, section 9
/// </summary>
[TestFixture(TestOf = typeof(DecisionEngine))]
public class DecisionEngineTests
{
  private sealed record StoredSession(string       Id,
                                      string?      Owner,
                                      List<string> CreatedUnder,
                                      List<string> SharedWith,
                                      List<string> DelegatedTo)
  {
    public SessionFacts Facts
      => new(Owner,
             CreatedUnder,
             SharedWith,
             DelegatedTo);
  }

  private static readonly SessionFields<StoredSession> Fields = new(session => session.Owner,
                                                                    session => session.CreatedUnder,
                                                                    session => session.SharedWith,
                                                                    session => session.DelegatedTo);

  private static readonly DecisionEngine Engine = DecisionEngine.Build(GrantValidatorTests.WorkedExample.Append(new GrantDefinition("prod",
                                                                                                                                     "role:research-users",
                                                                                                                                     "session-create",
                                                                                                                                     "request.partition == \"prod\"")),
                                                                       GrantValidatorTests.Sets,
                                                                       out _);

  private static readonly Caller Alice = new(Principal.Parse("oid:alice"),
                                             new[]
                                             {
                                               Principal.Parse("role:modelling-team"),
                                               Principal.Parse("role:research-users"),
                                             });

  private static readonly Caller Bob = new(Principal.Parse("oid:bob"),
                                           new[]
                                           {
                                             Principal.Parse("role:modelling-team"),
                                           });

  private static readonly Caller Lead = new(Principal.Parse("oid:lead"),
                                            new[]
                                            {
                                              Principal.Parse("role:lead"),
                                            });

  private static readonly Caller Support = new(Principal.Parse("oid:support"),
                                               new[]
                                               {
                                                 Principal.Parse("role:support"),
                                               });

  private static readonly Caller Reviewer = new(Principal.Parse("cn:reviewer"),
                                                new[]
                                                {
                                                  Principal.Parse("role:reviewer"),
                                                });

  private static readonly Caller Admin = new(Principal.Parse("oid:admin"),
                                             new[]
                                             {
                                               Principal.Parse("role:platform-team"),
                                             });

  private static readonly Caller Stranger = new(Principal.Parse("oid:stranger"),
                                                []);

  private static readonly Caller Carol = new(Principal.Parse("oid:carol"),
                                             []);

  private static readonly Caller Dave = new(Principal.Parse("oid:dave"),
                                            []);

  // Alice's session, created for the modelling team, shared with Carol and delegated to Dave
  private static readonly StoredSession AliceSession = new("s-alice",
                                                           "oid:alice",
                                                           ["role:modelling-team"],
                                                           ["oid:carol"],
                                                           ["oid:dave"]);

  // A session of another team
  private static readonly StoredSession OtherSession = new("s-other",
                                                           "oid:eve",
                                                           ["role:other-team"],
                                                           [],
                                                           []);

  private static Decision Decide(Caller       caller,
                                 Permission   required,
                                 StoredSession session,
                                 Precondition precondition = Precondition.None)
    => Engine.DecideOnSession(caller,
                              required,
                              session.Facts,
                              precondition);

  [Test]
  public void AliceCreatesASessionCoveredByTwoGrantsForTwoRoles()
  {
    var creation = Engine.DecideSessionCreation(Alice,
                                                new[]
                                                {
                                                  "research",
                                                  "prod",
                                                });

    Assert.That(creation.Decision.IsAllowed,
                Is.True);
    Assert.That(creation.Decision.AllowingGrants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "prod",
                             "submit",
                           }));
    Assert.That(creation.CreatedUnder,
                Is.EqualTo(new[]
                           {
                             "role:modelling-team",
                             "role:research-users",
                           }));
  }

  [Test]
  public void APartitionNotCoveredRefusesTheWholeCreation()
  {
    var creation = Engine.DecideSessionCreation(Alice,
                                                new[]
                                                {
                                                  "research",
                                                  "prod",
                                                  "gpu",
                                                });

    Assert.That(creation.Decision.Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(creation.Decision.Reason,
                Is.EqualTo(DenialReason.PartitionNotCovered));
    Assert.That(creation.Decision.Detail,
                Is.EqualTo("gpu"));
    Assert.That(creation.CreatedUnder,
                Is.Empty);
  }

  [Test]
  public void ACallerWithoutGrantIsToldNothingExistsWhenCreating()
    => Assert.That(Engine.DecideSessionCreation(Stranger,
                                                new[]
                                                {
                                                  "research",
                                                })
                         .Decision.Outcome,
                   Is.EqualTo(DecisionOutcome.NotFound));

  [Test]
  public void AnUnconditionedGrantIsRecordedInCreatedUnder()
    => Assert.That(Engine.DecideSessionCreation(Admin,
                                                new[]
                                                {
                                                  "anything",
                                                })
                         .CreatedUnder,
                   Is.EqualTo(new[]
                              {
                                "role:platform-team",
                              }));

  [Test]
  public void TheOwnerDoesEverythingSessionOwnHolds()
  {
    foreach (var permission in ShippedPermissionSets.SessionOwn.Permissions)
    {
      Assert.That(Decide(Alice,
                         permission,
                         AliceSession)
                    .IsAllowed,
                  Is.True,
                  permission.Name);
    }

    Assert.That(Decide(Alice,
                       Permission.DataImport,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
  }

  [Test]
  public void ATeammateCannotReachWorkTheyDoNotOwn()
    => Assert.That(Decide(Bob,
                          Permission.SessionRead,
                          AliceSession)
                     .Outcome,
                   Is.EqualTo(DecisionOutcome.NotFound));

  [Test]
  public void TheOwnerLosesTheSessionOnLeavingTheTeam()
  {
    var aliceElsewhere = new Caller(Alice.Identity,
                                    new[]
                                    {
                                      Principal.Parse("role:research-users"),
                                    });

    Assert.That(Decide(aliceElsewhere,
                       Permission.SessionRead,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
  }

  [Test]
  public void TheLeadSupervisesAndReadsResultsOfTheTeam()
  {
    Assert.That(Decide(Lead,
                       Permission.SessionCancel,
                       AliceSession)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Lead,
                       Permission.DataDownload,
                       AliceSession)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Lead,
                       Permission.TaskSubmit,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(Decide(Lead,
                       Permission.EventWatch,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(Decide(Lead,
                       Permission.SessionRead,
                       OtherSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
  }

  [Test]
  public void SupportStopsWorkWithoutSeeingData()
  {
    Assert.That(Decide(Support,
                       Permission.SessionPause,
                       AliceSession)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Support,
                       Permission.DataRead,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(Decide(Support,
                       Permission.SessionClose,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
  }

  [Test]
  public void TheReviewerSeesActivityAcrossTheClusterButNoContent()
  {
    Assert.That(Decide(Reviewer,
                       Permission.SessionRead,
                       OtherSession)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Reviewer,
                       Permission.DataDownload,
                       OtherSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
  }

  [Test]
  public void AShareOpensTheResultsToSomebodyHoldingNoGrantOfTheirOwn()
  {
    Assert.That(Engine.Admitted(Carol),
                Is.False);
    Assert.That(Decide(Carol,
                       Permission.SessionRead,
                       AliceSession)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Carol,
                       Permission.DataDownload,
                       AliceSession)
                  .AllowingGrants.Select(grant => grant.Id),
                Is.EqualTo(new[]
                           {
                             "share",
                           }));
    Assert.That(Decide(Carol,
                       Permission.TaskSubmit,
                       AliceSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(Decide(Carol,
                       Permission.SessionRead,
                       OtherSession)
                  .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
  }

  [Test]
  public void ADelegateRunsTheWorkButCannotPassItOn()
  {
    Assert.That(Decide(Dave,
                       Permission.TaskSubmit,
                       AliceSession)
                  .IsAllowed,
                Is.True);

    foreach (var permission in new[]
                               {
                                 Permission.SessionShare,
                                 Permission.SessionDelegate,
                                 Permission.SessionTransfer,
                               })
    {
      var decision = Decide(Dave,
                            permission,
                            AliceSession,
                            Precondition.OwnerOrUnconditional);
      Assert.That(decision.Outcome,
                  Is.EqualTo(DecisionOutcome.Forbidden),
                  permission.Name);
      Assert.That(decision.Reason,
                  Is.EqualTo(DenialReason.PreconditionFailed),
                  permission.Name);
    }
  }

  [Test]
  public void TheOwnerAndAClusterWideHolderPassThePrecondition()
  {
    Assert.That(Decide(Alice,
                       Permission.SessionShare,
                       AliceSession,
                       Precondition.OwnerOrUnconditional)
                  .IsAllowed,
                Is.True);
    Assert.That(Decide(Admin,
                       Permission.SessionTransfer,
                       OtherSession,
                       Precondition.OwnerOrUnconditional)
                  .IsAllowed,
                Is.True);
  }

  [Test]
  public void ThePreconditionIsCheckedAfterVisibility()
    => Assert.That(Decide(Bob,
                          Permission.SessionShare,
                          AliceSession,
                          Precondition.OwnerOrUnconditional)
                     .Outcome,
                   Is.EqualTo(DecisionOutcome.NotFound));

  [Test]
  public void ACallerWithoutGrantFindsNothing()
  {
    Assert.That(Engine.Admitted(Stranger),
                Is.False);
    Assert.That(Engine.DecideAdmission(Stranger)
                      .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));

    foreach (var session in new[]
                            {
                              AliceSession,
                              OtherSession,
                            })
    {
      foreach (var permission in Permission.All.Where(p => p.Target == PermissionTarget.Session))
      {
        Assert.That(Decide(Stranger,
                           permission,
                           session)
                      .Outcome,
                    Is.EqualTo(DecisionOutcome.NotFound));
      }
    }
  }

  [Test]
  public void AnUnconditionedSessionCreationDoesNotMakeEverySessionVisible()
  {
    var engine = DecisionEngine.Build(new[]
                                      {
                                        new GrantDefinition("open",
                                                            "role:team",
                                                            "session-create",
                                                            null),
                                      },
                                      GrantValidatorTests.Sets,
                                      out _);
    var caller = new Caller(Principal.Parse("oid:x"),
                            new[]
                            {
                              Principal.Parse("role:team"),
                            });

    Assert.That(engine.DecideOnSession(caller,
                                       Permission.SessionRead,
                                       OtherSession.Facts)
                      .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
  }

  [Test]
  public void ClusterAdminIsAllowedEverythingWhateverTheOtherSets()
  {
    foreach (var permission in Permission.All.Where(p => p.Target == PermissionTarget.Session && p != Permission.SessionCreate))
    {
      Assert.That(Decide(Admin,
                         permission,
                         OtherSession,
                         Precondition.OwnerOrUnconditional)
                    .IsAllowed,
                  Is.True,
                  permission.Name);
    }
  }

  [Test]
  public void PartitionsAreVisibleThroughSessionCreationOrPartitionReading()
  {
    var withoutRead = DecisionEngine.Build(new[]
                                           {
                                             new GrantDefinition("create-only",
                                                                 "role:team",
                                                                 "creator",
                                                                 "request.partition == \"research\""),
                                           },
                                           new Dictionary<string, PermissionSet>
                                           {
                                             ["creator"] = new("creator",
                                                               1,
                                                               new[]
                                                               {
                                                                 Permission.SessionCreate,
                                                               }),
                                           },
                                           out var rejected);
    Assert.That(rejected,
                Is.Empty);
    var caller = new Caller(Principal.Parse("oid:x"),
                            new[]
                            {
                              Principal.Parse("role:team"),
                            });

    Assert.That(withoutRead.DecideOnPartition(caller,
                                              Permission.PartitionRead,
                                              "research")
                           .Outcome,
                Is.EqualTo(DecisionOutcome.Forbidden));
    Assert.That(withoutRead.DecideOnPartition(caller,
                                              Permission.PartitionRead,
                                              "prod")
                           .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
    Assert.That(Engine.DecideOnPartition(Alice,
                                         Permission.PartitionRead,
                                         "research")
                      .IsAllowed,
                Is.True);
  }

  [Test]
  public void PartitionsListedAreTheOnesCovered()
  {
    Assert.That(Engine.Partitions(Alice,
                                  Permission.PartitionRead),
                Is.EqualTo(new PartitionFilter(false,
                                               new HashSet<string>
                                               {
                                                 "research",
                                                 "prod",
                                               }))
                  .Using<PartitionFilter>((a,
                                           b) => a.Unrestricted == b.Unrestricted && a.Partitions.SetEquals(b.Partitions)));
    Assert.That(Engine.Partitions(Reviewer,
                                  Permission.PartitionRead)
                      .Unrestricted,
                Is.True);
    Assert.That(Engine.Partitions(Lead,
                                  Permission.PartitionRead)
                      .Partitions,
                Is.Empty);
  }

  [Test]
  public void ListingsSelectTheSessionsWhereThePermissionIsHeld()
  {
    var sessions = new[]
                   {
                     AliceSession,
                     OtherSession,
                     new StoredSession("s-bob",
                                       "oid:bob",
                                       ["role:modelling-team"],
                                       [],
                                       []),
                   };

    IEnumerable<string> Listed(Caller     caller,
                               Permission permission)
    {
      var predicate = Engine.SessionPredicate(caller,
                                              permission,
                                              Fields)
                            .Compile();
      return sessions.Where(predicate)
                     .Select(session => session.Id);
    }

    Assert.That(Listed(Alice,
                       Permission.SessionRead),
                Is.EqualTo(new[]
                           {
                             "s-alice",
                           }));
    Assert.That(Listed(Lead,
                       Permission.SessionRead),
                Is.EquivalentTo(new[]
                                {
                                  "s-alice",
                                  "s-bob",
                                }));
    Assert.That(Listed(Support,
                       Permission.DataRead),
                Is.Empty);
    Assert.That(Listed(Carol,
                       Permission.DataRead),
                Is.EqualTo(new[]
                           {
                             "s-alice",
                           }));
    Assert.That(Listed(Reviewer,
                       Permission.SessionRead),
                Has.Exactly(3)
                   .Items);
    Assert.That(Listed(Stranger,
                       Permission.SessionRead),
                Is.Empty);

    // The listing selects exactly what the decision allows
    foreach (var caller in new[]
                           {
                             Alice,
                             Bob,
                             Lead,
                             Support,
                             Reviewer,
                             Admin,
                             Stranger,
                             Carol,
                             Dave,
                           })
    {
      foreach (var permission in Permission.All.Where(p => p.MakesSessionVisible))
      {
        Assert.That(Listed(caller,
                           permission),
                    Is.EquivalentTo(sessions.Where(session => Decide(caller,
                                                                     permission,
                                                                     session)
                                                                .IsAllowed)
                                            .Select(session => session.Id)),
                    $"{caller.Identity} {permission}");
      }
    }
  }

  [Test]
  public void TheDescriptionHidesLiteralsFromCallersWithoutGrantRead()
  {
    var description = Engine.Describe(Lead);

    Assert.That(description.Admitted,
                Is.True);
    Assert.That(description.PermissionSets,
                Is.EqualTo(new[]
                           {
                             "session-own",
                             "session-supervise",
                             "session-view",
                             "team-reader",
                           }));
    Assert.That(description.Permissions,
                Does.Contain("session:cancel@\"?\" in session.createdUnder"));
    Assert.That(description.Permissions,
                Does.Contain("data:read@request.caller in session.sharedWith"));
  }

  [Test]
  public void TheDescriptionShowsLiteralsToCallersWithGrantRead()
  {
    var engine = DecisionEngine.Build(new[]
                                      {
                                        new GrantDefinition("admin",
                                                            "role:admins",
                                                            "access-admin",
                                                            null),
                                        new GrantDefinition("supervise",
                                                            "role:admins",
                                                            "session-supervise",
                                                            "\"role:modelling-team\" in session.createdUnder"),
                                      },
                                      GrantValidatorTests.Sets,
                                      out _);
    var permissions = engine.Describe(new Caller(Principal.Parse("oid:x"),
                                                 new[]
                                                 {
                                                   Principal.Parse("role:admins"),
                                                 }))
                            .Permissions;

    Assert.That(permissions,
                Does.Contain("session:cancel@\"role:modelling-team\" in session.createdUnder"));
    Assert.That(permissions,
                Does.Contain("grant:manage@cluster"));
  }

  [Test]
  public void ACallerWithoutGrantIsDescribedAsHoldingNothing()
    => Assert.That(Engine.Describe(Carol),
                   Is.EqualTo(new CallerDescription(false,
                                                    [],
                                                    []))
                     .Using<CallerDescription>((a,
                                                b) => a.Admitted == b.Admitted && a.PermissionSets.SequenceEqual(b.PermissionSets) &&
                                                      a.Permissions.SequenceEqual(b.Permissions)));

  [Test]
  public void ARefusedGrantIsLeftOutRatherThanReadWithoutCondition()
  {
    var engine = DecisionEngine.Build(new[]
                                      {
                                        new GrantDefinition("broken",
                                                            "role:team",
                                                            "session-own",
                                                            "session.owner = request.caller"),
                                      },
                                      GrantValidatorTests.Sets,
                                      out var rejected);
    var caller = new Caller(Principal.Parse("oid:x"),
                            new[]
                            {
                              Principal.Parse("role:team"),
                            });

    Assert.That(rejected.Select(r => r.Grant.Id),
                Is.EqualTo(new[]
                           {
                             "broken",
                           }));
    Assert.That(engine.Grants,
                Is.Empty);
    Assert.That(engine.DecideOnSession(caller,
                                       Permission.SessionRead,
                                       OtherSession.Facts)
                      .Outcome,
                Is.EqualTo(DecisionOutcome.NotFound));
  }
}
