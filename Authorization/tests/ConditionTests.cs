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

using NUnit.Framework;

namespace ArmoniK.Core.Authorization.Tests;

[TestFixture(TestOf = typeof(Condition))]
public class ConditionTests
{
  private sealed record StoredSession(string?      Owner,
                                      List<string> CreatedUnder,
                                      List<string> SharedWith,
                                      List<string> DelegatedTo);

  private static readonly SessionFields<StoredSession> Fields = new(session => session.Owner,
                                                                    session => session.CreatedUnder,
                                                                    session => session.SharedWith,
                                                                    session => session.DelegatedTo);

  private const string Caller         = "oid:alice";
  private const string GrantPrincipal = "role:modelling-team";
  private const string Literal        = "role:other-team";

  [TestCase("session.owner == request.caller",
            typeof(OwnerIsCaller))]
  [TestCase("\"role:team\" in session.createdUnder",
            typeof(LiteralInCreatedUnder))]
  [TestCase("grant.principal in session.createdUnder",
            typeof(GrantPrincipalInCreatedUnder))]
  [TestCase("request.caller in session.sharedWith",
            typeof(CallerInSharedWith))]
  [TestCase("request.caller in session.delegatedTo",
            typeof(CallerInDelegatedTo))]
  [TestCase("request.partition == \"research\"",
            typeof(PartitionIs))]
  public void EachFormParses(string text,
                             Type   term)
  {
    var condition = Condition.Parse(text);

    Assert.That(condition.Terms,
                Has.Count.EqualTo(1));
    Assert.That(condition.Terms[0],
                Is.TypeOf(term));
    Assert.That(condition.ToString(),
                Is.EqualTo(text));
  }

  [TestCase("session.owner==request.caller&&grant.principal in session.createdUnder",
            "session.owner == request.caller && grant.principal in session.createdUnder")]
  [TestCase("  request.partition   ==\t\"q\"  ",
            "request.partition == \"q\"")]
  public void WhitespaceIsNotSignificant(string text,
                                         string canonical)
    => Assert.That(Condition.Parse(text)
                            .ToString(),
                   Is.EqualTo(canonical));

  [TestCase("",
            "empty")]
  [TestCase("   ",
            "empty")]
  [TestCase("request.caller == session.owner",
            "not a supported form")]
  [TestCase("session.owner == \"oid:alice\"",
            "not a supported form")]
  [TestCase("\"a\" == \"a\"",
            "not a supported form")]
  [TestCase("session.createdUnder == request.caller",
            "use 'in'")]
  [TestCase("request.caller in session.owner",
            "list on the right")]
  [TestCase("session.sharedWith in session.createdUnder",
            "value on the left")]
  [TestCase("request.partition == \"a\" || request.partition == \"b\"",
            "Unexpected '|'")]
  [TestCase("!(session.owner == request.caller)",
            "Unexpected '!'")]
  [TestCase("(session.owner == request.caller)",
            "Unexpected '('")]
  [TestCase("session.owner == request.caller &&",
            "Incomplete term")]
  [TestCase("session.owner == request.caller session.owner == request.caller",
            "Expected '&&'")]
  [TestCase("session.id == \"s\"",
            "Unknown attribute 'session.id'")]
  [TestCase("request.partition = \"q\"",
            "Unexpected '='")]
  [TestCase("request.partition == \"\"",
            "cannot be empty")]
  [TestCase("request.partition == \"q",
            "Unterminated")]
  [TestCase("request.partition == \"a\\\"",
            "backslash")]
  [TestCase("request.partition == \"a\tb\"",
            "control character")]
  [TestCase("request.partition == \" q\"",
            "starts or ends with a space")]
  [TestCase("request.partition == \"q \"",
            "starts or ends with a space")]
  [TestCase("request.partition == \"q\u200Bq\"",
            "invisible character")]
  [TestCase("request.partition == 'q'",
            "Unexpected '''")]
  [TestCase("request.partition in session.createdUnder",
            "not a supported form")]
  public void TextsOutsideTheGrammarAreRefused(string text,
                                               string message)
    => Assert.That(() => Condition.Parse(text),
                   Throws.InstanceOf<InvalidConditionException>()
                         .With.Message.Contains(message));

  private static IEnumerable<TestCaseData> InvisibleLiterals()
    => GrantValidatorTests.InvisibleNames.Select(name => new TestCaseData($"request.partition == \"{name.Text}\"").SetArgDisplayNames(name.Name));

  private static IEnumerable<TestCaseData> VisibleLiterals()
    => GrantValidatorTests.VisibleNames.Select(name => new TestCaseData($"\"role:{name.Text}\" in session.createdUnder").SetArgDisplayNames(name.Name));

  [TestCaseSource(nameof(InvisibleLiterals))]
  public void ALiteralWithAnInvisibleCharacterIsRefused(string text)
    => Assert.That(() => Condition.Parse(text),
                   Throws.InstanceOf<InvalidConditionException>());

  [TestCaseSource(nameof(VisibleLiterals))]
  public void ALiteralThatReadsAsItComparesIsAccepted(string text)
    => Assert.That(() => Condition.Parse(text),
                   Throws.Nothing);

  [Test]
  public void RenderingWithoutLiteralsHidesThem()
    => Assert.That(Condition.Parse("\"role:x\" in session.createdUnder && session.owner == request.caller")
                            .Render(false),
                   Is.EqualTo("\"?\" in session.createdUnder && session.owner == request.caller"));

  [Test]
  public void ParsingTheRenderingGivesTheSameCondition()
  {
    foreach (var condition in Conjunctions(3))
    {
      Assert.That(Condition.Parse(condition.ToString()),
                  Is.EqualTo(condition),
                  condition.ToString());
    }
  }

  [Test]
  public void ATermReadingThePartitionHoldsOnNoSession()
  {
    var condition = Condition.Parse("request.partition == \"q\"");

    Assert.That(condition.Eval(null,
                               new RequestFacts(Caller,
                                                "q"),
                               GrantPrincipal),
                Is.True);
    Assert.That(condition.Eval(null,
                               new RequestFacts(Caller,
                                                null),
                               GrantPrincipal),
                Is.False);
    Assert.That(SessionPredicate.FromCondition(condition,
                                               Fields,
                                               Caller,
                                               GrantPrincipal)
                                .Compile()(new StoredSession(Caller,
                                                             [],
                                                             [],
                                                             [])),
                Is.False);
  }

  [Test]
  public void ATermReadingTheSessionDoesNotHoldWithoutSession()
  {
    foreach (var text in new[]
                         {
                           "session.owner == request.caller",
                           $"\"{Literal}\" in session.createdUnder",
                           "grant.principal in session.createdUnder",
                           "request.caller in session.sharedWith",
                           "request.caller in session.delegatedTo",
                         })
    {
      Assert.That(Condition.Parse(text)
                           .Eval(null,
                                 new RequestFacts(Caller,
                                                  "q"),
                                 GrantPrincipal),
                  Is.False,
                  text);
    }
  }

  [Test]
  public void ASessionWithoutOwnerIsOwnedByNobody()
    => Assert.That(Condition.Parse("session.owner == request.caller")
                            .Eval(new SessionFacts(null,
                                                   [],
                                                   [],
                                                   []),
                                  new RequestFacts(Caller,
                                                   null),
                                  GrantPrincipal),
                   Is.False);

  /// <summary>
  ///   Every session term depends on one fact. Enumerating every assignment of the facts covers every session a
  ///   conjunction can meet, so agreement here means the evaluation and the listing predicate select the same
  ///   sessions.
  /// </summary>
  [Test]
  public void EvaluationAndPredicateAgreeOnEverySession()
  {
    var checkedPairs = 0;

    foreach (var condition in Conjunctions(3)
               .Where(c => !c.ReadsPartition))
    {
      var predicate = SessionPredicate.FromCondition(condition,
                                                     Fields,
                                                     Caller,
                                                     GrantPrincipal)
                                      .Compile();

      foreach (var (facts, stored) in Sessions())
      {
        Assert.That(predicate(stored),
                    Is.EqualTo(condition.Eval(facts,
                                              new RequestFacts(Caller,
                                                               null),
                                              GrantPrincipal)),
                    $"{condition} on {stored}");
        checkedPairs++;
      }
    }

    // 5 session terms, conjunctions of 1 to 3 distinct terms, 2^5 sessions
    Assert.That(checkedPairs,
                Is.EqualTo((5 + 10 + 10) * 32));
  }

  private static IEnumerable<(SessionFacts, StoredSession)> Sessions()
  {
    for (var mask = 0; mask < 32; mask++)
    {
      var owner = (mask & 1) != 0
                    ? Caller
                    : "oid:bob";
      var createdUnder = new List<string>();
      if ((mask & 2) != 0)
      {
        createdUnder.Add(Literal);
      }

      if ((mask & 4) != 0)
      {
        createdUnder.Add(GrantPrincipal);
      }

      var sharedWith = (mask & 8) != 0
                         ? new List<string>
                           {
                             Caller,
                           }
                         : new List<string>
                           {
                             "oid:bob",
                           };
      var delegatedTo = (mask & 16) != 0
                          ? new List<string>
                            {
                              Caller,
                            }
                          : new List<string>();

      yield return (new SessionFacts(owner,
                                     createdUnder,
                                     sharedWith,
                                     delegatedTo), new StoredSession(owner,
                                                                     createdUnder,
                                                                     sharedWith,
                                                                     delegatedTo));
    }
  }

  private static readonly string[] TermTexts =
  {
    "session.owner == request.caller",
    $"\"{Literal}\" in session.createdUnder",
    "grant.principal in session.createdUnder",
    "request.caller in session.sharedWith",
    "request.caller in session.delegatedTo",
    "request.partition == \"q\"",
  };

  private static IEnumerable<Condition> Conjunctions(int maxTerms)
  {
    IEnumerable<IEnumerable<int>> Combinations(int start,
                                               int size)
      => size == 0
           ? new[]
             {
               Enumerable.Empty<int>(),
             }
           : Enumerable.Range(start,
                              TermTexts.Length - start)
                       .SelectMany(i => Combinations(i    + 1,
                                                     size - 1)
                                     .Select(rest => rest.Prepend(i)));

    return Enumerable.Range(1,
                            maxTerms)
                     .SelectMany(size => Combinations(0,
                                                      size))
                     .Select(indices => Condition.Parse(string.Join(" && ",
                                                                    indices.Select(i => TermTexts[i]))));
  }
}
