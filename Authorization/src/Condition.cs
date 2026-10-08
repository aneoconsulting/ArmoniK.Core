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

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Facts about the session a request resolves to, as read by a condition
/// </summary>
/// <param name="Owner">Principal recorded as owner, null when none was recorded</param>
/// <param name="CreatedUnder">Principals whose grants permitted the creation of the session</param>
/// <param name="SharedWith">Principals the session is shared with</param>
/// <param name="DelegatedTo">Principals the session is delegated to</param>
public sealed record SessionFacts(string?                     Owner,
                                  IReadOnlyCollection<string> CreatedUnder,
                                  IReadOnlyCollection<string> SharedWith,
                                  IReadOnlyCollection<string> DelegatedTo);

/// <summary>
///   Facts about the request, as read by a condition
/// </summary>
/// <param name="Caller">Principal of whoever makes the request</param>
/// <param name="Partition">Partition the request puts in play, null when it puts none</param>
public sealed record RequestFacts(string  Caller,
                                  string? Partition);

/// <summary>
///   One term of a condition. Terms are the closed set of forms of the access model
/// </summary>
public abstract record Term
{
  private protected Term()
  {
  }

  /// <summary>Whether the term reads the session</summary>
  public abstract bool ReadsSession { get; }

  /// <summary>Whether the term reads the partition of the request</summary>
  public bool ReadsPartition
    => !ReadsSession;

  /// <summary>
  ///   Evaluate the term
  /// </summary>
  /// <param name="session">The session, null when the request resolves to none</param>
  /// <param name="request">The request</param>
  /// <param name="grantPrincipal">The principal of the grant being evaluated</param>
  /// <returns>Whether the term holds</returns>
  public abstract bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal);

  /// <summary>
  ///   Render the term
  /// </summary>
  /// <param name="withLiterals">Whether literals are written, or replaced by <c>"?"</c></param>
  /// <returns>The canonical text of the term</returns>
  public abstract string Render(bool withLiterals);

  /// <inheritdoc />
  public sealed override string ToString()
    => Render(true);

  private protected static string Quote(string value,
                                        bool   withLiterals)
    => withLiterals
         ? $"\"{value}\""
         : "\"?\"";
}

/// <summary><c>session.owner == request.caller</c></summary>
public sealed record OwnerIsCaller : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => true;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => session?.Owner is not null && string.Equals(session.Owner,
                                                   request.Caller,
                                                   StringComparison.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => "session.owner == request.caller";
}

/// <summary><c>"P" in session.createdUnder</c></summary>
/// <param name="Principal">The principal written in the grant</param>
public sealed record LiteralInCreatedUnder(string Principal) : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => true;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => session is not null && session.CreatedUnder.Contains(Principal,
                                                           StringComparer.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => $"{Quote(Principal, withLiterals)} in session.createdUnder";
}

/// <summary><c>grant.principal in session.createdUnder</c></summary>
public sealed record GrantPrincipalInCreatedUnder : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => true;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => session is not null && session.CreatedUnder.Contains(grantPrincipal,
                                                           StringComparer.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => "grant.principal in session.createdUnder";
}

/// <summary><c>request.caller in session.sharedWith</c></summary>
public sealed record CallerInSharedWith : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => true;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => session is not null && session.SharedWith.Contains(request.Caller,
                                                         StringComparer.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => "request.caller in session.sharedWith";
}

/// <summary><c>request.caller in session.delegatedTo</c></summary>
public sealed record CallerInDelegatedTo : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => true;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => session is not null && session.DelegatedTo.Contains(request.Caller,
                                                          StringComparer.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => "request.caller in session.delegatedTo";
}

/// <summary><c>request.partition == "Q"</c></summary>
/// <param name="Partition">The partition written in the grant</param>
public sealed record PartitionIs(string Partition) : Term
{
  /// <inheritdoc />
  public override bool ReadsSession
    => false;

  /// <inheritdoc />
  public override bool Eval(SessionFacts? session,
                            RequestFacts  request,
                            string        grantPrincipal)
    => request.Partition is not null && string.Equals(request.Partition,
                                                      Partition,
                                                      StringComparison.Ordinal);

  /// <inheritdoc />
  public override string Render(bool withLiterals)
    => $"request.partition == {Quote(Partition, withLiterals)}";
}

/// <summary>
///   A condition: terms joined by <c>&amp;&amp;</c>, all of which must hold
/// </summary>
public sealed class Condition : IEquatable<Condition>
{
  internal Condition(IReadOnlyList<Term> terms)
  {
    if (terms.Count == 0)
    {
      throw new ArgumentException("A condition has at least one term",
                                  nameof(terms));
    }

    Terms = terms;
  }

  /// <summary>The terms, in the order they were written</summary>
  public IReadOnlyList<Term> Terms { get; }

  /// <summary>Whether a term reads the session</summary>
  public bool ReadsSession
    => Terms.Any(term => term.ReadsSession);

  /// <summary>Whether a term reads the partition of the request</summary>
  public bool ReadsPartition
    => Terms.Any(term => term.ReadsPartition);

  /// <inheritdoc />
  public bool Equals(Condition? other)
    => other is not null && Terms.SequenceEqual(other.Terms);

  /// <summary>
  ///   Parse a condition
  /// </summary>
  /// <param name="text">The text of the condition</param>
  /// <returns>The condition</returns>
  /// <exception cref="InvalidConditionException">The text does not belong to the grammar</exception>
  public static Condition Parse(string text)
    => ConditionParser.Parse(text);

  /// <summary>
  ///   Evaluate the condition
  /// </summary>
  /// <param name="session">The session, null when the request resolves to none</param>
  /// <param name="request">The request</param>
  /// <param name="grantPrincipal">The principal of the grant being evaluated</param>
  /// <returns>Whether every term holds</returns>
  public bool Eval(SessionFacts? session,
                   RequestFacts  request,
                   string        grantPrincipal)
    => Terms.All(term => term.Eval(session,
                                   request,
                                   grantPrincipal));

  /// <summary>
  ///   Render the condition
  /// </summary>
  /// <param name="withLiterals">Whether literals are written, or replaced by <c>"?"</c></param>
  /// <returns>The canonical text of the condition</returns>
  public string Render(bool withLiterals)
    => string.Join(" && ",
                   Terms.Select(term => term.Render(withLiterals)));

  /// <inheritdoc />
  public override bool Equals(object? obj)
    => Equals(obj as Condition);

  /// <inheritdoc />
  public override int GetHashCode()
    => Terms.Aggregate(0,
                       HashCode.Combine);

  /// <inheritdoc />
  public override string ToString()
    => Render(true);
}
