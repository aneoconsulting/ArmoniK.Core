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

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Outcome of a decision
/// </summary>
public enum DecisionOutcome
{
  /// <summary>The request is allowed</summary>
  Allow,

  /// <summary>The request is refused on something the caller can see: 403</summary>
  Forbidden,

  /// <summary>The request is refused on something the caller cannot see, presented as absent: 404</summary>
  NotFound,
}

/// <summary>
///   Why a request was refused
/// </summary>
public enum DenialReason
{
  /// <summary>Not refused</summary>
  None,

  /// <summary>No surviving grant confers the required permission</summary>
  MissingPermission,

  /// <summary>No action at all on the resource would be permitted</summary>
  NotVisible,

  /// <summary>The cluster holds no grant naming the caller or one of its roles</summary>
  NotAdmitted,

  /// <summary>The caller is neither the owner of the session nor holds the permission without condition</summary>
  PreconditionFailed,

  /// <summary>A partition the request puts in play is not covered</summary>
  PartitionNotCovered,
}

/// <summary>
///   Precondition an operation carries on top of the grants
/// </summary>
public enum Precondition
{
  /// <summary>No precondition</summary>
  None,

  /// <summary>The caller must be the owner of the session, or hold the permission without condition</summary>
  OwnerOrUnconditional,
}

/// <summary>
///   The answer to a request
/// </summary>
/// <param name="Outcome">Whether the request is allowed</param>
/// <param name="Reason">Why the request was refused, <see cref="DenialReason.None" /> when allowed</param>
/// <param name="AllowingGrants">Every grant that conferred the required permission, empty when refused</param>
/// <param name="Detail">Partition that was not covered, when relevant</param>
public sealed record Decision(DecisionOutcome      Outcome,
                              DenialReason         Reason,
                              IReadOnlyList<Grant> AllowingGrants,
                              string?              Detail = null)
{
  /// <summary>Whether the request is allowed</summary>
  public bool IsAllowed
    => Outcome == DecisionOutcome.Allow;

  internal static Decision Allow(IReadOnlyList<Grant> grants)
    => new(DecisionOutcome.Allow,
           DenialReason.None,
           grants);

  internal static Decision Deny(DecisionOutcome outcome,
                                DenialReason    reason,
                                string?         detail = null)
    => new(outcome,
           reason,
           [],
           detail);
}

/// <summary>
///   The answer to a session creation
/// </summary>
/// <param name="Decision">The decision</param>
/// <param name="CreatedUnder">
///   Principals of every grant that conferred <c>session:create</c> and whose condition held, over every declared
///   partition; empty when refused
/// </param>
public sealed record SessionCreationDecision(Decision              Decision,
                                             IReadOnlyList<string> CreatedUnder);
