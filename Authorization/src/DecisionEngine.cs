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
using System.Linq.Expressions;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Whoever makes a request, as derived from the credential presented
/// </summary>
public sealed record Caller
{
  /// <summary>
  ///   Build a caller
  /// </summary>
  /// <param name="identity">The person or service account making the request</param>
  /// <param name="roles">The roles carried by the credential</param>
  /// <exception cref="ArgumentException">The identity is not a person or a service account, or a role is not a role</exception>
  public Caller(Principal              identity,
                IEnumerable<Principal> roles)
  {
    if (!identity.IsIdentity)
    {
      throw new ArgumentException($"'{identity}' is not a person or a service account",
                                  nameof(identity));
    }

    Identity = identity;
    Roles = roles.Distinct()
                 .ToList();

    if (Roles.Any(role => role.Kind != PrincipalKind.Role))
    {
      throw new ArgumentException("Every role of a caller must be prefixed with " + Principal.RolePrefix,
                                  nameof(roles));
    }

    Expansion = Roles.Select(role => role.Value)
                     .Prepend(Identity.Value)
                     .Append(Principal.EveryoneValue)
                     .ToList();
  }

  /// <summary>The person or service account making the request</summary>
  public Principal Identity { get; }

  /// <summary>The roles carried by the credential</summary>
  public IReadOnlyList<Principal> Roles { get; }

  /// <summary>The caller itself, every role it holds and <c>*</c></summary>
  public IReadOnlyList<string> Expansion { get; }
}

/// <summary>
///   Partitions a caller may list
/// </summary>
/// <param name="Unrestricted">Whether every partition may be listed</param>
/// <param name="Partitions">Partitions that may be listed when not unrestricted</param>
public sealed record PartitionFilter(bool                 Unrestricted,
                                     IReadOnlySet<string> Partitions);

/// <summary>
///   What a caller holds, as reported to the caller itself
/// </summary>
/// <param name="Admitted">Whether the cluster holds a grant naming the caller or one of its roles</param>
/// <param name="PermissionSets">Names of the permission sets held</param>
/// <param name="Permissions">
///   Permissions held, written <c>permission@cluster</c> when held without condition and
///   <c>permission@condition</c> otherwise
/// </param>
public sealed record CallerDescription(bool                  Admitted,
                                       IReadOnlyList<string> PermissionSets,
                                       IReadOnlyList<string> Permissions);

/// <summary>
///   The grants in force and the decisions taken on them
/// </summary>
/// <remarks>
///   Pure: decisions only depend on the grants of the snapshot and the facts given.
/// </remarks>
public sealed class DecisionEngine
{
  private readonly Dictionary<string, List<Grant>> byPrincipal_;

  /// <summary>
  ///   Build an engine over validated grants
  /// </summary>
  /// <param name="grants">The grants in force</param>
  public DecisionEngine(IEnumerable<Grant> grants)
  {
    Grants = grants.OrderBy(grant => grant.Id,
                            StringComparer.Ordinal)
                   .ToList();
    byPrincipal_ = Grants.GroupBy(grant => grant.Principal.Value,
                                  StringComparer.Ordinal)
                         .ToDictionary(group => group.Key,
                                       group => group.ToList(),
                                       StringComparer.Ordinal);
  }

  /// <summary>The grants in force, ordered by identifier</summary>
  public IReadOnlyList<Grant> Grants { get; }

  /// <summary>
  ///   Validate grant definitions and build an engine over those that pass
  /// </summary>
  /// <param name="definitions">The grants as written</param>
  /// <param name="permissionSets">The permission sets, by name</param>
  /// <param name="rejected">The grants refused, with the reason</param>
  /// <returns>The engine</returns>
  /// <remarks>
  ///   A refused grant is left out rather than read with a missing condition, which would give it the whole cluster.
  /// </remarks>
  public static DecisionEngine Build(IEnumerable<GrantDefinition>                                            definitions,
                                     IReadOnlyDictionary<string, PermissionSet>                              permissionSets,
                                     out IReadOnlyList<(GrantDefinition Grant, InvalidGrantException Error)> rejected)
  {
    var grants  = new List<Grant>();
    var refused = new List<(GrantDefinition, InvalidGrantException)>();

    foreach (var definition in definitions)
    {
      try
      {
        grants.Add(GrantValidator.Validate(definition,
                                           permissionSets));
      }
      catch (InvalidGrantException e)
      {
        refused.Add((definition, e));
      }
      catch (Exception e)
      {
        // A grant as stored may hold what no validation expects, such as a missing field: it is refused alone, rather
        // than leaving every request undecided
        refused.Add((definition, new InvalidGrantException(InvalidGrantKind.Malformed,
                                                           $"Grant '{definition.Id}' is refused: {e.Message}",
                                                           e)));
      }
    }

    rejected = refused;
    return new DecisionEngine(grants);
  }

  /// <summary>
  ///   The grants held by the caller, itself, its roles or <c>*</c>
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <returns>The grants held, ordered by identifier</returns>
  public IReadOnlyList<Grant> Held(Caller caller)
    => caller.Expansion.SelectMany(principal => byPrincipal_.GetValueOrDefault(principal) ?? [])
             .DistinctBy(grant => grant.Id)
             .OrderBy(grant => grant.Id,
                      StringComparer.Ordinal)
             .ToList();

  /// <summary>
  ///   Whether the cluster holds a grant naming the caller or one of its roles. Grants for <c>*</c> do not count
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <returns>Whether the caller is admitted</returns>
  public bool Admitted(Caller caller)
    => Held(caller)
      .Any(grant => grant.Principal.Kind != PrincipalKind.Everyone);

  /// <summary>
  ///   Whether the caller holds a permission through a grant without condition, that is across the cluster
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="permission">The permission</param>
  /// <returns>Whether the permission is held without condition</returns>
  public bool HoldsUnconditionally(Caller     caller,
                                   Permission permission)
    => Held(caller)
      .Any(grant => grant.Condition is null && grant.PermissionSet.Permissions.Contains(permission));

  /// <summary>
  ///   Decide an operation that requires no permission but admission
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <returns>The decision</returns>
  public Decision DecideAdmission(Caller caller)
    => Admitted(caller)
         ? Decision.Allow([])
         : Decision.Deny(DecisionOutcome.NotFound,
                         DenialReason.NotAdmitted);

  /// <summary>
  ///   Decide an operation on a session, or on a task or a data object of that session
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="required">The permission the operation requires</param>
  /// <param name="session">The session the request resolves to</param>
  /// <param name="precondition">The precondition the operation carries</param>
  /// <returns>The decision</returns>
  /// <remarks>
  ///   The session must be visible, then the precondition must hold, then a surviving grant must confer the permission.
  /// </remarks>
  public Decision DecideOnSession(Caller       caller,
                                  Permission   required,
                                  SessionFacts session,
                                  Precondition precondition = Precondition.None)
  {
    var held = Held(caller);
    var request = new RequestFacts(caller.Identity.Value,
                                   null);
    var surviving = held.Where(grant => Holds(grant,
                                              session,
                                              request))
                        .ToList();

    if (!surviving.Any(grant => grant.PermissionSet.Permissions.Any(permission => permission.MakesSessionVisible)))
    {
      return Decision.Deny(DecisionOutcome.NotFound,
                           DenialReason.NotVisible);
    }

    if (precondition == Precondition.OwnerOrUnconditional && !string.Equals(session.Owner,
                                                                            caller.Identity.Value,
                                                                            StringComparison.Ordinal) &&
        !held.Any(grant => grant.Condition is null && grant.PermissionSet.Permissions.Contains(required)))
    {
      return Decision.Deny(DecisionOutcome.Forbidden,
                           DenialReason.PreconditionFailed);
    }

    var allowing = surviving.Where(grant => grant.PermissionSet.Permissions.Contains(required))
                            .ToList();

    return allowing.Count == 0
             ? Decision.Deny(DecisionOutcome.Forbidden,
                             DenialReason.MissingPermission)
             : Decision.Allow(allowing);
  }

  /// <summary>
  ///   Decide the creation of a session declaring partitions, one pass per partition
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="partitions">The partitions the session declares</param>
  /// <returns>The decision, and the principals the session is created under when allowed</returns>
  /// <remarks>
  ///   The creation is refused as soon as one partition is not covered. A refusal is a 403 for an admitted caller and a
  ///   404 otherwise.
  /// </remarks>
  public SessionCreationDecision DecideSessionCreation(Caller                      caller,
                                                       IReadOnlyCollection<string> partitions)
  {
    if (partitions.Count == 0)
    {
      throw new ArgumentException("A session declares at least one partition",
                                  nameof(partitions));
    }

    var required     = Permission.SessionCreate;
    var held         = Held(caller);
    var allowing     = new List<Grant>();
    var createdUnder = new List<string>();

    foreach (var partition in partitions)
    {
      var request = new RequestFacts(caller.Identity.Value,
                                     partition);
      var pass = held.Where(grant => grant.PermissionSet.Permissions.Contains(required) && Holds(grant,
                                                                                                 null,
                                                                                                 request))
                     .ToList();

      if (pass.Count == 0)
      {
        return new SessionCreationDecision(Decision.Deny(Admitted(caller)
                                                           ? DecisionOutcome.Forbidden
                                                           : DecisionOutcome.NotFound,
                                                         DenialReason.PartitionNotCovered,
                                                         partition),
                                           []);
      }

      allowing.AddRange(pass);
      createdUnder.AddRange(pass.Select(grant => grant.Principal.Value));
    }

    return new SessionCreationDecision(Decision.Allow(allowing.DistinctBy(grant => grant.Id)
                                                              .OrderBy(grant => grant.Id,
                                                                       StringComparer.Ordinal)
                                                              .ToList()),
                                       createdUnder.Distinct(StringComparer.Ordinal)
                                                   .ToList());
  }

  /// <summary>
  ///   Decide an operation on one partition
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="required">The permission the operation requires</param>
  /// <param name="partition">The partition</param>
  /// <returns>The decision</returns>
  /// <remarks>
  ///   A partition is visible when a surviving grant confers <c>session:create</c> or <c>partition:read</c> on it.
  /// </remarks>
  public Decision DecideOnPartition(Caller     caller,
                                    Permission required,
                                    string     partition)
  {
    var held = Held(caller);
    var inPlay = new RequestFacts(caller.Identity.Value,
                                  partition);
    var surviving = held.Where(grant => Holds(grant,
                                              null,
                                              inPlay))
                        .ToList();

    if (!surviving.Any(grant => grant.PermissionSet.Permissions.Contains(Permission.SessionCreate) ||
                                grant.PermissionSet.Permissions.Contains(Permission.PartitionRead)))
    {
      return Decision.Deny(DecisionOutcome.NotFound,
                           DenialReason.NotVisible);
    }

    // A permission that puts no partition in play is only reached by grants that do not read the partition
    var request = required.PutsPartitionInPlay
                    ? inPlay
                    : inPlay with
                      {
                        Partition = null,
                      };
    var allowing = held.Where(grant => grant.PermissionSet.Permissions.Contains(required) && Holds(grant,
                                                                                                   null,
                                                                                                   request))
                       .ToList();

    return allowing.Count == 0
             ? Decision.Deny(DecisionOutcome.Forbidden,
                             DenialReason.MissingPermission)
             : Decision.Allow(allowing);
  }

  /// <summary>
  ///   Build the predicate selecting the stored sessions on which the caller holds a permission
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="permission">The permission</param>
  /// <param name="fields">Where the session fields live</param>
  /// <typeparam name="TSession">The stored session type</typeparam>
  /// <returns>The predicate, constant true when the permission is held without condition</returns>
  public Expression<Func<TSession, bool>> SessionPredicate<TSession>(Caller                  caller,
                                                                     Permission              permission,
                                                                     SessionFields<TSession> fields)
  {
    var grants = Held(caller)
                 .Where(grant => grant.PermissionSet.Permissions.Contains(permission))
                 .ToList();

    if (grants.Any(grant => grant.Condition is null))
    {
      return _ => true;
    }

    return Authorization.SessionPredicate.Any(grants.Where(grant => grant.Condition!.ReadsSession)
                                                    .Select(grant => Authorization.SessionPredicate.FromCondition(grant.Condition!,
                                                                                                                  fields,
                                                                                                                  caller.Identity.Value,
                                                                                                                  grant.Principal.Value)));
  }

  /// <summary>
  ///   The partitions on which the caller holds a permission putting a partition in play
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <param name="permission">The permission, usually <c>partition:read</c></param>
  /// <returns>The partitions</returns>
  public PartitionFilter Partitions(Caller     caller,
                                    Permission permission)
  {
    var grants = Held(caller)
                 .Where(grant => grant.PermissionSet.Permissions.Contains(permission))
                 .ToList();

    if (grants.Any(grant => grant.Condition is null))
    {
      return new PartitionFilter(true,
                                 new HashSet<string>());
    }

    // A permission that puts no partition in play is only reached by grants that do not read the partition, as when
    // deciding on a partition
    if (!permission.PutsPartitionInPlay)
    {
      return new PartitionFilter(false,
                                 new HashSet<string>());
    }

    return new PartitionFilter(false,
                               grants.Where(grant => !grant.Condition!.ReadsSession)
                                     .SelectMany(grant => grant.Condition!.Terms.OfType<PartitionIs>())
                                     .Select(term => term.Partition)
                                     .ToHashSet(StringComparer.Ordinal));
  }

  /// <summary>
  ///   Describe what the caller holds, for the caller itself
  /// </summary>
  /// <param name="caller">The caller</param>
  /// <returns>
  ///   Nothing for a caller that is not admitted. Otherwise the sets and permissions held, with the literals of the
  ///   conditions written only for a caller holding <c>grant:read</c>
  /// </returns>
  public CallerDescription Describe(Caller caller)
  {
    if (!Admitted(caller))
    {
      return new CallerDescription(false,
                                   [],
                                   []);
    }

    var held = Held(caller);
    var withLiterals = HoldsUnconditionally(caller,
                                            Permission.GrantRead);

    return new CallerDescription(true,
                                 held.Select(grant => grant.PermissionSet.Name)
                                     .Distinct(StringComparer.Ordinal)
                                     .OrderBy(name => name,
                                              StringComparer.Ordinal)
                                     .ToList(),
                                 held.SelectMany(grant => grant.PermissionSet.Permissions.Select(permission => permission.Name + "@" + (grant.Condition is null
                                                                                                                                          ? "cluster"
                                                                                                                                          : grant.Condition
                                                                                                                                                 .Render(withLiterals))))
                                     .Distinct(StringComparer.Ordinal)
                                     .OrderBy(entry => entry,
                                              StringComparer.Ordinal)
                                     .ToList());
  }

  private static bool Holds(Grant         grant,
                            SessionFacts? session,
                            RequestFacts  request)
    => grant.Condition is null || grant.Condition.Eval(session,
                                                       request,
                                                       grant.Principal.Value);
}
