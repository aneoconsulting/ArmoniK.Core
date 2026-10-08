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

namespace ArmoniK.Core.Authorization;

/// <summary>
///   A grant as written: the principal it is for, the permission set it gives and the condition it is given under
/// </summary>
/// <param name="Id">Identifier of the grant</param>
/// <param name="Principal">Prefixed identifier of the principal, or <c>*</c></param>
/// <param name="PermissionSet">Name of the permission set</param>
/// <param name="Condition">Text of the condition, null for a grant without condition</param>
public sealed record GrantDefinition(string  Id,
                                     string  Principal,
                                     string  PermissionSet,
                                     string? Condition);

/// <summary>
///   A grant that passed validation
/// </summary>
/// <param name="Id">Identifier of the grant</param>
/// <param name="Principal">Principal of the grant</param>
/// <param name="PermissionSet">Permission set of the grant</param>
/// <param name="Condition">Condition of the grant, null for a grant without condition</param>
public sealed record Grant(string        Id,
                           Principal     Principal,
                           PermissionSet PermissionSet,
                           Condition?    Condition);

/// <summary>
///   Checks a grant before it is stored or used, so that a grant that can never match anything is refused rather than
///   silently conferring nothing
/// </summary>
public static class GrantValidator
{
  /// <summary>
  ///   Validate a grant
  /// </summary>
  /// <param name="definition">The grant as written</param>
  /// <param name="permissionSets">The permission sets, by name</param>
  /// <returns>The validated grant</returns>
  /// <exception cref="InvalidGrantException">The grant is refused</exception>
  public static Grant Validate(GrantDefinition                            definition,
                               IReadOnlyDictionary<string, PermissionSet> permissionSets)
  {
    var condition = ParseCondition(definition);

    if (!permissionSets.TryGetValue(definition.PermissionSet,
                                    out var set))
    {
      throw Refuse(definition,
                   InvalidGrantKind.Malformed,
                   $"permission set '{definition.PermissionSet}' does not exist");
    }

    if (!Principal.TryParse(definition.Principal,
                            out var principal))
    {
      throw Refuse(definition,
                   InvalidGrantKind.MissingPrefix,
                   $"'{definition.Principal}' is not a principal: expected '{Principal.DirectoryPrefix}', '{Principal.CertificatePrefix}' or '{Principal.RolePrefix}' followed by a name, or '{Principal.EveryoneValue}'");
    }

    foreach (var literal in condition?.Terms.OfType<LiteralInCreatedUnder>() ?? Enumerable.Empty<LiteralInCreatedUnder>())
    {
      if (!Principal.TryParse(literal.Principal,
                              out _))
      {
        throw Refuse(definition,
                     InvalidGrantKind.MissingPrefix,
                     $"\"{literal.Principal}\" in session.createdUnder does not name a principal");
      }
    }

    CheckReachable(definition,
                   set,
                   condition);

    if (principal.Kind == PrincipalKind.Everyone && condition is not null && condition.Terms.Any(term => term is GrantPrincipalInCreatedUnder) &&
        !condition.Terms.Any(term => term is OwnerIsCaller or CallerInSharedWith or CallerInDelegatedTo))
    {
      throw Refuse(definition,
                   InvalidGrantKind.Overreaching,
                   "a grant for '*' tests grant.principal in session.createdUnder without a term on the caller, so it would give every caller every session created under '*'");
    }

    return new Grant(definition.Id,
                     principal,
                     set,
                     condition);
  }

  private static Condition? ParseCondition(GrantDefinition definition)
  {
    if (definition.Condition is null)
    {
      return null;
    }

    try
    {
      return Condition.Parse(definition.Condition);
    }
    catch (InvalidConditionException e)
    {
      throw new InvalidGrantException(InvalidGrantKind.Malformed,
                                      $"Grant '{definition.Id}' is refused: {e.Message}",
                                      e);
    }
  }

  private static void CheckReachable(GrantDefinition definition,
                                     PermissionSet   set,
                                     Condition?      condition)
  {
    var permissions = set.Permissions;

    if (permissions.Count == 0)
    {
      throw Unreachable(definition,
                        $"permission set '{set.Name}' is empty");
    }

    if (condition is null)
    {
      return;
    }

    var partitions = condition.Terms.OfType<PartitionIs>()
                              .Select(term => term.Partition)
                              .Distinct()
                              .ToList();

    if (partitions.Count > 1)
    {
      throw Unreachable(definition,
                        $"the condition names several partitions ({string.Join(", ", partitions)}) and a request puts one in play at a time");
    }

    if (condition.ReadsPartition && condition.ReadsSession)
    {
      throw Unreachable(definition,
                        "the condition reads both the partition and the session, and no request puts a partition in play while resolving to a session");
    }

    if (permissions.All(permission => permission.Target == PermissionTarget.Administration))
    {
      throw Unreachable(definition,
                        $"every permission of '{set.Name}' is administrative, and no condition bears on grants or permission sets");
    }

    if (condition.ReadsSession && permissions.All(permission => permission.Target == PermissionTarget.Partition))
    {
      throw Unreachable(definition,
                        $"every permission of '{set.Name}' acts on a partition, and the condition selects sessions");
    }

    if (condition.ReadsPartition && !permissions.Any(permission => permission.PutsPartitionInPlay))
    {
      throw Unreachable(definition,
                        $"the condition names a partition, and no permission of '{set.Name}' puts one in play (only {Permission.SessionCreate} and {Permission.PartitionRead} do)");
    }

    if (condition.ReadsSession && permissions.Contains(Permission.SessionCreate))
    {
      throw Unreachable(definition,
                        $"'{set.Name}' carries {Permission.SessionCreate} under a condition that reads the session, and there is no session to read while one is being created");
    }
  }

  private static InvalidGrantException Unreachable(GrantDefinition definition,
                                                   string          reason)
    => Refuse(definition,
              InvalidGrantKind.Unreachable,
              reason);

  private static InvalidGrantException Refuse(GrantDefinition  definition,
                                              InvalidGrantKind kind,
                                              string           reason)
    => new(kind,
           $"Grant '{definition.Id}' is refused: {reason}");
}
