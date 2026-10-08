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

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Injection.Options;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Provides the decision engine over the grants in force
/// </summary>
public interface IDecisionEngineProvider
{
  /// <summary>
  ///   The decision engine over the grants in force
  /// </summary>
  DecisionEngine Engine { get; }
}

/// <summary>
///   Decision engine over the grants and permission sets of the configuration
/// </summary>
public class ConfiguredDecisionEngineProvider : IDecisionEngineProvider
{
  /// <summary>
  ///   Build the engine from the configuration
  /// </summary>
  /// <param name="options">Options of the authorization</param>
  /// <exception cref="InvalidOperationException">A permission set or a grant of the configuration is refused</exception>
  public ConfiguredDecisionEngineProvider(AuthorizationOptions options)
  {
    var sets = PermissionSets(options.PermissionSets);

    Engine = DecisionEngine.Build(Grants(options.Grants),
                                  sets,
                                  out var rejected);

    if (rejected.Count != 0)
    {
      throw new InvalidOperationException($"The configuration holds grants that are refused: {string.Join("; ", rejected.Select(r => r.Error.Message))}");
    }
  }

  /// <inheritdoc />
  public DecisionEngine Engine { get; }

  /// <summary>
  ///   The shipped permission sets, redefined or completed by the configuration
  /// </summary>
  /// <param name="configured">Permission sets of the configuration</param>
  /// <returns>The permission sets, by name</returns>
  /// <exception cref="InvalidOperationException">A permission set of the configuration is refused</exception>
  public static IReadOnlyDictionary<string, PermissionSet> PermissionSets(IEnumerable<PermissionSetOption> configured)
  {
    var sets = ShippedPermissionSets.All.ToDictionary(set => set.Name,
                                                      StringComparer.Ordinal);
    var seen = new HashSet<string>(StringComparer.Ordinal);

    foreach (var option in configured)
    {
      if (string.IsNullOrWhiteSpace(option.Name))
      {
        throw new InvalidOperationException("A permission set of the configuration has no name");
      }

      if (option.Name == PermissionSet.ClusterAdminName)
      {
        throw new InvalidOperationException($"Permission set '{PermissionSet.ClusterAdminName}' cannot be redefined");
      }

      if (!seen.Add(option.Name))
      {
        throw new InvalidOperationException($"Permission set '{option.Name}' is defined twice in the configuration");
      }

      try
      {
        sets[option.Name] = PermissionSet.FromNames(option.Name,
                                                    1,
                                                    option.Permissions);
      }
      catch (InvalidPermissionSetException e)
      {
        throw new InvalidOperationException(e.Message,
                                            e);
      }
    }

    return sets;
  }

  /// <summary>
  ///   The grants of the configuration
  /// </summary>
  /// <param name="configured">Grants of the configuration</param>
  /// <returns>The grant definitions</returns>
  /// <exception cref="InvalidOperationException">A grant has no identifier, or two grants share one</exception>
  public static IReadOnlyList<GrantDefinition> Grants(IEnumerable<GrantOption> configured)
  {
    var grants = configured.Select(option => new GrantDefinition(option.Id,
                                                                 option.Principal,
                                                                 option.PermissionSet,
                                                                 option.Condition))
                           .ToList();

    if (grants.Any(grant => string.IsNullOrWhiteSpace(grant.Id)))
    {
      throw new InvalidOperationException("A grant of the configuration has no identifier");
    }

    var duplicates = grants.GroupBy(grant => grant.Id,
                                    StringComparer.Ordinal)
                           .Where(group => group.Count() > 1)
                           .Select(group => group.Key)
                           .ToList();

    if (duplicates.Count != 0)
    {
      throw new InvalidOperationException($"Grant identifiers are used twice in the configuration: {string.Join(", ", duplicates)}");
    }

    return grants;
  }
}
