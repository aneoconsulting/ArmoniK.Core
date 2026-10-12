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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Provides the decision engine over the grants in force
/// </summary>
public interface IDecisionEngineProvider
{
  /// <summary>
  ///   The decision engine over the grants in force
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The engine</returns>
  ValueTask<DecisionEngine> GetEngineAsync(CancellationToken cancellationToken = default);
}

/// <summary>
///   Decision engine over the grants and permission sets stored, read again once they are older than
///   <see cref="AuthorizationOptions.GrantCacheTtl" />
/// </summary>
public sealed class StoredDecisionEngineProvider : IDecisionEngineProvider, IDisposable
{
  private readonly AuthorizationAudit                    audit_;
  private readonly ILogger<StoredDecisionEngineProvider> logger_;
  private readonly ExecutionSingleizer<DecisionEngine>   reload_;
  private readonly IAuthorizationTable                   table_;
  private readonly TimeSpan                              ttl_;
  private          Loaded?                               last_;

  /// <summary>
  ///   Build the provider
  /// </summary>
  public StoredDecisionEngineProvider(IAuthorizationTable                   table,
                                      AuthorizationOptions                  options,
                                      AuthorizationAudit                    audit,
                                      ILogger<StoredDecisionEngineProvider> logger)
  {
    table_  = table;
    audit_  = audit;
    logger_ = logger;
    ttl_    = options.GrantCacheTtl;
    reload_ = new ExecutionSingleizer<DecisionEngine>(ttl_);
  }

  /// <inheritdoc />
  public void Dispose()
    => reload_.Dispose();

  /// <inheritdoc />
  /// <remarks>
  ///   The grants are read once per period, by a single read shared by the callers waiting for it; a failed read is not
  ///   tried again before a period either. Until the engine read last is two periods old, it is served while the grants
  ///   are read again; past that, callers wait for the read, and are refused when it fails.
  /// </remarks>
  public async ValueTask<DecisionEngine> GetEngineAsync(CancellationToken cancellationToken = default)
  {
    var stale = last_ is { } last && Stopwatch.GetElapsedTime(last.LoadedAt) < 2 * ttl_
                  ? last.Engine
                  : null;

    // Served the stale engine, a caller does not wait for the read, which must not be cancelled with its call
    var reload = reload_.Call(ReloadAsync,
                              stale is null
                                ? cancellationToken
                                : CancellationToken.None);

    if (reload.IsCompletedSuccessfully)
    {
      return reload.Result;
    }

    return stale ?? await reload.ConfigureAwait(false);
  }

  private async Task<DecisionEngine> ReloadAsync(CancellationToken cancellationToken)
  {
    try
    {
      var loaded = await LoadAsync(cancellationToken)
                     .ConfigureAwait(false);
      last_ = new Loaded(loaded,
                         Stopwatch.GetTimestamp());
      return loaded;
    }
    catch (Exception e) when (e is not OperationCanceledException)
    {
      logger_.LogWarning(e,
                         "Grants could not be read");
      throw;
    }
  }

  private sealed record Loaded(DecisionEngine Engine,
                               long           LoadedAt);

  private async Task<DecisionEngine> LoadAsync(CancellationToken cancellationToken)
  {
    var sets = new Dictionary<string, PermissionSet>(StringComparer.Ordinal);

    await foreach (var stored in table_.ListPermissionSetsAsync(cancellationToken)
                                       .ConfigureAwait(false))
    {
      try
      {
        sets[stored.Name] = PermissionSet.FromNames(stored.Name,
                                                    stored.Version,
                                                    stored.Permissions);
      }
      catch (InvalidPermissionSetException e)
      {
        // Left out: the grants naming it are refused below
        audit_.RecordFault($"permission set {stored.Name}",
                           e.Message);
      }
    }

    var grants = await table_.ListGrantsAsync(cancellationToken)
                             .Select(grant => new GrantDefinition(grant.GrantId,
                                                                  grant.Principal,
                                                                  grant.PermissionSet,
                                                                  grant.Condition))
                             .ToListAsync(cancellationToken)
                             .ConfigureAwait(false);

    var engine = DecisionEngine.Build(grants,
                                      sets,
                                      out var rejected);

    // A refused grant is left out, never read without its condition
    foreach (var (grant, error) in rejected)
    {
      audit_.RecordFault($"grant {grant.Id}",
                         error.Message);
    }

    return engine;
  }
}

/// <summary>
///   Grants and permission sets written by the deployment configuration
/// </summary>
public static class AuthorizationConfiguration
{
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
