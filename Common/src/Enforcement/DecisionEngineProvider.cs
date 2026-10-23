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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
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
  private readonly ExceptionManager                      exceptionManager_;
  private readonly ILogger<StoredDecisionEngineProvider> logger_;
  private readonly TimeSpan                              maxStaleness_;
  private readonly ExecutionSingleizer<DecisionEngine>   reload_;
  private readonly IAuthorizationTable                   table_;
  private          Loaded?                               last_;

  /// <summary>
  ///   Build the provider
  /// </summary>
  public StoredDecisionEngineProvider(IAuthorizationTable                   table,
                                      AuthorizationOptions                  options,
                                      AuthorizationAudit                    audit,
                                      ExceptionManager                      exceptionManager,
                                      ILogger<StoredDecisionEngineProvider> logger)
  {
    table_            = table;
    audit_            = audit;
    exceptionManager_ = exceptionManager;
    logger_           = logger;
    // Checked at startup as well: twice a larger delay may overflow, and nothing is cached at zero or below
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.GrantCacheTtl,
                                                       TimeSpan.Zero);
    ArgumentOutOfRangeException.ThrowIfGreaterThan(options.GrantCacheTtl,
                                                   AuthorizationOptions.MaxGrantCacheTtl);
    maxStaleness_ = 2 * options.GrantCacheTtl;
    reload_       = new ExecutionSingleizer<DecisionEngine>(options.GrantCacheTtl);
  }

  /// <inheritdoc />
  /// <remarks>
  ///   The generation is read once per period, and the grants only when it moved, by a single read shared by the callers
  ///   waiting for it; a failed read is not tried again before a period either. Until the engine read last is two periods
  ///   old, it is served while the grants are read again; past that, callers wait for the read, and are refused when it
  ///   fails. A read during a change of the grants fails, so that the change is never decided on half written.
  /// </remarks>
  public async ValueTask<DecisionEngine> GetEngineAsync(CancellationToken cancellationToken = default)
  {
    var stale = last_ is
                {
                } last && Stopwatch.GetElapsedTime(last.LoadedAt) < maxStaleness_
                  ? last.Engine
                  : null;

    // The read is shared and kept for a period, so it is never cancelled with the call of one caller
    var reload = reload_.Call(ReloadAsync,
                              CancellationToken.None);

    if (reload.IsCompletedSuccessfully)
    {
      return reload.Result;
    }

    // A caller waiting for the read stops waiting with its call, while the read goes on for the others
    return stale ?? await reload.WaitAsync(cancellationToken)
                                .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public void Dispose()
    => reload_.Dispose();

  private async Task<DecisionEngine> ReloadAsync(CancellationToken cancellationToken)
  {
    try
    {
      // Kept only when no change was under way, so that a change is never read half written
      var before = await table_.GetGenerationAsync(cancellationToken)
                               .ConfigureAwait(false);

      // Every change moves the generation, so the engine read last is still the one in force
      if (before.Stable && last_?.Generation == before.Ended)
      {
        last_ = last_ with
                {
                  LoadedAt = Stopwatch.GetTimestamp(),
                };
        return last_.Engine;
      }

      var loaded = await LoadAsync(cancellationToken)
                     .ConfigureAwait(false);
      var after = await table_.GetGenerationAsync(cancellationToken)
                              .ConfigureAwait(false);
      if (!before.Stable || after != before)
      {
        // A change under way is not a fault of the control plane: not counted toward the errors that stop it
        throw new GrantsUnavailableException($"Grants were being changed while they were read, at generation {after.Begun}");
      }

      if (last_?.Generation != before.Ended)
      {
        audit_.RecordGeneration(before.Ended);
      }

      last_ = new Loaded(loaded,
                         before.Ended,
                         Stopwatch.GetTimestamp());
      return loaded;
    }
    catch (GrantsUnavailableException e)
    {
      logger_.LogWarning(e,
                         "Grants could not be read");
      throw;
    }
    catch (Exception e) when (e is not OperationCanceledException)
    {
      // Counted once for the read, not once for each request refused while it is kept
      exceptionManager_.RecordError(logger_,
                                    e,
                                    "Grants could not be read");
      throw new GrantsUnavailableException("Grants could not be read",
                                           e);
    }
  }

  private async Task<DecisionEngine> LoadAsync(CancellationToken cancellationToken)
  {
    var sets = AuthorizationConfiguration.StoredPermissionSets(await table_.ListPermissionSetsAsync(cancellationToken)
                                                                           .ToListAsync(cancellationToken)
                                                                           .ConfigureAwait(false),
                                                               audit_);

    var grants = await table_.ListGrantsAsync(cancellationToken)
                             .Select(grant => grant.ToDefinition())
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

  private sealed record Loaded(DecisionEngine Engine,
                               long           Generation,
                               long           LoadedAt);
}

/// <summary>
///   Grants and permission sets written by the deployment configuration
/// </summary>
public static class AuthorizationConfiguration
{
  /// <summary>
  ///   The stored permission sets, without the permissions this version does not know: recorded as faults, they are left
  ///   out of their set, which a later version may have written. A malformed set is left out, recorded as a fault, so that
  ///   the grants naming it are refused as for a missing set
  /// </summary>
  /// <param name="stored">Permission sets as stored</param>
  /// <param name="audit">Audit recording the permissions left out</param>
  /// <returns>The permission sets, by name</returns>
  internal static Dictionary<string, PermissionSet> StoredPermissionSets(IEnumerable<PermissionSetData> stored,
                                                                         AuthorizationAudit             audit)
  {
    var sets = new Dictionary<string, PermissionSet>(StringComparer.Ordinal);

    foreach (var set in stored)
    {
      // A set as stored may hold what no validation expects: it is refused alone, as a grant is, rather than leaving
      // every request undecided
      if (string.IsNullOrWhiteSpace(set.Name) || set.Permissions is null || set.Permissions.Any(permission => permission is null))
      {
        audit.RecordFault($"permission set {set.Name}",
                          "the stored set has no name, no permission list or a null permission: it is left out");
        continue;
      }

      sets[set.Name] = PermissionSet.FromNames(set.Name,
                                               set.Version,
                                               set.Permissions,
                                               out var unknown);
      if (unknown.Count != 0)
      {
        audit.RecordFault($"permission set {set.Name}",
                          $"permissions that are not in the catalogue are left out: {string.Join(", ", unknown)}");
      }
    }

    return sets;
  }

  /// <summary>
  ///   The permission sets of the configuration: none is shipped with ArmoniK
  /// </summary>
  /// <param name="configured">Permission sets of the configuration</param>
  /// <returns>The permission sets, by name</returns>
  /// <exception cref="InvalidOperationException">A permission set of the configuration is refused</exception>
  public static IReadOnlyDictionary<string, PermissionSet> PermissionSets(IEnumerable<PermissionSetOption> configured)
  {
    var sets = new Dictionary<string, PermissionSet>(StringComparer.Ordinal);

    foreach (var option in configured)
    {
      if (string.IsNullOrWhiteSpace(option.Name))
      {
        throw new InvalidOperationException("A permission set of the configuration has no name");
      }

      if (sets.ContainsKey(option.Name))
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
