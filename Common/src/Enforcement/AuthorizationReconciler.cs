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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Makes the stored grants and permission sets written by the configuration match the configuration
/// </summary>
/// <remarks>
///   Grants and permission sets written by the administration API are never changed. A configuration older than
///   the one stored changes nothing, so that a replica restarting with a previous configuration does not bring
///   back a revoked grant.
/// </remarks>
public class AuthorizationReconciler
{
  private const string Actor = "config";

  private readonly AuthorizationAudit               audit_;
  private readonly ILogger<AuthorizationReconciler> logger_;
  private readonly AuthorizationOptions             options_;
  private readonly IAuthorizationTable              table_;
  private readonly TimeProvider                     time_;

  /// <summary>
  ///   Build the reconciler
  /// </summary>
  public AuthorizationReconciler(IAuthorizationTable              table,
                                 AuthorizationOptions             options,
                                 AuthorizationAudit               audit,
                                 TimeProvider                     time,
                                 ILogger<AuthorizationReconciler> logger)
  {
    table_   = table;
    options_ = options;
    audit_   = audit;
    time_    = time;
    logger_  = logger;
  }

  /// <summary>
  ///   Reconcile the stored grants and permission sets with the configuration
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>Whether the configuration was applied, false when it is older than the stored one</returns>
  /// <exception cref="InvalidOperationException">
  ///   The configuration would leave a grant refused, or is older than the stored one while a change is left unfinished;
  ///   nothing is changed
  /// </exception>
  public async Task<bool> ReconcileAsync(CancellationToken cancellationToken = default)
  {
    var desiredSets = AuthorizationConfiguration.PermissionSets(options_.PermissionSets);
    var desiredGrants = AuthorizationConfiguration.Grants(options_.Grants)
                                                  .ToDictionary(grant => grant.Id,
                                                                StringComparer.Ordinal);

    await using var held = await table_.LockAsync(cancellationToken)
                                       .ConfigureAwait(false);
    // A lock lost while reconciling stops the writes that follow
    using var whileHeld = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken,
                                                                          held.Lost);
    cancellationToken = whileHeld.Token;

    var revision = await table_.GetConfigRevisionAsync(cancellationToken)
                               .ConfigureAwait(false);
    if (options_.ConfigRevision < revision)
    {
      // An older configuration can neither complete a change it does not know nor end it half written: until a later
      // one is reconciled, every request is refused, so the replica does not start
      var stored = await table_.GetGenerationAsync(cancellationToken)
                               .ConfigureAwait(false);
      if (!stored.Stable)
      {
        throw new InvalidOperationException($"Authorization configuration revision {options_.ConfigRevision} is older than the stored revision {revision}, " +
                                            "and a change of the grants was left unfinished by a writer that failed: every request is refused " +
                                            $"until a configuration of revision {revision} or later is reconciled");
      }

      logger_.LogWarning("Authorization configuration revision {Revision} is older than the stored revision {Stored}, it is not applied",
                         options_.ConfigRevision,
                         revision);
      return false;
    }

    var storedSets = await table_.ListPermissionSetsAsync(cancellationToken)
                                 .ToDictionaryAsync(set => set.Name,
                                                    StringComparer.Ordinal,
                                                    cancellationToken)
                                 .ConfigureAwait(false);
    var storedGrants = await table_.ListGrantsAsync(cancellationToken)
                                   .ToListAsync(cancellationToken)
                                   .ConfigureAwait(false);

    // What the stored sets become
    var setChanges = new List<PermissionSetData>();
    foreach (var set in desiredSets.Values)
    {
      var permissions = set.Permissions.Select(permission => permission.Name)
                           .Order(StringComparer.Ordinal)
                           .ToList();
      if (!storedSets.TryGetValue(set.Name,
                                  out var stored))
      {
        setChanges.Add(new PermissionSetData(set.Name,
                                             1,
                                             permissions,
                                             AuthorizationOrigin.Config));
      }
      // A set written by the administration API is never changed
      else if (stored.Origin == AuthorizationOrigin.Config && !stored.Permissions.Order(StringComparer.Ordinal)
                                                                     .SequenceEqual(permissions))
      {
        setChanges.Add(new PermissionSetData(set.Name,
                                             stored.Version + 1,
                                             permissions,
                                             AuthorizationOrigin.Config));
      }
    }

    var setDeletions = storedSets.Values.Where(set => set.Origin == AuthorizationOrigin.Config && !desiredSets.ContainsKey(set.Name))
                                 .ToList();

    var finalSets = new Dictionary<string, PermissionSet>(StringComparer.Ordinal);
    foreach (var set in storedSets.Values.Where(set => setDeletions.All(deleted => deleted.Name != set.Name))
                                  .Select(set => setChanges.FirstOrDefault(change => change.Name == set.Name) ?? set)
                                  .Concat(setChanges.Where(change => !storedSets.ContainsKey(change.Name))))
    {
      try
      {
        finalSets[set.Name] = PermissionSet.FromNames(set.Name,
                                                      set.Version,
                                                      set.Permissions);
      }
      catch (InvalidPermissionSetException e)
      {
        // Left out as when deciding: the grants naming it are refused below
        audit_.RecordFault($"permission set {set.Name}",
                           e.Message);
      }
    }

    // What the stored grants become
    var apiGrants = storedGrants.Where(grant => grant.Origin == AuthorizationOrigin.Api)
                                .ToList();
    var clash = apiGrants.Where(grant => desiredGrants.ContainsKey(grant.GrantId))
                         .Select(grant => grant.GrantId)
                         .ToList();
    if (clash.Count != 0)
    {
      throw new InvalidOperationException($"Grants of the configuration have the identifier of grants written by the administration API: {string.Join(", ", clash)}");
    }

    var configGrants = storedGrants.Where(grant => grant.Origin == AuthorizationOrigin.Config)
                                   .ToDictionary(grant => grant.GrantId,
                                                 StringComparer.Ordinal);
    var grantDeletions = configGrants.Values.Where(grant => !desiredGrants.TryGetValue(grant.GrantId,
                                                                                         out var desired) || !Same(grant,
                                                                                                                    desired))
                                     .ToList();
    var grantCreations = desiredGrants.Values.Where(desired => !configGrants.TryGetValue(desired.Id,
                                                                                         out var stored) || !Same(stored,
                                                                                                                   desired))
                                      .ToList();

    // Every grant must still be valid with the sets as they will be
    var errors = desiredGrants.Values.Concat(apiGrants.Select(grant => new GrantDefinition(grant.GrantId,
                                                                                            grant.Principal,
                                                                                            grant.PermissionSet,
                                                                                            grant.Condition)))
                              .Select(grant => Validate(grant,
                                                        finalSets))
                              .OfType<string>()
                              .ToList();
    if (errors.Count != 0)
    {
      throw new InvalidOperationException($"The authorization configuration is refused: {string.Join("; ", errors)}");
    }

    // Written as one change, so that no replica reads it half written. A change a failed writer left unended is ended
    // by this one, even with nothing else to write
    var current = await table_.GetGenerationAsync(cancellationToken)
                              .ConfigureAwait(false);
    var changing = !current.Stable || setChanges.Count + grantDeletions.Count + grantCreations.Count + setDeletions.Count != 0;
    cancellationToken.ThrowIfCancellationRequested();
    var generation = changing
                       ? await table_.BeginChangeAsync(cancellationToken)
                                     .ConfigureAwait(false)
                       : 0;

    foreach (var set in setChanges)
    {
      cancellationToken.ThrowIfCancellationRequested();
      await table_.SetPermissionSetAsync(set,
                                         cancellationToken)
                  .ConfigureAwait(false);
      audit_.RecordChange(Actor,
                          generation,
                          "PermissionSetChanged",
                          set.Name,
                          new
                          {
                            OldVersion = storedSets.GetValueOrDefault(set.Name)
                                                   ?.Version,
                            NewVersion = set.Version,
                            set.Permissions,
                          });
    }

    foreach (var grant in grantDeletions)
    {
      cancellationToken.ThrowIfCancellationRequested();
      await table_.DeleteGrantAsync(grant.GrantId,
                                    cancellationToken)
                  .ConfigureAwait(false);
      audit_.RecordChange(Actor,
                          generation,
                          "GrantDeleted",
                          grant.GrantId,
                          grant);
    }

    foreach (var grant in grantCreations)
    {
      var data = new GrantData(grant.Id,
                               grant.Principal,
                               grant.PermissionSet,
                               grant.Condition,
                               AuthorizationOrigin.Config,
                               time_.GetUtcNow()
                                    .UtcDateTime);
      cancellationToken.ThrowIfCancellationRequested();
      await table_.CreateGrantAsync(data,
                                    cancellationToken)
                  .ConfigureAwait(false);
      audit_.RecordChange(Actor,
                          generation,
                          "GrantCreated",
                          grant.Id,
                          data);
    }

    foreach (var set in setDeletions)
    {
      cancellationToken.ThrowIfCancellationRequested();
      await table_.DeletePermissionSetAsync(set.Name,
                                            cancellationToken)
                  .ConfigureAwait(false);
      audit_.RecordChange(Actor,
                          generation,
                          "PermissionSetDeleted",
                          set.Name,
                          new
                          {
                            OldVersion = set.Version,
                          });
    }

    // Before the change is ended: grants stable under an older revision would let an older configuration write them again
    cancellationToken.ThrowIfCancellationRequested();
    await table_.SetConfigRevisionAsync(options_.ConfigRevision,
                                        cancellationToken)
                .ConfigureAwait(false);

    // Left unended when a write fails, so that what was written is not read before a later change completes it
    if (changing)
    {
      cancellationToken.ThrowIfCancellationRequested();
      await table_.EndChangeAsync(generation,
                                  cancellationToken)
                  .ConfigureAwait(false);

      // Another writer changed the grants meanwhile, so the lock was lost and what was written here may mix with its
      // change: a change is left unended, so that none of it is served until a reconciliation completes it
      var ended = await table_.GetGenerationAsync(cancellationToken)
                              .ConfigureAwait(false);
      if (ended.Ended != generation)
      {
        await table_.BeginChangeAsync(cancellationToken)
                    .ConfigureAwait(false);
        throw new InvalidOperationException($"The grants were changed by another writer during the change of generation {generation}: " +
                                            "the lock was lost, and the grants are refused until a reconciliation completes them");
      }
    }

    return true;
  }

  private static bool Same(GrantData       stored,
                           GrantDefinition desired)
    => stored.Principal == desired.Principal && stored.PermissionSet == desired.PermissionSet && stored.Condition == desired.Condition;

  private static string? Validate(GrantDefinition                            grant,
                                  IReadOnlyDictionary<string, PermissionSet> sets)
  {
    try
    {
      GrantValidator.Validate(grant,
                              sets);
      return null;
    }
    catch (InvalidGrantException e)
    {
      return e.Message;
    }
  }
}
