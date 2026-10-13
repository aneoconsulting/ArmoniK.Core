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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Authorization;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <inheritdoc />
public class AuthorizationTable : IAuthorizationTable
{
  private const string RevisionId   = "revision";
  private const string LockId       = "lock";
  private const string GenerationId = "generation";

  private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(1);

  private readonly ActivitySource                                                                 activitySource_;
  private readonly MongoCollectionProvider<GrantData, GrantDataModelMapping>                       grants_;
  private readonly MongoCollectionProvider<PermissionSetData, PermissionSetDataModelMapping>       sets_;
  private readonly MongoCollectionProvider<AuthorizationStateData, AuthorizationStateDataModelMapping> state_;
  private readonly MongoCollectionProvider<PermissionSetVersionData, PermissionSetVersionDataModelMapping> versions_;
  private          bool                                                                           isInitialized_;

  /// <summary>
  ///   Creates a new AuthorizationTable
  /// </summary>
  public AuthorizationTable(MongoCollectionProvider<GrantData, GrantDataModelMapping>                             grants,
                            MongoCollectionProvider<PermissionSetData, PermissionSetDataModelMapping>             sets,
                            MongoCollectionProvider<PermissionSetVersionData, PermissionSetVersionDataModelMapping> versions,
                            MongoCollectionProvider<AuthorizationStateData, AuthorizationStateDataModelMapping>     state,
                            ActivitySource                                                                         activitySource)
  {
    grants_         = grants;
    sets_           = sets;
    versions_       = versions;
    state_          = state;
    activitySource_ = activitySource;
  }

  /// <inheritdoc />
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    var result = await HealthCheckResultCombiner.Combine(tag,
                                                         $"{nameof(AuthorizationTable)} is not initialized",
                                                         grants_,
                                                         sets_,
                                                         versions_,
                                                         state_)
                                                .ConfigureAwait(false);

    return isInitialized_ && result.Status == HealthStatus.Healthy
             ? HealthCheckResult.Healthy()
             : HealthCheckResult.Unhealthy(result.Description);
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (isInitialized_)
    {
      return;
    }

    await grants_.Init(cancellationToken)
                 .ConfigureAwait(false);
    await sets_.Init(cancellationToken)
               .ConfigureAwait(false);
    await versions_.Init(cancellationToken)
                   .ConfigureAwait(false);
    await state_.Init(cancellationToken)
                .ConfigureAwait(false);
    isInitialized_ = true;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<GrantData> ListGrantsAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    return grants_.Get()
                  .FindAsync(FilterDefinition<GrantData>.Empty,
                             cancellationToken: cancellationToken)
                  .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task CreateGrantAsync(GrantData         grant,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    try
    {
      await grants_.Get()
                   .InsertOneAsync(grant,
                                   cancellationToken: cancellationToken)
                   .ConfigureAwait(false);
    }
    catch (MongoWriteException e) when (e.WriteError.Category == ServerErrorCategory.DuplicateKey)
    {
      throw new GrantAlreadyExistsException($"Grant '{grant.GrantId}' already exists",
                                            e);
    }
  }

  /// <inheritdoc />
  public async Task<bool> DeleteGrantAsync(string            grantId,
                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var result = await grants_.Get()
                              .DeleteOneAsync(grant => grant.GrantId == grantId,
                                              cancellationToken)
                              .ConfigureAwait(false);
    return result.DeletedCount > 0;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<PermissionSetData> ListPermissionSetsAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    return sets_.Get()
                .FindAsync(FilterDefinition<PermissionSetData>.Empty,
                           cancellationToken: cancellationToken)
                .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<PermissionSetData?> GetPermissionSetVersionAsync(string            name,
                                                                     int               version,
                                                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var id = VersionId(name,
                       version);
    var found = await versions_.Get()
                               .Find(data => data.Id == id)
                               .FirstOrDefaultAsync(cancellationToken)
                               .ConfigureAwait(false);

    return found is null
             ? null
             : new PermissionSetData(found.Name,
                                     found.Version,
                                     found.Permissions,
                                     found.Origin);
  }

  /// <inheritdoc />
  public async Task SetPermissionSetAsync(PermissionSetData permissionSet,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var id = VersionId(permissionSet.Name,
                       permissionSet.Version);
    await versions_.Get()
                   .ReplaceOneAsync(data => data.Id == id,
                                    new PermissionSetVersionData(id,
                                                                 permissionSet.Name,
                                                                 permissionSet.Version,
                                                                 permissionSet.Permissions,
                                                                 permissionSet.Origin),
                                    new ReplaceOptions
                                    {
                                      IsUpsert = true,
                                    },
                                    cancellationToken)
                   .ConfigureAwait(false);
    await sets_.Get()
               .ReplaceOneAsync(data => data.Name == permissionSet.Name,
                                permissionSet,
                                new ReplaceOptions
                                {
                                  IsUpsert = true,
                                },
                                cancellationToken)
               .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<bool> DeletePermissionSetAsync(string            name,
                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var result = await sets_.Get()
                            .DeleteOneAsync(data => data.Name == name,
                                            cancellationToken)
                            .ConfigureAwait(false);
    return result.DeletedCount > 0;
  }

  /// <inheritdoc />
  public async Task<int> GetConfigRevisionAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var state = await state_.Get()
                            .Find(data => data.Id == RevisionId)
                            .FirstOrDefaultAsync(cancellationToken)
                            .ConfigureAwait(false);
    return state?.Revision ?? 0;
  }

  /// <inheritdoc />
  public async Task SetConfigRevisionAsync(int               revision,
                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await state_.Get()
                .ReplaceOneAsync(data => data.Id == RevisionId,
                                 new AuthorizationStateData(RevisionId,
                                                            revision,
                                                            null,
                                                            null),
                                 new ReplaceOptions
                                 {
                                   IsUpsert = true,
                                 },
                                 cancellationToken)
                .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<IAsyncDisposable> LockAsync(CancellationToken cancellationToken = default)
  {
    using var activity   = activitySource_.StartActivity();
    var       collection = state_.Get();
    var       owner      = Guid.NewGuid()
                               .ToString();

    while (true)
    {
      try
      {
        await collection.InsertOneAsync(new AuthorizationStateData(LockId,
                                                                   0,
                                                                   owner,
                                                                   DateTime.UtcNow + LockDuration),
                                        cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
        return new Release(collection,
                           owner);
      }
      catch (MongoWriteException e) when (e.WriteError.Category == ServerErrorCategory.DuplicateKey)
      {
      }

      // Take over a lock whose holder did not release it in time
      var now = DateTime.UtcNow;
      var taken = await collection.FindOneAndUpdateAsync<AuthorizationStateData>(data => data.Id == LockId && data.ExpiresAt < now,
                                                                                 Builders<AuthorizationStateData>.Update.Set(data => data.Owner,
                                                                                                                             owner)
                                                                                                                 .Set(data => data.ExpiresAt,
                                                                                                                      now + LockDuration),
                                                                                 cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);
      if (taken is not null)
      {
        return new Release(collection,
                           owner);
      }

      await Task.Delay(TimeSpan.FromMilliseconds(100),
                       cancellationToken)
                .ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<AuthorizationGeneration> GetGenerationAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var state = await state_.Get()
                            .Find(data => data.Id == GenerationId)
                            .FirstOrDefaultAsync(cancellationToken)
                            .ConfigureAwait(false);
    return state is null
             ? default
             : new AuthorizationGeneration(state.Begun,
                                           state.Ended);
  }

  /// <inheritdoc />
  public async Task<long> BeginChangeAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var state = await state_.Get()
                            .FindOneAndUpdateAsync<AuthorizationStateData>(data => data.Id == GenerationId,
                                                                           Builders<AuthorizationStateData>.Update.Inc(data => data.Begun,
                                                                                                                       1L),
                                                                           new FindOneAndUpdateOptions<AuthorizationStateData>
                                                                           {
                                                                             IsUpsert       = true,
                                                                             ReturnDocument = ReturnDocument.After,
                                                                           },
                                                                           cancellationToken)
                            .ConfigureAwait(false);
    return state.Begun;
  }

  /// <inheritdoc />
  public async Task EndChangeAsync(long              generation,
                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await state_.Get()
                .UpdateOneAsync(data => data.Id == GenerationId && data.Begun == generation,
                                Builders<AuthorizationStateData>.Update.Set(data => data.Ended,
                                                                            generation),
                                cancellationToken: cancellationToken)
                .ConfigureAwait(false);
  }

  private static string VersionId(string name,
                                  int    version)
    => $"{name}@{version}";

  private sealed class Release : IAsyncDisposable
  {
    private readonly IMongoCollection<AuthorizationStateData> collection_;
    private readonly string                                   owner_;

    public Release(IMongoCollection<AuthorizationStateData> collection,
                   string                                   owner)
    {
      collection_ = collection;
      owner_      = owner;
    }

    public async ValueTask DisposeAsync()
      => await collection_.DeleteOneAsync(data => data.Id == LockId && data.Owner == owner_)
                          .ConfigureAwait(false);
  }
}
