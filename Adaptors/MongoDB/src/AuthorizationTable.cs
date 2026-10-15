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

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <inheritdoc />
public class AuthorizationTable : IAuthorizationTable
{
  private const string RevisionId   = "revision";
  private const string LockId       = "lock";
  private const string GenerationId = "generation";


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

  /// <summary>
  ///   Duration of the lease of the lock, renewed every third of it while the lock is held
  /// </summary>
  public TimeSpan LockDuration { get; init; } = TimeSpan.FromMinutes(1);

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
  public async Task<int> GetLatestPermissionSetVersionAsync(string            name,
                                                            CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var latest = await versions_.Get()
                                .Find(data => data.Name == name)
                                .SortByDescending(data => data.Version)
                                .Limit(1)
                                .FirstOrDefaultAsync(cancellationToken)
                                .ConfigureAwait(false);
    return latest?.Version ?? 0;
  }

  /// <inheritdoc />
  public async Task SetPermissionSetAsync(PermissionSetData permissionSet,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var id = VersionId(permissionSet.Name,
                       permissionSet.Version);
    try
    {
      await versions_.Get()
                     .InsertOneAsync(new PermissionSetVersionData(id,
                                                                  permissionSet.Name,
                                                                  permissionSet.Version,
                                                                  permissionSet.Permissions,
                                                                  permissionSet.Origin),
                                     cancellationToken: cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (MongoWriteException e) when (e.WriteError.Category == ServerErrorCategory.DuplicateKey)
    {
      // A stored version is never rewritten: written again only with the same content
      var stored = await GetPermissionSetVersionAsync(permissionSet.Name,
                                                      permissionSet.Version,
                                                      cancellationToken)
                     .ConfigureAwait(false);
      if (stored is null || !stored.SameAs(permissionSet))
      {
        throw new PermissionSetVersionAlreadyExistsException($"Permission set {permissionSet.Name}@{permissionSet.Version} is already stored with other permissions",
                                                             e);
      }
    }

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
                .UpdateOneAsync(data => data.Id == RevisionId,
                                Builders<AuthorizationStateData>.Update.Max(data => data.Revision,
                                                                            revision),
                                new UpdateOptions
                                {
                                  IsUpsert = true,
                                },
                                cancellationToken)
                .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<IAuthorizationLock> LockAsync(CancellationToken cancellationToken = default)
  {
    using var activity   = activitySource_.StartActivity();
    var       collection = state_.Get();
    var       owner      = Guid.NewGuid()
                               .ToString();

    // Taken when expired by the clock of the server, so that the clocks of the replicas do not matter
    var expired = new BsonDocument
                  {
                    {
                      "_id", LockId
                    },
                    {
                      "$expr", new BsonDocument("$lt",
                                                new BsonArray
                                                {
                                                  "$" + nameof(AuthorizationStateData.ExpiresAt),
                                                  "$$NOW",
                                                })
                    },
                  };

    while (true)
    {
      // Created expired when absent, then taken: an upsert cannot be conditioned on the clock of the server
      try
      {
        await collection.UpdateOneAsync(data => data.Id == LockId,
                                        Builders<AuthorizationStateData>.Update.SetOnInsert(data => data.ExpiresAt,
                                                                                            DateTime.UnixEpoch),
                                        new UpdateOptions
                                        {
                                          IsUpsert = true,
                                        },
                                        cancellationToken)
                        .ConfigureAwait(false);
      }
      catch (MongoWriteException e) when (e.WriteError.Category == ServerErrorCategory.DuplicateKey)
      {
        // Created at the same time by another replica
      }

      var taken = await collection.UpdateOneAsync(expired,
                                                  Lease.Hold(owner,
                                                             LockDuration),
                                                  cancellationToken: cancellationToken)
                                  .ConfigureAwait(false);
      if (taken.MatchedCount == 1)
      {
        return new Lease(collection,
                         owner,
                         LockDuration);
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

  /// <summary>
  ///   Lease of the lock, renewed while it is held, and reported lost when another replica took it over or when it could
  ///   not be renewed before it expired
  /// </summary>
  private sealed class Lease : IAuthorizationLock
  {
    private readonly IMongoCollection<AuthorizationStateData> collection_;
    private readonly TimeSpan                                 duration_;
    private readonly CancellationTokenSource                  lost_ = new();
    private readonly string                                   owner_;
    private readonly Task                                     renewal_;
    private readonly CancellationTokenSource                  stop_ = new();

    public Lease(IMongoCollection<AuthorizationStateData> collection,
                 string                                   owner,
                 TimeSpan                                 duration)
    {
      collection_ = collection;
      owner_      = owner;
      duration_   = duration;
      renewal_    = Task.Run(RenewAsync);
    }

    public CancellationToken Lost
      => lost_.Token;

    public async ValueTask DisposeAsync()
    {
      await stop_.CancelAsync()
                 .ConfigureAwait(false);
      await renewal_.ConfigureAwait(false);
      await collection_.DeleteOneAsync(data => data.Id == LockId && data.Owner == owner_)
                       .ConfigureAwait(false);
      stop_.Dispose();
      lost_.Dispose();
    }

    /// <summary>The update giving the lock to its owner until the clock of the server reaches the end of the lease</summary>
    public static global::MongoDB.Driver.UpdateDefinition<AuthorizationStateData> Hold(string   owner,
                                                                TimeSpan duration)
      => Builders<AuthorizationStateData>.Update.Pipeline(new BsonDocumentStagePipelineDefinition<AuthorizationStateData, AuthorizationStateData>([
                                                                                                                                                   new BsonDocument("$set",
                                                                                                                                                                    new BsonDocument
                                                                                                                                                                    {
                                                                                                                                                                      {
                                                                                                                                                                        nameof(AuthorizationStateData.Owner), owner
                                                                                                                                                                      },
                                                                                                                                                                      {
                                                                                                                                                                        nameof(AuthorizationStateData.ExpiresAt),
                                                                                                                                                                        new BsonDocument("$add",
                                                                                                                                                                                         new BsonArray
                                                                                                                                                                                         {
                                                                                                                                                                                           "$$NOW",
                                                                                                                                                                                           (long)duration.TotalMilliseconds,
                                                                                                                                                                                         })
                                                                                                                                                                      },
                                                                                                                                                                    }),
                                                                                                                                                 ]));

    private async Task RenewAsync()
    {
      var renewed = Stopwatch.StartNew();
      while (!stop_.IsCancellationRequested)
      {
        try
        {
          await Task.Delay(duration_ / 3,
                           stop_.Token)
                    .ConfigureAwait(false);
          var result = await collection_.UpdateOneAsync(data => data.Id == LockId && data.Owner == owner_,
                                                        Hold(owner_,
                                                             duration_),
                                                        cancellationToken: stop_.Token)
                                        .ConfigureAwait(false);
          if (result.MatchedCount == 0)
          {
            // Taken over by another replica
            await lost_.CancelAsync()
                       .ConfigureAwait(false);
            return;
          }

          renewed.Restart();
        }
        catch (OperationCanceledException) when (stop_.IsCancellationRequested)
        {
          return;
        }
        catch (Exception) when (renewed.Elapsed < duration_)
        {
          // Tried again while the lease still holds
        }
        catch (Exception)
        {
          await lost_.CancelAsync()
                     .ConfigureAwait(false);
          return;
        }
      }
    }
  }
}
