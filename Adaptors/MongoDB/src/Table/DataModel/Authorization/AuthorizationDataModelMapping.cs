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
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Authorization;

/// <summary>
///   A version of a permission set, current or past
/// </summary>
/// <param name="Id">Name and version of the set</param>
/// <param name="Name">Name of the set</param>
/// <param name="Version">Version of the set</param>
/// <param name="Permissions">Names of the permissions</param>
/// <param name="Origin">Who wrote the set</param>
public record PermissionSetVersionData(string              Id,
                                       string              Name,
                                       int                 Version,
                                       IList<string>       Permissions,
                                       AuthorizationOrigin Origin);

/// <summary>
///   State shared by the replicas: the revision of the configuration, the lock serializing changes, and the
///   generation of the grants and permission sets
/// </summary>
/// <param name="Id">Identifier of the document</param>
/// <param name="Revision">Revision of the configuration</param>
/// <param name="Owner">Holder of the lock</param>
/// <param name="ExpiresAt">When the lock may be taken over</param>
/// <param name="Begun">Generation of the last change that began</param>
/// <param name="Ended">Generation of the last change that ended</param>
public record AuthorizationStateData(string    Id,
                                     int       Revision,
                                     string?   Owner,
                                     DateTime? ExpiresAt,
                                     long      Begun = 0,
                                     long      Ended = 0);

/// <summary>
///   Mapping of <see cref="GrantData" />
/// </summary>
public class GrantDataModelMapping : IMongoDataModelMapping<GrantData>
{
  static GrantDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(GrantData)))
    {
      BsonClassMap.RegisterClassMap<GrantData>(cm =>
                                               {
                                                 cm.MapIdProperty(nameof(GrantData.GrantId))
                                                   .SetIsRequired(true);
                                                 cm.MapProperty(nameof(GrantData.Principal))
                                                   .SetIsRequired(true);
                                                 cm.MapProperty(nameof(GrantData.PermissionSet))
                                                   .SetIsRequired(true);
                                                 cm.MapProperty(nameof(GrantData.Condition));
                                                 cm.MapProperty(nameof(GrantData.Origin))
                                                   .SetIsRequired(true);
                                                 cm.MapProperty(nameof(GrantData.CreationDate))
                                                   .SetIsRequired(true);
                                                 cm.SetIgnoreExtraElements(true);
                                                 cm.MapCreator(model => new GrantData(model.GrantId,
                                                                                      model.Principal,
                                                                                      model.PermissionSet,
                                                                                      model.Condition,
                                                                                      model.Origin,
                                                                                      model.CreationDate));
                                               });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => "Grant";

  /// <inheritdoc />
  public ICollection<CreateIndexModel<GrantData>> InitializeIndexes(Options.MongoDB options)
    => Array.Empty<CreateIndexModel<GrantData>>();

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}

/// <summary>
///   Mapping of the current version of <see cref="PermissionSetData" />
/// </summary>
public class PermissionSetDataModelMapping : IMongoDataModelMapping<PermissionSetData>
{
  static PermissionSetDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(PermissionSetData)))
    {
      BsonClassMap.RegisterClassMap<PermissionSetData>(cm =>
                                                       {
                                                         cm.MapIdProperty(nameof(PermissionSetData.Name))
                                                           .SetIsRequired(true);
                                                         cm.MapProperty(nameof(PermissionSetData.Version))
                                                           .SetIsRequired(true);
                                                         cm.MapProperty(nameof(PermissionSetData.Permissions))
                                                           .SetIsRequired(true);
                                                         cm.MapProperty(nameof(PermissionSetData.Origin))
                                                           .SetIsRequired(true);
                                                         cm.SetIgnoreExtraElements(true);
                                                         cm.MapCreator(model => new PermissionSetData(model.Name,
                                                                                                      model.Version,
                                                                                                      model.Permissions,
                                                                                                      model.Origin));
                                                       });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => "PermissionSet";

  /// <inheritdoc />
  public ICollection<CreateIndexModel<PermissionSetData>> InitializeIndexes(Options.MongoDB options)
    => Array.Empty<CreateIndexModel<PermissionSetData>>();

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}

/// <summary>
///   Mapping of <see cref="PermissionSetVersionData" />
/// </summary>
public class PermissionSetVersionDataModelMapping : IMongoDataModelMapping<PermissionSetVersionData>
{
  static PermissionSetVersionDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(PermissionSetVersionData)))
    {
      BsonClassMap.RegisterClassMap<PermissionSetVersionData>(cm =>
                                                              {
                                                                cm.MapIdProperty(nameof(PermissionSetVersionData.Id))
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(PermissionSetVersionData.Name))
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(PermissionSetVersionData.Version))
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(PermissionSetVersionData.Permissions))
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(PermissionSetVersionData.Origin))
                                                                  .SetIsRequired(true);
                                                                cm.SetIgnoreExtraElements(true);
                                                                cm.MapCreator(model => new PermissionSetVersionData(model.Id,
                                                                                                                    model.Name,
                                                                                                                    model.Version,
                                                                                                                    model.Permissions,
                                                                                                                    model.Origin));
                                                              });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => "PermissionSetVersion";

  /// <inheritdoc />
  public ICollection<CreateIndexModel<PermissionSetVersionData>> InitializeIndexes(Options.MongoDB options)
    => Array.Empty<CreateIndexModel<PermissionSetVersionData>>();

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}

/// <summary>
///   Mapping of <see cref="AuthorizationStateData" />
/// </summary>
public class AuthorizationStateDataModelMapping : IMongoDataModelMapping<AuthorizationStateData>
{
  static AuthorizationStateDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(AuthorizationStateData)))
    {
      BsonClassMap.RegisterClassMap<AuthorizationStateData>(cm =>
                                                            {
                                                              cm.MapIdProperty(nameof(AuthorizationStateData.Id))
                                                                .SetIsRequired(true);
                                                              // Each document holds only some of them: the generation is created by an upsert of its counters
                                                              cm.MapProperty(nameof(AuthorizationStateData.Revision))
                                                                .SetDefaultValue(0);
                                                              cm.MapProperty(nameof(AuthorizationStateData.Owner))
                                                                .SetDefaultValue((object?)null);
                                                              cm.MapProperty(nameof(AuthorizationStateData.ExpiresAt))
                                                                .SetDefaultValue((object?)null);
                                                              cm.MapProperty(nameof(AuthorizationStateData.Begun))
                                                                .SetDefaultValue(0L);
                                                              cm.MapProperty(nameof(AuthorizationStateData.Ended))
                                                                .SetDefaultValue(0L);
                                                              cm.SetIgnoreExtraElements(true);
                                                              cm.MapCreator(model => new AuthorizationStateData(model.Id,
                                                                                                                model.Revision,
                                                                                                                model.Owner,
                                                                                                                model.ExpiresAt,
                                                                                                                model.Begun,
                                                                                                                model.Ended));
                                                            });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => "AuthorizationState";

  /// <inheritdoc />
  public ICollection<CreateIndexModel<AuthorizationStateData>> InitializeIndexes(Options.MongoDB options)
    => Array.Empty<CreateIndexModel<AuthorizationStateData>>();

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}
