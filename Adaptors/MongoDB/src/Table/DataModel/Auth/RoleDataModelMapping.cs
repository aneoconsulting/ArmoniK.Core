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
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options.Database;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

/// <summary>
///   MongoDB mapping of the Role document
/// </summary>
public class RoleDataModelMapping : IMongoDataModelMapping<RoleData>
{
  static RoleDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(RoleData)))
    {
      BsonClassMap.RegisterClassMap<RoleData>(cm =>
                                              {
                                                cm.MapIdProperty(nameof(RoleData.RoleId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(RoleData.RoleName))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(RoleData.Permissions))
                                                  .SetIgnoreIfDefault(true)
                                                  .SetDefaultValue(Array.Empty<string>());
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new RoleData(model.RoleId,
                                                                                    model.RoleName,
                                                                                    model.Permissions));
                                              });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => nameof(RoleData);

  /// <inheritdoc />
  public ICollection<CreateIndexModel<RoleData>> InitializeIndexes(Options.MongoDB options)
    => new[]
       {
         IndexHelper.CreateAscendingIndex<RoleData>(model => model.RoleName,
                                                    true),
       };

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;

  /// <inheritdoc />
  public async Task InitializeCollectionAsync(IClientSessionHandle       sessionHandle,
                                              IMongoCollection<RoleData> collection,
                                              InitDatabase               initDatabase)
  {
    if (initDatabase.Roles.Any())
    {
      // Ids are positions in the sorted configuration: remove the documents beyond the end of the list, and those
      // whose name moves to another id, which would break the unique index during the replacement below.
      // A list without entries leaves the collection untouched, since only some init processes carry the
      // authentication configuration.
      var stale = Builders<RoleData>.Filter.Or(initDatabase
                                               .Roles.Select(data => Builders<RoleData>.Filter.And(Builders<RoleData>.Filter.Eq(roleData => roleData.RoleName,
                                                                                                                                data.RoleName),
                                                                                                   Builders<RoleData>.Filter.Ne(roleData => roleData.RoleId,
                                                                                                                                data.RoleId)))
                                               .Append(Builders<RoleData>.Filter.Gte(roleData => roleData.RoleId,
                                                                                     initDatabase.Roles.Count)));

      await collection.DeleteManyAsync(sessionHandle,
                                       stale)
                      .ConfigureAwait(false);

      var upsert = initDatabase.Roles.Select(data => new ReplaceOneModel<RoleData>(Builders<RoleData>.Filter.Where(roleData => roleData.RoleId == data.RoleId),
                                                                                   data)
                                                     {
                                                       IsUpsert = true,
                                                     });

      await collection.BulkWriteAsync(sessionHandle,
                                      upsert)
                      .ConfigureAwait(false);
    }
  }
}
