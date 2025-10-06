// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;

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
                                                  .SetIsRequired(true)
                                                  .SetSerializer(IdSerializer.Instance);
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
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<RoleData> collection,
                                           Options.MongoDB            options)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateTextIndex<RoleData>(model => model.RoleName,
                                                              true),
                        IndexHelper.CreateHashedIndex<RoleData>(model => model.RoleName),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}
