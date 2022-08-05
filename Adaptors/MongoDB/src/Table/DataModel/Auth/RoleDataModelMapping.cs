// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<RoleData> collection)
  {
    var rolenameIndex       = Builders<RoleData>.IndexKeys.Text(model => model.RoleName);
    var rolenameIndexHashed = Builders<RoleData>.IndexKeys.Hashed(model => model.RoleName);
    var indexModels = new CreateIndexModel<RoleData>[]
                      {
                        new(rolenameIndex,
                            new CreateIndexOptions
                            {
                              Name   = nameof(rolenameIndex),
                              Unique = true,
                            }),
                        new(rolenameIndexHashed,
                            new CreateIndexOptions
                            {
                              Name = nameof(rolenameIndexHashed),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
