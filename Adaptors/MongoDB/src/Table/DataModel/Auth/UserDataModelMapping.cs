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

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

public class UserDataModelMapping : IMongoDataModelMapping<UserData>
{
  static UserDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(UserData)))
    {
      BsonClassMap.RegisterClassMap<UserData>(cm =>
                                              {
                                                cm.MapIdProperty(nameof(UserData.UserId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(UserData.Username))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(UserData.Roles))
                                                  .SetIgnoreIfDefault(true)
                                                  .SetDefaultValue(Array.Empty<string>());
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new UserData(model.UserId,
                                                                                    model.Username,
                                                                                    model.Roles));
                                              });
    }
  }

  public string CollectionName
    => nameof(UserData);

  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<UserData> collection)
  {
    var usernameIndex = Builders<UserData>.IndexKeys.Text(model => model.Username);
    var usernameIndexHashed = Builders<UserData>.IndexKeys.Hashed(model => model.Username);

    var indexModels = new CreateIndexModel<UserData>[]
                      {
                        new(usernameIndex,
                            new CreateIndexOptions
                            {
                              Name   = nameof(usernameIndex),
                              Unique = true,
                            }),
                        new(usernameIndexHashed,
                            new CreateIndexOptions
                            {
                              Name   = nameof(usernameIndexHashed)
                            }),
                      };
    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
