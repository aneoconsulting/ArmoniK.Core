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

using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

public class AuthDataModelMapping : IMongoDataModelMapping<AuthData>
{
  static AuthDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(AuthData)))
    {
      BsonClassMap.RegisterClassMap<AuthData>(cm =>
                                              {
                                                cm.MapIdProperty(nameof(AuthData.AuthId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(AuthData.UserId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(AuthData.CN))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(AuthData.Fingerprint))
                                                  .SetDefaultValue(BsonNull.Value);
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new AuthData(model.AuthId,
                                                                                    model.UserId,
                                                                                    model.CN,
                                                                                    model.Fingerprint));
                                              });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => nameof(AuthData);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<AuthData> collection)
  {
    var fingerprintIndex       = Builders<AuthData>.IndexKeys.Descending(model => model.Fingerprint);
    var fingerprintHashedIndex = Builders<AuthData>.IndexKeys.Hashed(model => model.Fingerprint);
    var cnIndex                = Builders<AuthData>.IndexKeys.Ascending(model => model.CN);
    var compoundIndex = Builders<AuthData>.IndexKeys.Combine(cnIndex,
                                                             fingerprintIndex);
    var userIndex = Builders<AuthData>.IndexKeys.Hashed(model => model.UserId);

    var indexModels = new CreateIndexModel<AuthData>[]
                      {
                        new(fingerprintIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(fingerprintIndex),
                            }),
                        new(fingerprintHashedIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(fingerprintHashedIndex),
                            }),
                        new(cnIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(cnIndex),
                            }),
                        new(compoundIndex,
                            new CreateIndexOptions
                            {
                              Name   = nameof(compoundIndex),
                              Unique = true,
                            }),
                        new(userIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(userIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
