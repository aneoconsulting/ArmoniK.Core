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

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record ResultDataModelMapping : IMongoDataModelMapping<Result>
{
  public ResultDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(Result)))
    {
      BsonClassMap.RegisterClassMap<Result>(cm =>
                                            {
                                              cm.MapIdProperty(nameof(Result.Id));
                                              cm.MapCreator(model => new Result(model.SessionId,
                                                                                model.Name,
                                                                                model.OwnerTaskId,
                                                                                model.Status,
                                                                                model.CreationDate,
                                                                                model.Data));
                                            });
    }
  }


  /// <inheritdoc />
  public string CollectionName
    => nameof(Result);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle     sessionHandle,
                                           IMongoCollection<Result> collection)
  {
    var sessionIndex   = Builders<Result>.IndexKeys.Hashed(model => model.SessionId);
    var ownerTaskIndex = Builders<Result>.IndexKeys.Hashed(model => model.OwnerTaskId);
    var creationIndex  = Builders<Result>.IndexKeys.Ascending(model => model.CreationDate);

    var indexModels = new CreateIndexModel<Result>[]
                      {
                        new(sessionIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(sessionIndex),
                            }),
                        new(ownerTaskIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(ownerTaskIndex),
                            }),
                        new(creationIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(creationIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
