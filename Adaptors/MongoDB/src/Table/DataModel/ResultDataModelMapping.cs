// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using System.Collections.Generic;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

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
                                              cm.MapProperty(nameof(Result.SessionId))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.Name))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.OwnerTaskId))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.Status))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.DependentTasks))
                                                .SetIsRequired(true)
                                                .SetDefaultValue(new List<string>());
                                              cm.MapProperty(nameof(Result.CreationDate))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.Data))
                                                .SetIsRequired(true);
                                              cm.SetIgnoreExtraElements(true);
                                              cm.MapCreator(model => new Result(model.SessionId,
                                                                                model.Name,
                                                                                model.OwnerTaskId,
                                                                                model.Status,
                                                                                model.DependentTasks,
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
