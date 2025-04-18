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
                                              cm.MapIdProperty(nameof(Result.ResultId));
                                              cm.MapProperty(nameof(Result.SessionId))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.Name))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.CreatedBy))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.CompletedBy))
                                                .SetIsRequired(false)
                                                .SetDefaultValue("");
                                              cm.MapProperty(nameof(Result.OwnerTaskId))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.Status))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.DependentTasks))
                                                .SetIsRequired(true)
                                                .SetDefaultValue(new List<string>());
                                              cm.MapProperty(nameof(Result.CreationDate))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.CompletionDate))
                                                .SetIsRequired(false)
                                                .SetDefaultValue((DateTime?)null);
                                              cm.MapProperty(nameof(Result.Size))
                                                .SetIgnoreIfDefault(true)
                                                .SetDefaultValue(0);
                                              cm.MapProperty(nameof(Result.OpaqueId))
                                                .SetIsRequired(true);
                                              cm.MapProperty(nameof(Result.ManualDeletion))
                                                .SetIgnoreIfDefault(true)
                                                .SetDefaultValue(false);
                                              cm.SetIgnoreExtraElements(true);
                                              cm.MapCreator(model => new Result(model.SessionId,
                                                                                model.ResultId,
                                                                                model.Name,
                                                                                model.CreatedBy,
                                                                                model.CompletedBy,
                                                                                model.OwnerTaskId,
                                                                                model.Status,
                                                                                model.DependentTasks,
                                                                                model.CreationDate,
                                                                                model.CompletionDate,
                                                                                model.Size,
                                                                                model.OpaqueId,
                                                                                model.ManualDeletion));
                                            });
    }
  }


  /// <inheritdoc />
  public string CollectionName
    => nameof(Result);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle     sessionHandle,
                                           IMongoCollection<Result> collection,
                                           Options.MongoDB          options)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateHashedIndex<Result>(model => model.SessionId),
                        IndexHelper.CreateHashedIndex<Result>(model => model.CreatedBy),
                        IndexHelper.CreateHashedIndex<Result>(model => model.CompletedBy),
                        IndexHelper.CreateHashedIndex<Result>(model => model.OwnerTaskId),
                        IndexHelper.CreateAscendingIndex<Result>(model => model.CreationDate,
                                                                 expireAfter: options.DataRetention),
                        IndexHelper.CreateAscendingIndex<Result>(model => model.CompletionDate),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  public async Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                         Options.MongoDB      options)
    => await sessionHandle.shardCollection(options,
                                           CollectionName)
                          .ConfigureAwait(false);
}
