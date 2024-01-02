// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Object;

public class ObjectDataModelMapping : IMongoDataModelMapping<ObjectDataModelMapping>
{
  public const string Collection = "Object";

  [BsonId]
  public string Id
    => $"{Key}.{ChunkIdx}";

  [BsonElement]
  public string Key { get; set; } = "";

  [BsonElement]
  public byte[] Chunk { get; set; } = Array.Empty<byte>();

  [BsonElement]
  public int ChunkIdx { get; set; }

  /// <inheritdoc />
  [BsonIgnore]
  public string CollectionName { get; } = Collection;

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle                     sessionHandle,
                                           IMongoCollection<ObjectDataModelMapping> collection,
                                           Options.MongoDB                          options)
  {
    var keyIndex      = Builders<ObjectDataModelMapping>.IndexKeys.Hashed(model => model.Key);
    var chunkIdxIndex = Builders<ObjectDataModelMapping>.IndexKeys.Hashed(model => model.ChunkIdx);
    var iDIndex       = Builders<ObjectDataModelMapping>.IndexKeys.Hashed(model => model.Id);

    var indexModels = new CreateIndexModel<ObjectDataModelMapping>[]
                      {
                        new(iDIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(iDIndex),
                            }),
                        new(keyIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(keyIndex),
                            }),
                        new(chunkIdxIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(chunkIdxIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
