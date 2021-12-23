// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class ObjectDataModel : IMongoDataModel<ObjectDataModel>
  {
    [BsonId]
    public string Id => $"{Key}{ChunkIdx}";

    [BsonElement]
    public string Key { get; set; }

    [BsonElement]
    public byte[] Chunk { get; set; }

    [BsonElement]
    public int ChunkIdx { get; set; }

    [BsonIgnore]
    /// <inheritdoc />
    public string CollectionName { get; } = "Object";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(
      IClientSessionHandle              sessionHandle,
      IMongoCollection<ObjectDataModel> collection)
    {
      var keyIndex      = Builders<ObjectDataModel>.IndexKeys.Text(model => model.Key);
      var chunkIdxIndex = Builders<ObjectDataModel>.IndexKeys.Text(model => model.ChunkIdx);
      var iDIndex       = Builders<ObjectDataModel>.IndexKeys.Text(model => model.Id);
      var combinedIndex = Builders<ObjectDataModel>.IndexKeys.Combine(keyIndex,
                                                                      chunkIdxIndex);


      var indexModels = new CreateIndexModel<ObjectDataModel>[]
                        {
                          new(iDIndex,
                              new()
                              {
                                Name   = nameof(iDIndex),
                                Unique = true,
                              }),
                          new(keyIndex,
                              new()
                              {
                                Name = nameof(keyIndex),
                              }),
                          new(combinedIndex,
                              new()
                              {
                                Name   = nameof(combinedIndex),
                                Unique = true,
                              }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }
  }
}
