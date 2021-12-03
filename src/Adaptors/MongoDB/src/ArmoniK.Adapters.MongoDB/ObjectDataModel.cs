// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

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
      var combinedIndex = Builders<ObjectDataModel>.IndexKeys.Combine(keyIndex, chunkIdxIndex);


      var indexModels = new CreateIndexModel<ObjectDataModel>[]
      {
        new(iDIndex, new CreateIndexOptions { Name       = nameof(iDIndex), Unique = true }),
        new(keyIndex, new CreateIndexOptions { Name      = nameof(keyIndex) }),
        new(combinedIndex, new CreateIndexOptions { Name = nameof(combinedIndex), Unique = true }),
      };

      return collection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }
  }
}