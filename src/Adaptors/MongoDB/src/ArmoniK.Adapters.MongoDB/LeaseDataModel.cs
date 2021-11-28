// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class LeaseDataModel : IMongoDataModel<LeaseDataModel>
  {
    [BsonId]
    public string Key { get; set; }

    [BsonElement]
    public string Lock { get; set; }

    [BsonElement]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc,DateOnly = false)]
    public DateTime ExpiresAt { get; set; }

    /// <inheritdoc />
    public string CollectionName { get; } = "Lease";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(
      IClientSessionHandle             sessionHandle,
      IMongoCollection<LeaseDataModel> collection)
    {
      var keyIndex   = Builders<LeaseDataModel>.IndexKeys.Text(model => model.Key);
      var lockIndex  = Builders<LeaseDataModel>.IndexKeys.Text(model => model.Lock);
      var wholeIndex = Builders<LeaseDataModel>.IndexKeys.Combine(keyIndex, lockIndex);

      var indexModels = new CreateIndexModel<LeaseDataModel>[]
                        {
                          new(keyIndex, new CreateIndexOptions { Name   = nameof(keyIndex), Unique   = true }),
                          new(wholeIndex, new CreateIndexOptions { Name = nameof(wholeIndex), Unique = true }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }
  }
}
