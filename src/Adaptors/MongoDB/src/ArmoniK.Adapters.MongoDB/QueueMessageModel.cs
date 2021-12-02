// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class QueueMessageModel : IMongoDataModel<QueueMessageModel>
  {
    [BsonId(IdGenerator = typeof( StringCombGuidGenerator))]
    public string MessageId { get; set; }

    [BsonElement]
    public TaskId TaskId { get; set; }

    [BsonElement]
    public string OwnerId { get; set; }

    [BsonElement]
    public DateTime SubmissionDate { get; set; }

    [BsonElement]
    public int Priority { get; set; }

    [BsonElement]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc,DateOnly = false)]
    public DateTime OwnedUntil { get; set; }

    /// <inheritdoc />
    public string CollectionName { get; } = "Queue";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(
      IClientSessionHandle                sessionHandle,
      IMongoCollection<QueueMessageModel> collection)
    {
      var messageIdIndex  = Builders<QueueMessageModel>.IndexKeys.Text(model => model.MessageId);
      var ownerIdIndex    = Builders<QueueMessageModel>.IndexKeys.Text(model => model.OwnerId);
      var submissionIndex = Builders<QueueMessageModel>.IndexKeys.Ascending(model => model.SubmissionDate);
      var priorityIndex   = Builders<QueueMessageModel>.IndexKeys.Descending(model => model.Priority);
      var ownedUntilIndex = Builders<QueueMessageModel>.IndexKeys.Text(model => model.OwnedUntil);
      var pullIndex       = Builders<QueueMessageModel>.IndexKeys.Combine(submissionIndex, priorityIndex, ownedUntilIndex);
      var lockedIndex     = Builders<QueueMessageModel>.IndexKeys.Combine(messageIdIndex, ownerIdIndex);


      var indexModels = new CreateIndexModel<QueueMessageModel>[]
                        {
                          new(pullIndex, new CreateIndexOptions { Name      = nameof(pullIndex) }),
                          new(lockedIndex, new CreateIndexOptions { Name    = nameof(lockedIndex), Unique    = true }),
                          new(messageIdIndex, new CreateIndexOptions { Name = nameof(messageIdIndex), Unique = true }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }

  }
}
