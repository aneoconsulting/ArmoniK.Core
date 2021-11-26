// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Adapters.MongoDB
{
  public class QueueMessageModel
  {
    [BsonId(IdGenerator = typeof( CombGuidGenerator))]
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
  }
}
