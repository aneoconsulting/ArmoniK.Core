// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Adapters.MongoDB
{
  public class LeaseDataModel
  {
    [BsonId]
    public string Key { get; set; }

    [BsonElement]
    public string Lock { get; set; }

    [BsonElement]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc, Representation = BsonType.DateTime)]
    public DateTime ExpiresAt { get; set; }
  }
}
