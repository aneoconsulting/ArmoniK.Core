// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using MongoDB.Bson.Serialization.Attributes;

namespace ArmoniK.Adapters.MongoDB
{
  public class ObjectDataModel
  {
    [BsonId]
    public string Id => $"{Key}{ChunkIdx}";

    [BsonElement]
    public string Key { get; set; }

    [BsonElement]
    public byte[] Chunk { get; set; }

    [BsonElement]
    public int ChunkIdx { get; set; }
  }
}
