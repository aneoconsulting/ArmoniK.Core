using ArmoniK.Core.gRPC.V1;

using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ArmoniK.Adapters.MongoDB
{
  public class SessionDataModel
  {
    public class ParentId
    {
      [BsonElement]
      public string Id { get; set; }
    }

    [BsonIgnore]
    public string IdTag { get; set; }

    [BsonElement]
    [BsonRequired]
    public string SessionId { get; set; }

    [BsonId(IdGenerator = typeof(SessionIdGenerator))]
    public string SubSessionId { get; set; }

    [BsonElement]
    public List<ParentId> ParentsId { get; set; }

    [BsonElement]
    public bool IsClosed{ get; set; }

    [BsonElement]
    public bool IsCancelled { get; set; }

    [BsonElement]
    [BsonRequired]
    public TaskOptions Options { get; set; }
  }
}
