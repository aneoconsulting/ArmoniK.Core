// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Adapters.MongoDB
{
  public class TaskDataModel
  {
    [BsonElement]
    [BsonRequired]
    public string SessionId { get; set; }

    [BsonElement]
    [BsonRequired]
    public string SubSessionId { get; set; }

    [BsonId(IdGenerator = typeof(CombGuidGenerator))]
    public string TaskId { get; set; }

    [BsonElement]
    [BsonRequired]
    public TaskOptions Options { get; set; }

    [BsonElement]
    public Core.gRPC.V1.TaskStatus Status { get; set; }

    [BsonElement]
    public int Retries { get; set; }

    [BsonElement]
    [BsonRequired]
    public bool HasPayload { get; set; }

    [BsonElement]
    public Payload Payload { get; set; }

    public TaskData ToTaskData() => new()
                                    {
                                      Id = new TaskId
                                           {
                                             Session    = SessionId,
                                             SubSession = SubSessionId,
                                             Task       = TaskId,
                                           },
                                      HasPayload = HasPayload,
                                      Payload    = Payload,
                                      Options    = Options,
                                      Retries    = Retries,
                                      Status     = Status,
                                    };

    public TaskId GetTaskId() => new() { Session = SessionId, SubSession = SubSessionId, Task = TaskId };
  }
}
