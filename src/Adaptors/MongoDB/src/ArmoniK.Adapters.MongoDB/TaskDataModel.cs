// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class TaskDataModel : IMongoDataModel<TaskDataModel>
  {
    [BsonElement]
    [BsonRequired]
    public string SessionId { get; set; }

    [BsonElement]
    [BsonRequired]
    public string SubSessionId { get; set; }

    [BsonId(IdGenerator = typeof(StringCombGuidGenerator))]
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

    /// <inheritdoc />
    public string CollectionName { get; } = "tasks";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(IClientSessionHandle            sessionHandle,
                                       IMongoCollection<TaskDataModel> collection)
    {
      var sessionIndex       = Builders<TaskDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex    = Builders<TaskDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var taskIndex          = Builders<TaskDataModel>.IndexKeys.Text(model => model.TaskId);
      var statusIndex        = Builders<TaskDataModel>.IndexKeys.Text(model => model.Status);
      var taskIdIndex        = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex, subSessionIndex, taskIndex);
      var sessionStatusIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex, statusIndex);

      var indexModels = new CreateIndexModel<TaskDataModel>[]
                        {
                          new(sessionIndex, new CreateIndexOptions { Name       = nameof(sessionIndex) }),
                          new(taskIdIndex, new CreateIndexOptions { Name        = nameof(taskIdIndex), Unique = true }),
                          new(sessionStatusIndex, new CreateIndexOptions { Name = nameof(sessionStatusIndex) }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }
  }
}
