using ArmoniK.Core.gRPC.V1;

using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class SessionDataModel : IMongoDataModel<SessionDataModel>
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

    /// <inheritdoc />
    public string CollectionName { get; } = "SessionData";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<SessionDataModel> collection)
    {
      var sessionIndex           = Builders<SessionDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex        = Builders<SessionDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var parentsIndex           = Builders<SessionDataModel>.IndexKeys.Text("ParentsId.Id");
      var sessionSubSessionIndex = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex, subSessionIndex);
      var sessionParentIndex     = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex, parentsIndex);

      var indexModels = new CreateIndexModel<SessionDataModel>[]
                        {
                          new(sessionIndex, new CreateIndexOptions { Name           = nameof(sessionIndex) }),
                          new(sessionSubSessionIndex, new CreateIndexOptions { Name = nameof(sessionSubSessionIndex), Unique = true }),
                          new(sessionParentIndex, new CreateIndexOptions { Name     = nameof(sessionParentIndex) }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }
  }
}
