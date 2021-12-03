using MongoDB.Driver;

using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Attributes;

namespace ArmoniK.Adapters.MongoDB
{
  public interface IMongoDataModel<T>
  {
    [BsonIgnore]
    string CollectionName { get; }

    Task InitializeIndexesAsync(IClientSessionHandle sessionHandle,
                                IMongoCollection<T>  collection);
  }
}