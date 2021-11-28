
using ArmoniK.Core.Injection;

using MongoDB.Driver;


namespace ArmoniK.Adapters.MongoDB
{
  public class SessionProvider : ProviderBase<IClientSessionHandle>
  {
    public SessionProvider(IMongoClient client):
      base(()=> client.StartSessionAsync())
    {}
  }
}
