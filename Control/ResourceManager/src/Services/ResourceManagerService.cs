using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.gRPC.V1;

using Google.Protobuf;

using Grpc.Core;

namespace ArmoniK.Core.Control.ResourceManager.Services
{

  public class ResourceManagerService : gRPC.V1.ResourceManagerService.ResourceManagerServiceBase
  {
    private readonly IObjectStorage objectStorage_;

    public ResourceManagerService(IObjectStorage objectStorage)
      => objectStorage_ = objectStorage;

    /// <inheritdoc />
    public override async Task<Empty> DeleteResources(ResourceRequest request, ServerCallContext context)
    {
      await objectStorage_.DeleteAsync(request.Key,
                                     context.CancellationToken);
      return new();
    }

    /// <inheritdoc />
    public override async Task<Resource> DownloadResources(ResourceRequest request, ServerCallContext context)
      => new()
         {
           Key = request.Key,
           Data = ByteString.CopyFrom(await objectStorage_.GetValuesAsync(request.Key,
                                                                        context.CancellationToken)),
         };

    /// <inheritdoc />
    public override async Task<ResourceList> ListResources(Empty request, ServerCallContext context)
      => new()
         {
           Keys =
           {
             await objectStorage_.ListKeysAsync(context.CancellationToken).ToListAsync(context.CancellationToken),
           },
         };

    /// <inheritdoc />
    public override async Task<Empty> UploadResources(Resource request, ServerCallContext context)
    {
      await objectStorage_.AddOrUpdateAsync(request.Key,
                                          request.Data.ToByteArray());
      return new();
    }
  }
}
