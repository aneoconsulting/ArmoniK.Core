using System;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using JetBrains.Annotations;

namespace ArmoniK.Core
{
  public static class RpcExt
  {

    [ItemNotNull]
    public static async Task<TMessage> WrapRpcException<TMessage>([NotNull] this AsyncUnaryCall<TMessage> asyncUnaryCall)
    {
      Contract.Requires<ArgumentNullException>(asyncUnaryCall is not null, nameof(asyncUnaryCall) + " != null");
      Contract.Requires<ArgumentNullException>(asyncUnaryCall.ResponseAsync is not null, "asyncUnaryCall.ResponseAsync != null");

      try
      {
        await asyncUnaryCall;
      }
      catch (RpcException e)
      {
        throw new ArmoniKException("An error occurred while creating a new job", e);
      }

      return asyncUnaryCall.ResponseAsync.Result!;
    }

    public static bool IsValid(this Lease lease) 
      => !string.IsNullOrEmpty(lease.LeaseId) && lease.ExpirationDate.CompareTo(Timestamp.FromDateTime(DateTime.UtcNow)) > 0;

    public static string ToPrintableId(this TaskId taskId)
      => $"{taskId.Session}|{taskId.SubSession}|{taskId.Task}";

  }
}
