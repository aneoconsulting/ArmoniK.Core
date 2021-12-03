using System;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using JetBrains.Annotations;

using TaskCanceledException = ArmoniK.Core.Exceptions.TaskCanceledException;
using TimeoutException = ArmoniK.Core.Exceptions.TimeoutException;

namespace ArmoniK.Core
{
  public static class RpcExt
  {
    [ItemNotNull]
    public static async Task<TMessage> WrapRpcException<TMessage>([NotNull] this AsyncUnaryCall<TMessage> asyncUnaryCall)
    {
      Contract.Requires<ArgumentNullException>(asyncUnaryCall is not null, nameof(asyncUnaryCall) + " != null");
      Contract.Requires<ArgumentNullException>(asyncUnaryCall.ResponseAsync is not null,
                                               "asyncUnaryCall.ResponseAsync != null");

      try
      {
        await asyncUnaryCall;
      }
      catch (RpcException e)
      {
        switch (asyncUnaryCall.GetStatus().StatusCode)
        {
          case StatusCode.DeadlineExceeded:
            throw new TimeoutException("Deadline Exceeded", e);
          case StatusCode.OK:
            break;
          case StatusCode.Cancelled:
            throw new TaskCanceledException("Operation Cancelled", e);
          case StatusCode.Unknown:
            break;
          case StatusCode.InvalidArgument:
            break;
          case StatusCode.NotFound:
            break;
          case StatusCode.AlreadyExists:
            break;
          case StatusCode.PermissionDenied:
            break;
          case StatusCode.Unauthenticated:
            break;
          case StatusCode.ResourceExhausted:
            break;
          case StatusCode.FailedPrecondition:
            break;
          case StatusCode.Aborted:
            break;
          case StatusCode.OutOfRange:
            break;
          case StatusCode.Unimplemented:
            break;
          case StatusCode.Internal:
            break;
          case StatusCode.Unavailable:
            break;
          case StatusCode.DataLoss:
            break;
          default:
            throw new ArmoniKException("An error occurred while computing the request", e);
        }
      }

      return asyncUnaryCall.ResponseAsync.Result!;
    }

    public static bool IsValid(this Lease lease)
      => !string.IsNullOrEmpty(lease.LeaseId) && lease.ExpirationDate.CompareTo(Timestamp.FromDateTime(DateTime.UtcNow)) > 0;

    public static string ToPrintableId(this TaskId taskId)
      => $"{taskId.Session}|{taskId.SubSession}|{taskId.Task}";
  }
}