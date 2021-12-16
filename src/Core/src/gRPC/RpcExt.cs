// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using JetBrains.Annotations;

using TaskCanceledException = ArmoniK.Core.Exceptions.TaskCanceledException;
using TimeoutException = ArmoniK.Core.Exceptions.TimeoutException;

namespace ArmoniK.Core.gRPC
{
  public static class RpcExt
  {
    private static bool HandleExceptions(Exception e, StatusCode status)
    {
      switch (e)
      {
        case RpcException:
        {
          switch (status)
          {
            case StatusCode.DeadlineExceeded:
              throw new TimeoutException("Deadline Exceeded",
                                         e);
            case StatusCode.OK:
              break;
            case StatusCode.Cancelled:
              throw new TaskCanceledException("Operation Cancelled",
                                              e);
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
              throw new ArmoniKException("An error occurred while computing the request",
                                         e);
          }

          return true;
        }
        case AggregateException ae:
        {
          return ae.InnerExceptions.All(ie => HandleExceptions(ie,
                                                               status));
        }
        default:
          return false;
      }
    }

    [ItemNotNull]
    public static async Task<TMessage> WrapRpcException<TMessage>([NotNull] this AsyncUnaryCall<TMessage> asyncUnaryCall)
    {
      try
      {
        await asyncUnaryCall;
      }
      catch (Exception e)
      {
        if (!HandleExceptions(e,
                              asyncUnaryCall.GetStatus().StatusCode))
          throw;
      }

      return asyncUnaryCall.ResponseAsync.Result!;
    }

    public static bool IsValid(this Lease lease)
    {
      return !string.IsNullOrEmpty(lease.LeaseId) && lease.ExpirationDate.CompareTo(Timestamp.FromDateTime(DateTime.UtcNow)) > 0;
    }

    public static string ToPrintableId(this TaskId taskId)
    {
      return $"{taskId.Session}|{taskId.SubSession}|{taskId.Task}";
    }
  }
}
