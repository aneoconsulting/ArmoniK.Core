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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using TaskCanceledException = ArmoniK.Core.Common.Exceptions.TaskCanceledException;
using TimeoutException = ArmoniK.Core.Common.Exceptions.TimeoutException;

namespace ArmoniK.Core.Common.gRPC;

public static class RpcExt
{
  public static void ThrowIfError(this Status status)
  {
    switch (status.StatusCode)
    {
      case StatusCode.OK:
        return;
      case StatusCode.DeadlineExceeded:
        throw new TimeoutException("Deadline Exceeded. " + status.Detail);
      case StatusCode.Cancelled:
        throw new TaskCanceledException("Operation Cancelled. " + status.Detail);
      case StatusCode.InvalidArgument:
        throw new ArmoniKException("Invalid argument in gRPC call. " + status.Detail);
      case StatusCode.NotFound:
        throw new ArmoniKException("Could not find gRPC method. " + status.Detail);
      case StatusCode.PermissionDenied:
        throw new ArmoniKException("Permission denied in gRPC method. " + status.Detail);
      case StatusCode.Unauthenticated:
        throw new ArmoniKException("Could not authenticate in gRPC method. " + status.Detail);
      case StatusCode.Unimplemented:
        throw new ArmoniKException("Method called was not implemented." + status.Detail);
      case StatusCode.Internal:
      case StatusCode.Unavailable:
      case StatusCode.DataLoss:
      case StatusCode.Unknown:
      case StatusCode.AlreadyExists:
      case StatusCode.ResourceExhausted:
      case StatusCode.FailedPrecondition:
      case StatusCode.Aborted:
      case StatusCode.OutOfRange:
      default:
        throw new ArmoniKException("An error occurred while computing the request. " + status.Detail);
    }
  }

  public static bool HandleExceptions(Exception e, StatusCode status)
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
        Console.WriteLine($"Type of Exception is {e.GetType()}");
        return false;
    }
  }

  public static async Task<TMessage> WrapRpcException<TMessage>([NotNull] this AsyncUnaryCall<TMessage> asyncUnaryCall)
  {
    try
    {
      await asyncUnaryCall;
      return asyncUnaryCall.ResponseAsync.Result;
    }
    catch (Exception e)
    {
      //await Task.Delay(TimeSpan.FromHours(2));
      if (!HandleExceptions(e,
                            asyncUnaryCall.GetStatus().StatusCode))
        throw;

      throw new ArmoniKException("An exception occurred during the rpc call but has been handled",
                                 e);
    }
  }

  public static async Task ForceMoveNext<T>(this IAsyncEnumerator<T> stream, string error, ILogger logger, CancellationToken cancellationToken) where T : class
  {
    if (!await stream.MoveNextAsync(cancellationToken))
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           error));
      logger.LogError(exception,
                       "Invalid stream");
      throw exception;
    }
  }

  public static string ToPrintableId(this TaskId taskId)
    => $"{taskId.Session}|{taskId.Task}";
}