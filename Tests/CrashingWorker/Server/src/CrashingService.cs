// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// In samples, Random can be used

#pragma warning disable SEC0115

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.CrashingWorker.Server;

[UsedImplicitly]
public class CrashingService : WorkerStreamWrapper
{
  public CrashingService(ILoggerFactory      loggerFactory,
                         ComputePlane        options,
                         GrpcChannelProvider provider)
    : base(loggerFactory,
           options,
           provider)
    => logger_ = loggerFactory.CreateLogger<CrashingService>();

  public override Task<Output> Process(ITaskHandler taskHandler)
  {
    var type = taskHandler.TaskOptions.Options.GetValueOrDefault("type",
                                                                 null)
                          ?.ToLower();

    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId),
                                                  ("type", type ?? ""));

    switch (type)
    {
      case "success":
        return Task.FromResult(new Output
                               {
                                 Ok = new Empty(),
                               });
      case "error":
        return Task.FromResult(new Output
                               {
                                 Error = new Output.Types.Error
                                         {
                                           Details = "Deterministic error",
                                         },
                               });
      case "exception":
        throw new ApplicationException("Deterministic exception");
      case "rpc-cancelled":
        throw new RpcException(new Status(StatusCode.Cancelled,
                                          "Deterministic RPC exception: Cancelled"));
      case "rpc-unknown":
        throw new RpcException(new Status(StatusCode.Unknown,
                                          "Deterministic RPC exception: Unknown"));
      case "rpc-invalid-argument":
        throw new RpcException(new Status(StatusCode.InvalidArgument,
                                          "Deterministic RPC exception: InvalidArgument"));
      case "rpc-deadline-exceeded":
        throw new RpcException(new Status(StatusCode.DeadlineExceeded,
                                          "Deterministic RPC exception: DeadlineExceeded"));
      case "rpc-not-found":
        throw new RpcException(new Status(StatusCode.NotFound,
                                          "Deterministic RPC exception: NotFound"));
      case "rpc-already-exists":
        throw new RpcException(new Status(StatusCode.AlreadyExists,
                                          "Deterministic RPC exception: AlreadyExists"));
      case "rpc-permission-denied":
        throw new RpcException(new Status(StatusCode.PermissionDenied,
                                          "Deterministic RPC exception: PermissionDenied"));
      case "rpc-resource-exhausted":
        throw new RpcException(new Status(StatusCode.ResourceExhausted,
                                          "Deterministic RPC exception: ResourceExhausted"));
      case "rpc-failed-precondition":
        throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                          "Deterministic RPC exception: FailedPrecondition"));
      case "rpc-aborted":
        throw new RpcException(new Status(StatusCode.Aborted,
                                          "Deterministic RPC exception: Aborted"));
      case "rpc-out-of-range":
        throw new RpcException(new Status(StatusCode.OutOfRange,
                                          "Deterministic RPC exception: OutOfRange"));
      case "rpc-unimplemented":
        throw new RpcException(new Status(StatusCode.Unimplemented,
                                          "Deterministic RPC exception: Unimplemented"));
      case "rpc-internal":
        throw new RpcException(new Status(StatusCode.Internal,
                                          "Deterministic RPC exception: Internal"));
      case "rpc-unavailable":
        throw new RpcException(new Status(StatusCode.Unavailable,
                                          "Deterministic RPC exception: Unavailable"));
      case "rpc-data-loss":
        throw new RpcException(new Status(StatusCode.DataLoss,
                                          "Deterministic RPC exception: DataLoss"));
      case "rpc-unauthenticated":
        throw new RpcException(new Status(StatusCode.Unauthenticated,
                                          "Deterministic RPC exception: Unauthenticated"));
      case "exit":
        Environment.Exit(1);
        break;
      case "kill":
        System.Diagnostics.Process.GetCurrentProcess()
              .Kill();
        break;
    }

    Environment.FailFast("Deterministic Abort");

    throw new UnreachableException();
  }
}
