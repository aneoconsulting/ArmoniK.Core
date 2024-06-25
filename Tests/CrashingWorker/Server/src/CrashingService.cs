// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;

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
    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId));

    Environment.Exit(1);

    return Task.FromResult(new Output
                           {
                             Ok = new Empty(),
                           });
  }
}
