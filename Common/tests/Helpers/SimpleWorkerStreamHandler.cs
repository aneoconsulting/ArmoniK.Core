// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Output = ArmoniK.Api.gRPC.V1.Output;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleWorkerStreamHandler : IWorkerStreamHandler
{
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public void Dispose()
  {
  }

  public void StartTaskProcessing(TaskData          taskData,
                                  CancellationToken cancellationToken)
    => Pipe = new ChannelAsyncPipe<ProcessReply, ProcessRequest>(new ProcessReply
                                                                 {
                                                                   CommunicationToken = "",
                                                                   Output = new Output
                                                                            {
                                                                              Ok = new Empty(),
                                                                            },
                                                                 });

  public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; private set; }
}
