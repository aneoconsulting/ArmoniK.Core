// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Output = ArmoniK.Core.Common.Storage.Output;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleWorkerStreamHandler : IWorkerStreamHandler
{
  // Continuations run on the thread pool: a test awaiting Started must not resume on the pollster's own
  // thread while it is handing the task over, or test and pollster serialise on that thread.
  private readonly TaskCompletionSource started_ = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public Output Output = new(OutputStatus.Success,
                             "");

  /// <summary>
  ///   Completes the first time the pollster hands a task to this worker.
  /// </summary>
  public Task Started
    => started_.Task;

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public void Dispose()
    => GC.SuppressFinalize(this);

  public Task<Output> StartTaskProcessing(TaskData          taskData,
                                          string            token,
                                          string            dataFolder,
                                          Configuration     configuration,
                                          CancellationToken cancellationToken)
  {
    started_.TrySetResult();

    return Task.FromResult(Output);
  }
}
