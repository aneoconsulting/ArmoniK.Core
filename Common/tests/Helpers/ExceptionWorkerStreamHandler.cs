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

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public sealed class ExceptionWorkerStreamHandler<T> : IWorkerStreamHandler
  where T : Exception, new()
{
  private readonly bool acceptCancellation_;
  private readonly int  delay_;

  public ExceptionWorkerStreamHandler(int  delay,
                                      bool acceptCancellation = true)
  {
    delay_              = delay;
    acceptCancellation_ = acceptCancellation;
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public void Dispose()
  {
  }

  public async Task<Output> StartTaskProcessing(TaskData          taskData,
                                                string            token,
                                                string            dataFolder,
                                                CancellationToken cancellationToken)
  {
    await Task.Delay(TimeSpan.FromMilliseconds(delay_),
                     acceptCancellation_
                       ? cancellationToken
                       : CancellationToken.None)
              .ConfigureAwait(false);
    throw new T();
  }
}
