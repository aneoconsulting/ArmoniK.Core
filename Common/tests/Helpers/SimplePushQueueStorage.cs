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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimplePushQueueStorage : IPushQueueStorage
{
  public readonly ConcurrentBag<string> Messages;

  public SimplePushQueueStorage()
    => Messages = new ConcurrentBag<string>();

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public int MaxPriority { get; } = 10;

  /// <inheritdoc />
  public Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                string                   partitionId,
                                CancellationToken        cancellationToken = default)
  {
    foreach (var msgData in messages)
    {
      Messages.Add(msgData.TaskId);
    }

    return Task.CompletedTask;
  }
}
