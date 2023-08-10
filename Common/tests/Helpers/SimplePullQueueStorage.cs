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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimplePullQueueStorage : IPullQueueStorage
{
  public readonly ConcurrentBag<string> Messages = new();

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public int MaxPriority
    => 10;


  public IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int               nbMessages,
                                                                  CancellationToken cancellationToken = default)
    // using ToAsyncEnumerable avoids using an async function needlessly
    => Enumerable.Repeat(0,
                         nbMessages)
                 .Select(_ =>
                         {
                           var success = Messages.TryTake(out var m);
                           cancellationToken.ThrowIfCancellationRequested();
                           return (success, message: m);
                         })
                 .Where(tuple => tuple.success)
                 .Select(tuple => new SimpleQueueMessageHandler
                                  {
                                    CancellationToken = CancellationToken.None,
                                    TaskId            = tuple.message!,
                                    MessageId = Guid.NewGuid()
                                                    .ToString(),
                                    Status            = QueueMessageStatus.Running,
                                    ReceptionDateTime = DateTime.UtcNow,
                                  } as IQueueMessageHandler)
                 .ToAsyncEnumerable();
}
