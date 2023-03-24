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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimplePullQueueStorage : IPullQueueStorage
{
  public readonly ConcurrentBag<string> Messages;

  public SimplePullQueueStorage()
    => Messages = new ConcurrentBag<string>();

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public int MaxPriority { get; } = 10;

#pragma warning disable CS1998
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int nbMessages,
#pragma warning restore CS1998
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var i = 0;
    while (i < nbMessages && Messages.TryTake(out var m))
    {
      i++;
      cancellationToken.ThrowIfCancellationRequested();
      yield return new SimpleQueueMessageHandler
                   {
                     CancellationToken = CancellationToken.None,
                     TaskId            = m,
                     MessageId = Guid.NewGuid()
                                     .ToString(),
                     Status            = QueueMessageStatus.Running,
                     ReceptionDateTime = DateTime.UtcNow,
                   };
    }
  }
}
