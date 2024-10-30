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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimplePullQueueStorageChannel : IPullQueueStorage, IPushQueueStorage
{
  public readonly Channel<IQueueMessageHandler> Channel     = System.Threading.Channels.Channel.CreateUnbounded<IQueueMessageHandler>();
  public          HealthCheckResult             CheckResult = HealthCheckResult.Healthy();

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(CheckResult);

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public int MaxPriority
    => 10;

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    foreach (var _ in Enumerable.Range(0,
                                       nbMessages))
    {
      if (cancellationToken.IsCancellationRequested)
      {
        yield break;
      }

      IQueueMessageHandler? msg;

      try
      {
        msg = await Channel.Reader.ReadAsync(cancellationToken)
                           .ConfigureAwait(false);
      }
      catch (OperationCanceledException)
      {
        yield break;
      }

      yield return msg;
    }
  }

  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = default)
  {
    foreach (var message in messages)
    {
      await Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                      {
                                        MessageId = Guid.NewGuid()
                                                        .ToString(),
                                        Status = QueueMessageStatus.Running,
                                        TaskId = message.TaskId,
                                      },
                                      cancellationToken)
                   .ConfigureAwait(false);
    }
  }

  public async Task EmptyAsync(CancellationToken cancellationToken = default)
  {
    if (!Channel.Reader.CanCount)
    {
      throw new InvalidOperationException("Channel should be countable");
    }

    while (Channel.Reader.Count > 0)
    {
      await Channel.Reader.ReadAsync(cancellationToken)
                   .ConfigureAwait(false);
    }
  }
}
