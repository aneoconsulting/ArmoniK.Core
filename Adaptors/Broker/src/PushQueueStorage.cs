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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

internal class PushQueueStorage : IPushQueueStorage
{
  private readonly BrokerClient           client_;
  private readonly ILogger<PushQueueStorage> logger_;
  private readonly Broker                 options_;
  private          bool                      isInitialized_;

  public PushQueueStorage(BrokerClient           client,
                          Broker                 options,
                          ILogger<PushQueueStorage> logger)
  {
    client_  = client;
    options_ = options;
    logger_  = logger;
  }

  /// <inheritdoc />
  public int MaxPriority
    => Protocol.MaxPriority;

  /// <inheritdoc />
  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = default)
  {
    // One request per homogeneous batch: same partition, fairness key and priority (protocol §6.1).
    foreach (var group in messages.GroupBy(m => (m.FairnessKey, m.Options.Priority)))
    {
      foreach (var chunk in group.Chunk(client_.MaxBatchItems))
      {
        var items = chunk.Select(m => new BrokerClient.EnqueueItem(m.TaskId,
                                                                      AffinityOf(m)))
                         .ToList();
        await client_.EnqueueAsync(partitionId,
                                   group.Key.FairnessKey,
                                   group.Key.Priority,
                                   items,
                                   cancellationToken)
                     .ConfigureAwait(false);
      }
    }

    logger_.LogDebug("Pushed messages to broker partition {PartitionId}",
                     partitionId);
  }

  private BrokerClient.AffinityBody? AffinityOf(MessageData message)
  {
    if (!options_.Affinity || message.Dependencies is not { Count: > 0 } deps)
    {
      return null;
    }

    var a = Affinity.Select(deps.Select(d => (d.Id, (ulong)d.Size)));
    return a is null
             ? null
             : new BrokerClient.AffinityBody(a.Hashes,
                                                a.Sizes.Select(s => (int)s)
                                                 .ToArray(),
                                                a.DepCount,
                                                a.TotalSize);
  }

  /// <inheritdoc />
  public bool UsesDataDependencies
    => options_.Affinity;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await client_.RefreshLimitsAsync(cancellationToken)
                   .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }
}
