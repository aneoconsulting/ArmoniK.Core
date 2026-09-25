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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly BrokerClient           client_;
  private readonly ILogger<PullQueueStorage> logger_;
  private          bool                      isInitialized_;

  public PullQueueStorage(BrokerClient           client,
                          ILogger<PullQueueStorage> logger)
  {
    client_ = client;
    logger_ = logger;
  }

  /// <inheritdoc />
  public int MaxPriority
    => Protocol.MaxPriority;

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var (consumerId, messages) = await client_.PullAsync(partitionId,
                                                         nbMessages,
                                                         cancellationToken)
                                              .ConfigureAwait(false);
    foreach (var m in messages)
    {
      yield return new QueueMessageHandler(client_,
                                           consumerId,
                                           m,
                                           logger_);
    }
  }

  /// <summary>
  ///   Healthy once initialized, including while the broker restarts: an unhealthy liveness is final
  ///   for the Pollster, and pull already absorbs the unavailability.
  /// </summary>
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_ && !await client_.RefreshLimitsAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      logger_.LogWarning("Broker is not reachable yet; pulls will retry");
    }

    isInitialized_ = true;
  }
}
