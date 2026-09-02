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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Google.Cloud.PubSub.V1;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.PubSub;

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly ILogger                    logger_;
  private readonly PubSub                     options_;
  private readonly PublisherServiceApiClient  publisher_;
  private readonly SubscriberServiceApiClient subscriber_;
  private          bool                       isInitialized_;

  public PullQueueStorage(SubscriberServiceApiClient subscriber,
                          PublisherServiceApiClient  publisher,
                          PubSub                     options,
                          ILogger<PullQueueStorage>  logger)
  {
    options_    = options;
    subscriber_ = subscriber;
    publisher_  = publisher;
    logger_     = logger;
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    var          topicName        = options_.GetTopicName(partitionId);
    var          subscriptionName = options_.GetSubscriptionName(partitionId);
    PullResponse messages;
    try
    {
      messages = await subscriber_.PullAsync(subscriptionName,
                                             nbMessages,
                                             cancellationToken)
                                  .ConfigureAwait(false);
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
    {
      await publisher_.EnsureTopicIsCreatedAsync(options_,
                                                 partitionId)
                      .ConfigureAwait(false);
      await subscriber_.EnsureSubscriptionIsCreatedAsync(options_,
                                                         partitionId)
                       .ConfigureAwait(false);

      messages = await subscriber_.PullAsync(subscriptionName,
                                             nbMessages,
                                             cancellationToken)
                                  .ConfigureAwait(false);
    }

    foreach (var message in messages.ReceivedMessages)
    {
      cancellationToken.ThrowIfCancellationRequested();
      yield return new QueueMessageHandler(message,
                                           subscriber_,
                                           subscriptionName,
                                           options_.AckDeadlinePeriod,
                                           options_.AckExtendDeadlineStep,
                                           logger_);
    }
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  public int MaxPriority
    => int.MaxValue;
}
