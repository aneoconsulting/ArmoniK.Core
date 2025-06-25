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

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.PubSub;

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly int     ackDeadlinePeriod_;
  private readonly int     ackExtendDeadlineStep_;
  private readonly bool    exactlyOnceDelivery_;
  private readonly string  kmsKeyName_;
  private readonly ILogger logger_;
  private readonly bool    messageOrdering_;

  private readonly TimeSpan                   messageRetention_;
  private readonly string                     prefix_;
  private readonly string                     project_;
  private readonly PublisherServiceApiClient  publisher_;
  private readonly SubscriberServiceApiClient subscriber_;
  private          bool                       isInitialized_;

  public PullQueueStorage(SubscriberServiceApiClient subscriber,
                          PublisherServiceApiClient  publisher,
                          PubSub                     options,
                          ILogger<PullQueueStorage>  logger)
  {
    messageRetention_      = options.MessageRetention;
    ackDeadlinePeriod_     = options.AckDeadlinePeriod;
    ackExtendDeadlineStep_ = options.AckExtendDeadlineStep;
    exactlyOnceDelivery_   = options.ExactlyOnceDelivery;
    kmsKeyName_            = options.KmsKeyName;
    messageOrdering_       = options.MessageOrdering;
    subscriber_            = subscriber;
    publisher_             = publisher;
    prefix_                = options.Prefix;
    project_               = options.ProjectId;
    logger_                = logger;
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    var topic = $"a{prefix_}-{partitionId}";
    var sub   = $"a{prefix_}-{partitionId}-ak-sub";

    var topicName = TopicName.FromProjectTopic(project_,
                                               topic);
    var subscriptionName = SubscriptionName.FromProjectSubscription(project_,
                                                                    sub);
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
      try
      {
        await publisher_.CreateTopicAsync(new Topic
                                          {
                                            MessageRetentionDuration = Duration.FromTimeSpan(messageRetention_),
                                            TopicName                = topicName,
                                            KmsKeyName               = kmsKeyName_,
                                          })
                        .ConfigureAwait(false);
      }
      catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
      {
      }

      var subscriptionRequest = new Subscription
                                {
                                  SubscriptionName          = subscriptionName,
                                  TopicAsTopicName          = topicName,
                                  EnableExactlyOnceDelivery = exactlyOnceDelivery_,
                                  EnableMessageOrdering     = messageOrdering_,
                                  AckDeadlineSeconds        = ackDeadlinePeriod_,
                                };
      try
      {
        await subscriber_.CreateSubscriptionAsync(subscriptionRequest)
                         .ConfigureAwait(false);
      }
      catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
      {
      }

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
                                           ackDeadlinePeriod_,
                                           ackExtendDeadlineStep_,
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
