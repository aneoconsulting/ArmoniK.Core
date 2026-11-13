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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Google.Cloud.PubSub.V1;
using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Encoding = System.Text.Encoding;

namespace ArmoniK.Core.Adapters.PubSub;

internal class PushQueueStorage : IPushQueueStorage
{
  /// <summary>
  ///   Partitions whose topic and subscription have already been ensured by this process
  /// </summary>
  private readonly ConcurrentDictionary<string, bool> ensuredPartitions_ = new();

  private readonly ILogger                    logger_;
  private readonly PubSub                     options_;
  private readonly PublisherServiceApiClient  publisher_;
  private readonly SubscriberServiceApiClient subscriber_;
  private          bool                       isInitialized_;

  public PushQueueStorage(PublisherServiceApiClient  publisher,
                          SubscriberServiceApiClient subscriber,
                          PubSub                     options,
                          ILogger<PushQueueStorage>  logger)
  {
    publisher_  = publisher;
    subscriber_ = subscriber;
    options_    = options;
    logger_     = logger;
  }

  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = new())
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PushQueueStorage)} should be initialized before calling this method.");
    }

    await EnsurePartitionAsync(partitionId,
                               cancellationToken)
      .ConfigureAwait(false);

    foreach (var chunks in messages.Chunk(500))
    {
      await Publish(partitionId,
                    chunks,
                    cancellationToken)
        .ConfigureAwait(false);
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

  /// <summary>
  ///   Ensure that the topic and the subscription of the partition exist before publishing to it, once per partition
  ///   for the lifetime of this process.
  /// </summary>
  /// <remarks>
  ///   Pub/Sub only delivers to a subscription the messages published after its creation:
  ///   the subscription must exist before publishing, even if no polling agent has started yet.
  ///   Checking only when publishing fails is not enough, since publishing to a topic without subscription succeeds.
  /// </remarks>
  private async Task EnsurePartitionAsync(string            partitionId,
                                          CancellationToken cancellationToken)
  {
    if (ensuredPartitions_.ContainsKey(partitionId))
    {
      return;
    }

    try
    {
      await PubSubExt.EnsurePartitionIsCreatedAsync(publisher_,
                                                    subscriber_,
                                                    options_,
                                                    partitionId,
                                                    cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.PermissionDenied)
    {
      // Deployments may create the topics and subscriptions beforehand and not grant the permission to create them
      logger_.LogWarning(e,
                         "Not allowed to create the topic or the subscription of partition {PartitionId}: they must be created by the deployment, otherwise the messages published to this partition are lost",
                         partitionId);
    }

    ensuredPartitions_.TryAdd(partitionId,
                              true);
  }

  private async Task Publish(string                   partitionId,
                             ICollection<MessageData> messages,
                             CancellationToken        cancellationToken)
  {
    var topicName = options_.GetTopicName(partitionId);
    var pubsubMessages = messages.Select(msg => new PubsubMessage
                                                {
                                                  Data = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(msg.TaskId)),
                                                })
                                 .ToList();
    try
    {
      await publisher_.PublishAsync(topicName,
                                    pubsubMessages,
                                    cancellationToken)
                      .ConfigureAwait(false);
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
    {
      // The topic has been deleted since the partition was ensured by this process
      ensuredPartitions_.TryRemove(partitionId,
                                   out _);
      await EnsurePartitionAsync(partitionId,
                                 cancellationToken)
        .ConfigureAwait(false);

      await publisher_.PublishAsync(topicName,
                                    pubsubMessages,
                                    cancellationToken)
                      .ConfigureAwait(false);
    }
  }
}
