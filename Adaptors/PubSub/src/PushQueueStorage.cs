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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Encoding = System.Text.Encoding;

namespace ArmoniK.Core.Adapters.PubSub;

internal class PushQueueStorage : IPushQueueStorage
{
  private readonly PubSub                    options_;
  private readonly PublisherServiceApiClient publisher_;
  private          bool                      isInitialized_;

  public PushQueueStorage(PublisherServiceApiClient publisher,
                          PubSub                    options)
  {
    publisher_ = publisher;
    options_   = options;
  }

  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = new())
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PushQueueStorage)} should be initialized before calling this method.");
    }

    var topic = $"a{options_.Prefix}-{partitionId}";
    var topicName = TopicName.FromProjectTopic(options_.ProjectId,
                                               topic);
    foreach (var chunks in messages.Chunk(500))
    {
      await Publish(topicName,
                    chunks)
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


  private async Task Publish(TopicName                topicName,
                             ICollection<MessageData> messages)
  {
    try
    {
      await publisher_.PublishAsync(topicName,
                                    messages.Select(msg => new PubsubMessage
                                                           {
                                                             Data = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(msg.TaskId)),
                                                           }))
                      .ConfigureAwait(false);
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
    {
      try
      {
        await publisher_.CreateTopicAsync(new Topic
                                          {
                                            MessageRetentionDuration = Duration.FromTimeSpan(options_.MessageRetention),
                                            TopicName                = topicName,
                                            KmsKeyName               = options_.KmsKeyName,
                                          })
                        .ConfigureAwait(false);
      }
      catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
      {
      }

      await publisher_.PublishAsync(topicName,
                                    messages.Select(msg => new PubsubMessage
                                                           {
                                                             Data = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(msg.TaskId)),
                                                           }))
                      .ConfigureAwait(false);
    }
  }
}
