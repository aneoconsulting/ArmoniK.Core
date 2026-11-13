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

using System.Threading;
using System.Threading.Tasks;

using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

namespace ArmoniK.Core.Adapters.PubSub;

internal static class PubSubExt
{
  private static string GetTopicNameInternal(this PubSub options,
                                             string      partition)
  {
    var prefix = options.Prefix;
    if (string.IsNullOrWhiteSpace(prefix))
    {
      prefix = "a";
    }

    return $"{prefix}-{partition}";
  }

  /// <summary>
  ///   Get the <see cref="TopicName" /> for the given <paramref name="partition" />.
  /// </summary>
  /// <param name="options">PubSub options to configure the topic</param>
  /// <param name="partition">Partition the topic refers to</param>
  /// <returns>The topic name in a format understood by GCP</returns>
  public static TopicName GetTopicName(this PubSub options,
                                       string      partition)
    => TopicName.FromProjectTopic(options.ProjectId,
                                  options.GetTopicNameInternal(partition));

  /// <summary>
  ///   Get the <see cref="SubscriptionName" /> for the given <paramref name="partition" />.
  /// </summary>
  /// <param name="options">PubSub options to configure the subscription</param>
  /// <param name="partition">Partition the subscription refers to</param>
  /// <returns>The subscription name in a format understood by GCP</returns>
  internal static SubscriptionName GetSubscriptionName(this PubSub options,
                                                       string      partition)
    => SubscriptionName.FromProjectSubscription(options.ProjectId,
                                                $"{options.GetTopicNameInternal(partition)}-ak-sub");

  /// <summary>
  ///   Create the topic and the subscription of the <paramref name="partition" /> if they do not already exist.
  /// </summary>
  /// <remarks>
  ///   Pub/Sub only delivers to a subscription the messages published after its creation.
  ///   The topic and its subscription are therefore always created together, so that no message can be published
  ///   to a topic without subscription.
  /// </remarks>
  /// <param name="publisher">The publisher client</param>
  /// <param name="subscriber">The subscriber client</param>
  /// <param name="options">PubSub options to configure the topic and the subscription</param>
  /// <param name="partition">Partition the topic and the subscription refer to</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  public static async ValueTask EnsurePartitionIsCreatedAsync(this PublisherServiceApiClient publisher,
                                                              SubscriberServiceApiClient     subscriber,
                                                              PubSub                         options,
                                                              string                         partition,
                                                              CancellationToken              cancellationToken)
  {
    await publisher.EnsureTopicIsCreatedAsync(options,
                                              partition,
                                              cancellationToken)
                   .ConfigureAwait(false);
    await subscriber.EnsureSubscriptionIsCreatedAsync(options,
                                                      partition,
                                                      cancellationToken)
                    .ConfigureAwait(false);
  }

  /// <summary>
  ///   Try to create the topic for the <paramref name="partition" /> if it does not already exist.
  /// </summary>
  /// <param name="publisher">The publisher client</param>
  /// <param name="options">PubSub options to configure the topic</param>
  /// <param name="partition">Partition the topic refers to</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  private static async ValueTask EnsureTopicIsCreatedAsync(this PublisherServiceApiClient publisher,
                                                           PubSub                         options,
                                                           string                         partition,
                                                           CancellationToken              cancellationToken)
  {
    var topicName = options.GetTopicName(partition);
    try
    {
      await publisher.CreateTopicAsync(new Topic
                                       {
                                         MessageRetentionDuration = Duration.FromTimeSpan(options.MessageRetention),
                                         TopicName                = topicName,
                                         KmsKeyName               = options.KmsKeyName,
                                       },
                                       cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
    {
    }
  }

  /// <summary>
  ///   Try to create the subscription for the <paramref name="partition" /> if it does not already exist.
  /// </summary>
  /// <remarks>
  ///   The subscription is created without expiration: an expired subscription would silently drop the messages
  ///   published to its topic.
  /// </remarks>
  /// <param name="subscriber">The subscriber client</param>
  /// <param name="options">PubSub options to configure the subscription</param>
  /// <param name="partition">Partition the subscription refers to</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  private static async ValueTask EnsureSubscriptionIsCreatedAsync(this SubscriberServiceApiClient subscriber,
                                                                  PubSub                          options,
                                                                  string                          partition,
                                                                  CancellationToken               cancellationToken)
  {
    var topicName        = options.GetTopicName(partition);
    var subscriptionName = options.GetSubscriptionName(partition);

    var subscriptionRequest = new Subscription
                              {
                                SubscriptionName          = subscriptionName,
                                TopicAsTopicName          = topicName,
                                EnableExactlyOnceDelivery = options.ExactlyOnceDelivery,
                                EnableMessageOrdering     = options.MessageOrdering,
                                AckDeadlineSeconds        = options.AckDeadlinePeriod,
                                // An expiration policy without TTL means that the subscription never expires
                                ExpirationPolicy = new ExpirationPolicy(),
                              };
    try
    {
      await subscriber.CreateSubscriptionAsync(subscriptionRequest,
                                               cancellationToken)
                      .ConfigureAwait(false);
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
    {
    }
  }
}
