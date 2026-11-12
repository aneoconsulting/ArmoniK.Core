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

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Utils;

using Google.Api.Gax;
using Google.Cloud.PubSub.V1;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PubSub.Tests;

/// <summary>
///   Tests specific to the Pub/Sub adapter, run against the Pub/Sub emulator.
///   They check the lifecycle of the subscriptions, which is not covered by the generic queue tests.
/// </summary>
[TestFixture]
public class PubSubQueueTests
{
  [SetUp]
  public async Task SetUp()
  {
    if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST")))
    {
      Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST",
                                         "localhost:8085");
    }

    projectId_ = Environment.GetEnvironmentVariable("PubSub__ProjectId") is
                 {
                   Length: > 0,
                 } projectId
                   ? projectId
                   : "plugincore";

    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    "Components:QueueAdaptorSettings:ClassName", "ArmoniK.Core.Adapters.PubSub.QueueBuilder"
                                                  },
                                                  {
                                                    "Components:QueueAdaptorSettings:AdapterAbsolutePath", $"{SolutionRoot}{PubSubPath}"
                                                  },
                                                  {
                                                    "PubSub:ProjectId", projectId_
                                                  },
                                                  {
                                                    "PubSub:Prefix", Prefix
                                                  },
                                                };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddLogging();
    var logger = new LoggerInit(configuration);

    services.AddAdapter(configuration,
                        nameof(Components.QueueAdaptorSettings),
                        logger.GetLogger());

    provider_ = services.BuildServiceProvider();

    pushQueueStorage_ = provider_.GetRequiredService<IPushQueueStorage>();
    pullQueueStorage_ = provider_.GetRequiredService<IPullQueueStorage>();
    await pushQueueStorage_.Init(CancellationToken.None)
                           .ConfigureAwait(false);
    await pullQueueStorage_.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    // The adapter is loaded in its own context: the test uses its own client to inspect the emulator
    subscriber_ = await new SubscriberServiceApiClientBuilder
                    {
                      EmulatorDetection = EmulatorDetection.EmulatorOnly,
                    }.BuildAsync()
                     .ConfigureAwait(false);
  }

  [TearDown]
  public async Task TearDown()
  {
    if (provider_ is not null)
    {
      await provider_.DisposeAsync()
                     .ConfigureAwait(false);
    }
  }

  private const string Prefix = "akpubsubtests";

  /// <summary>
  ///   Messages become available again within a minute after a seek on GCP: leave some margin
  /// </summary>
  private static readonly TimeSpan PullTimeout = TimeSpan.FromSeconds(90);

  private static readonly string SolutionRoot =
    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(typeof(PubSubQueueTests)
                                                                                                                                                .Assembly
                                                                                                                                                .Location))))) ??
                                  string.Empty));

  private static readonly string PubSubPath =
    $"{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net10.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.PubSub.dll";

  private string                      projectId_ = string.Empty;
  private ServiceProvider?            provider_;
  private IPullQueueStorage?          pullQueueStorage_;
  private IPushQueueStorage?          pushQueueStorage_;
  private SubscriberServiceApiClient? subscriber_;

  private static string NewPartition()
    => $"TestPartition{Guid.NewGuid():N}";

  private SubscriptionName GetSubscriptionName(string partition)
    => SubscriptionName.FromProjectSubscription(projectId_,
                                                $"{Prefix}-{partition}-ak-sub");

  private static List<MessageData> CreateMessages(string partition,
                                                  int    count)
  {
    var options = new TaskOptions(new Dictionary<string, string>(),
                                  TimeSpan.FromHours(1),
                                  1,
                                  1,
                                  partition,
                                  "testApplication",
                                  "testVersion",
                                  "testNamespace",
                                  "testService",
                                  "testEngineType");

    return Enumerable.Range(0,
                            count)
                     .Select(_ => new MessageData(Guid.NewGuid()
                                                      .ToString(),
                                                  "session",
                                                  options))
                     .ToList();
  }

  /// <summary>
  ///   Pull and acknowledge messages from <paramref name="partition" /> until all <paramref name="expected" />
  ///   task ids have been received or <see cref="PullTimeout" /> is reached.
  /// </summary>
  private async Task<HashSet<string>> PullUntilAsync(string              partition,
                                                     ICollection<string> expected)
  {
    var       received = new HashSet<string>();
    using var cts      = new CancellationTokenSource(PullTimeout);

    while (!expected.All(received.Contains) && !cts.IsCancellationRequested)
    {
      try
      {
        await foreach (var qmh in pullQueueStorage_!.PullMessagesAsync(partition,
                                                                       10,
                                                                       cts.Token)
                                                    .ConfigureAwait(false))
        {
          received.Add(qmh.TaskId);
          qmh.Status = QueueMessageStatus.Processed;
          await qmh.DisposeAsync()
                   .ConfigureAwait(false);
        }
      }
      catch (Exception) when (cts.IsCancellationRequested)
      {
        // Timeout reached: the assertions of the caller report the missing messages
      }
    }

    return received;
  }

  [Test]
  public async Task SubscriptionCreatedByPushShouldNeverExpire()
  {
    var partition = NewPartition();

    await pushQueueStorage_!.PushMessagesAsync(CreateMessages(partition,
                                                              1),
                                               partition)
                            .ConfigureAwait(false);

    var subscription = await subscriber_!.GetSubscriptionAsync(GetSubscriptionName(partition))
                                         .ConfigureAwait(false);

    Assert.That(subscription.ExpirationPolicy,
                Is.Not.Null);
    Assert.That(subscription.ExpirationPolicy.Ttl,
                Is.Null);
  }

  [Test]
  public async Task SubscriptionCreatedByPullShouldNeverExpire()
  {
    var partition = NewPartition();

    // The partition is empty: the pull may wait for messages until the token is cancelled
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    try
    {
      await foreach (var qmh in pullQueueStorage_!.PullMessagesAsync(partition,
                                                                     1,
                                                                     cts.Token)
                                                  .ConfigureAwait(false))
      {
        await qmh.DisposeAsync()
                 .ConfigureAwait(false);
      }
    }
    catch (Exception) when (cts.IsCancellationRequested)
    {
    }

    var subscription = await subscriber_!.GetSubscriptionAsync(GetSubscriptionName(partition))
                                         .ConfigureAwait(false);

    Assert.That(subscription.ExpirationPolicy,
                Is.Not.Null);
    Assert.That(subscription.ExpirationPolicy.Ttl,
                Is.Null);
  }

  /// <summary>
  ///   Messages published while the subscription does not exist are retained by the topic only.
  ///   The polling agent must recover them when it creates the subscription again.
  /// </summary>
  /// <remarks>
  ///   Limitation: unlike GCP, the emulator delivers the messages retained by the topic to a new subscription
  ///   even without seek. On the emulator, this test only checks that recreating the subscription and seeking
  ///   it does not fail and that the messages are received; it cannot prove that the seek is required.
  /// </remarks>
  [Test]
  public async Task MessagesPublishedWithoutSubscriptionShouldBeRecoveredByPull()
  {
    var partition = NewPartition();

    // Creates the topic and the subscription
    await pushQueueStorage_!.PushMessagesAsync(CreateMessages(partition,
                                                              1),
                                               partition)
                            .ConfigureAwait(false);

    // Simulates an expired or deleted subscription
    await subscriber_!.DeleteSubscriptionAsync(GetSubscriptionName(partition))
                      .ConfigureAwait(false);

    // The topic exists, so the publish succeeds even though no subscription can receive these messages
    var orphans = CreateMessages(partition,
                                 5);
    await pushQueueStorage_.PushMessagesAsync(orphans,
                                              partition)
                           .ConfigureAwait(false);

    var expected = orphans.Select(message => message.TaskId)
                          .ToList();
    var received = await PullUntilAsync(partition,
                                        expected)
                     .ConfigureAwait(false);

    Assert.That(received,
                Is.SupersetOf(expected));
  }
}
