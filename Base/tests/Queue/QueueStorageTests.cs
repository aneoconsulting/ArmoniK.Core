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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.DynamicLoading;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace ArmoniK.Core.Tests.Queue;

public class QueueStorageTests
{
  /// <summary>
  ///   Partition shared by the tests. Some queues (RabbitMQ) only accept the partitions declared by the deployment,
  ///   which are TestPartition0 to TestPartition{n-1} (2 partitions by default).
  /// </summary>
  private const string SharedPartition = "TestPartition0";

  /// <summary>
  ///   Partition used only by <see cref="PushBeforeAnyPullOnNewPartitionShouldDeliverMessages" />
  /// </summary>
  private const string FreshPartition = "TestPartition1";

  /// <summary>
  ///   Maximum time spent pulling the expected messages
  /// </summary>
  private static readonly TimeSpan PullTimeout = TimeSpan.FromSeconds(30);

  protected IPullQueueStorage? PullQueueStorage;
  protected IPushQueueStorage? PushQueueStorage;

  protected IServiceProvider ServiceProvider;

  public static IServiceProvider BuildServiceProvider()
  {
    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var loggerFactory = LoggerFactory.Create(builder =>
                                             {
                                               builder.AddConsole();
                                               builder.AddDebug();
                                             });

    AppDomain.CurrentDomain.AssemblyResolve += new CollocatedAssemblyResolver(loggerFactory.CreateLogger("root")).AssemblyResolve;
    var className = Environment.GetEnvironmentVariable("Components__QueueAdaptorSettings__ClassName");

    var adapterAbsolutePath = string.Empty;

    // Here we modify the AdaptorAbsolutePath based on the ClassName 
    switch (className)
    {
      case "ArmoniK.Core.Adapters.Amqp.QueueBuilder":
        adapterAbsolutePath = "../../../../../../Adaptors/Amqp/src/bin/Debug/net10.0/ArmoniK.Core.Adapters.Amqp.dll";
        break;

      case "ArmoniK.Core.Adapters.SQS.QueueBuilder":
        adapterAbsolutePath = "../../../../../../Adaptors/SQS/src/bin/Debug/net10.0/ArmoniK.Core.Adapters.SQS.dll";
        break;

      case "ArmoniK.Core.Adapters.PubSub.QueueBuilder":
        adapterAbsolutePath = "../../../../../../Adaptors/PubSub/src/bin/Debug/net10.0/ArmoniK.Core.Adapters.PubSub.dll";
        break;
      case "ArmoniK.Core.Adapters.Nats.QueueBuilder":
        adapterAbsolutePath = "../../../../../../Adaptors/Nats/src/bin/Debug/net10.0/ArmoniK.Core.Adapters.Nats.dll";
        break;

      default:
        throw new InvalidOperationException($"Unknown ClassName: {className}");
    }

    if (adapterAbsolutePath != null)
    {
      Environment.SetEnvironmentVariable("Components__QueueAdaptorSettings__AdapterAbsolutePath",
                                         adapterAbsolutePath);
    }

    var configuration = new ConfigurationManager();


    configuration.AddEnvironmentVariables();

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddLogging(loggingBuilder => loggingBuilder.AddConsole()
                                                                 .AddDebug()
                                                                 .SetMinimumLevel(LogLevel.Debug));
    serviceCollection.AddAdapter(configuration,
                                 nameof(Components.QueueAdaptorSettings),
                                 loggerFactory.CreateLogger("root"));
    return serviceCollection.BuildServiceProvider();
  }

  [SetUp]
  public void Setup()
  {
    ServiceProvider  = BuildServiceProvider();
    PullQueueStorage = ServiceProvider.GetRequiredService<IPullQueueStorage>();
    PushQueueStorage = ServiceProvider.GetRequiredService<IPushQueueStorage>();
  }

  [Test]
  public void GetQueueStorageInstanceShouldLoad()
  {
    Assert.That(PullQueueStorage,
                Is.Not.Null);
    Assert.That(PushQueueStorage,
                Is.Not.Null);
  }

  [Test]
  public async Task CreatePushQueueStorageShouldSucceed()
  {
    Assert.That((await PushQueueStorage!.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
    Assert.That((await PushQueueStorage.Check(HealthCheckTag.Readiness)
                                       .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
    Assert.That((await PushQueueStorage.Check(HealthCheckTag.Startup)
                                       .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));

    await PushQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.That((await PushQueueStorage.Check(HealthCheckTag.Liveness)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await PushQueueStorage.Check(HealthCheckTag.Readiness)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await PushQueueStorage.Check(HealthCheckTag.Startup)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
  }

  [Test]
  public async Task CreatePullQueueStorageShouldSucceed()
  {
    Assert.That((await PullQueueStorage!.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
    Assert.That((await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                       .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));
    Assert.That((await PullQueueStorage.Check(HealthCheckTag.Startup)
                                       .ConfigureAwait(false)).Status,
                Is.Not.EqualTo(HealthStatus.Healthy));

    await PullQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.That((await PullQueueStorage.Check(HealthCheckTag.Liveness)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await PullQueueStorage.Check(HealthCheckTag.Startup)
                                       .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
  }

  [Test]
  public async Task PushMessagesAsyncSucceeds()
  {
    await PushQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var partition = SharedPartition;

    await PushQueueStorage.PushMessagesAsync(CreateMessages(partition,
                                                            5),
                                             partition,
                                             CancellationToken.None)
                          .ConfigureAwait(false);
  }

  [Test]
  public async Task PullMessagesAsyncSucceedsOnMultipleCalls()
  {
    await PushQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);
    await PullQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var partition = SharedPartition;

    /* Push 5 messages to the queue to test the pull */
    await PushQueueStorage.PushMessagesAsync(CreateMessages(partition,
                                                            5),
                                             partition,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    /* Pull 3 messages from the queue, their default status being waiting means that
     they should be pushed again to the queue */
    var pulled = await PullAndDisposeAsync(partition,
                                           3,
                                           null)
                   .ConfigureAwait(false);

    Assert.That(pulled,
                Has.Count.EqualTo(3));

    /* Pull 2 messages from the queue and change their status to processed; this means that
     these two should be treated as dequeued by the broker and the remaining three
     as pending if the test passes */
    var pulled2 = await PullAndDisposeAsync(partition,
                                            2,
                                            QueueMessageStatus.Processed)
                    .ConfigureAwait(false);

    Assert.That(pulled2,
                Has.Count.EqualTo(2));
  }

  [Test]
  public async Task PullMessagesAsyncSucceeds()
  {
    await PullQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    await PushQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var partition = SharedPartition;
    var messages = CreateMessages(partition,
                                  5);

    await PushQueueStorage.PushMessagesAsync(messages,
                                             partition,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    var pulled = await PullAndDisposeAsync(partition,
                                           messages.Count,
                                           QueueMessageStatus.Processed)
                   .ConfigureAwait(false);

    // The shared partition may still hold messages left by the other tests: only the count is checked
    Assert.That(pulled,
                Has.Count.EqualTo(messages.Count));
  }

  /// <summary>
  ///   Messages pushed to a partition that no consumer has ever pulled from must not be lost:
  ///   this is the situation of a partition scaled to zero polling agent when tasks are submitted.
  /// </summary>
  /// <remarks>
  ///   The Pub/Sub emulator delivers the messages retained by the topic to subscriptions created after them,
  ///   unlike GCP, so this test cannot detect the issue for Pub/Sub on the emulator.
  ///   The Pub/Sub adapter tests check that the subscription is created when pushing.
  ///   This test relies on <see cref="FreshPartition" /> never being used by another test,
  ///   so that it has never been pulled from on a freshly deployed queue, as in the CI.
  /// </remarks>
  [Test]
  public async Task PushBeforeAnyPullOnNewPartitionShouldDeliverMessages()
  {
    await PushQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var partition = FreshPartition;
    var messages = CreateMessages(partition,
                                  5);

    await PushQueueStorage.PushMessagesAsync(messages,
                                             partition,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    // The consumer starts only after the messages have been pushed
    await PullQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var pulled = await PullAndDisposeAsync(partition,
                                           messages.Count,
                                           QueueMessageStatus.Processed)
                   .ConfigureAwait(false);

    Assert.That(pulled,
                Is.EquivalentTo(messages.Select(message => message.TaskId)));
  }

  private static List<MessageData> CreateMessages(string partition,
                                                  int    count)
  {
    var testTaskOptions = new TaskOptions(new Dictionary<string, string>
                                          {
                                            {
                                              "testOptionKey", "testOptionValue"
                                            },
                                          },
                                          TimeSpan.FromHours(2),
                                          2,
                                          1,
                                          partition,
                                          "testApplication",
                                          "testVersion",
                                          "testNamespace",
                                          "testService",
                                          "testEngineType");

    return Enumerable.Range(1,
                            count)
                     .Select(i => new MessageData($"msg{i}-{Guid.NewGuid()}",
                                                  "session1",
                                                  testTaskOptions))
                     .ToList();
  }

  /// <summary>
  ///   Pull messages from <paramref name="partition" /> until <paramref name="expected" /> messages have been received
  ///   or <see cref="PullTimeout" /> is reached. A single pull may return fewer messages than requested.
  /// </summary>
  /// <param name="partition">Partition to pull from</param>
  /// <param name="expected">Number of messages to receive</param>
  /// <param name="status">Status set on the messages before disposing them; <c>null</c> keeps the default status</param>
  /// <returns>The task ids of the received messages</returns>
  private async Task<List<string>> PullAndDisposeAsync(string              partition,
                                                       int                 expected,
                                                       QueueMessageStatus? status)
  {
    var       taskIds = new List<string>();
    using var cts     = new CancellationTokenSource(PullTimeout);

    while (taskIds.Count < expected && !cts.IsCancellationRequested)
    {
      try
      {
        await foreach (var qmh in PullQueueStorage!.PullMessagesAsync(partition,
                                                                      expected - taskIds.Count,
                                                                      cts.Token)
                                                   .ConfigureAwait(false))
        {
          Assert.That(qmh.Status,
                      Is.EqualTo(QueueMessageStatus.Waiting));
          taskIds.Add(qmh.TaskId);
          if (status is not null)
          {
            qmh.Status = status.Value;
          }

          await qmh.DisposeAsync()
                   .ConfigureAwait(false);
        }
      }
      catch (Exception e) when (cts.IsCancellationRequested && e is not AssertionException)
      {
        // Timeout reached while waiting for messages: the assertions of the caller report the missing ones
      }
    }

    return taskIds;
  }
}
