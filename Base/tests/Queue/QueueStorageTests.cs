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
  protected Adapters.QueueCommon.Amqp? Options;
  protected IPullQueueStorage?         PullQueueStorage;
  protected IPushQueueStorage?         PushQueueStorage;

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
        adapterAbsolutePath = "../../../../../../Adaptors/Amqp/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.Amqp.dll";
        break;

      case "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder":
        adapterAbsolutePath = "../../../../../../Adaptors/RabbitMQ/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.RabbitMQ.dll";
        break;

      default:
        throw new InvalidOperationException($"Unknown ClassName: {className}");
    }

    if (adapterAbsolutePath != null)
    {
      Environment.SetEnvironmentVariable("Components__QueueAdaptorSettings__AdapterAbsolutePath",
                                         adapterAbsolutePath);
    }

    Environment.SetEnvironmentVariable("Amqp__PartitionId",
                                       "TestPartition");
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
    Options          = ServiceProvider.GetRequiredService<Adapters.QueueCommon.Amqp>();
  }

  #region Tests

  [Test]
  public void GetQueueStorageInstanceShouldLoad()
  {
    Assert.NotNull(PullQueueStorage);
    Assert.NotNull(PushQueueStorage);
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

    var testTaskOptions = new TaskOptions(new Dictionary<string, string>
                                          {
                                            {
                                              "testOptionKey", "testOptionValue"
                                            },
                                          },
                                          TimeSpan.FromHours(2),
                                          2,
                                          1,
                                          "testPartition",
                                          "testApplication",
                                          "testVersion",
                                          "testNamespace",
                                          "testService",
                                          "testEngineType");
    var testMessages = new List<MessageData>
                       {
                         new("msg1",
                             "session1",
                             testTaskOptions),
                         new("msg2",
                             "session1",
                             testTaskOptions),
                         new("msg3",
                             "session1",
                             testTaskOptions),
                         new("msg4",
                             "session1",
                             testTaskOptions),
                         new("msg5",
                             "session1",
                             testTaskOptions),
                       };

    await PushQueueStorage.PushMessagesAsync(testMessages,
                                             "testPartition",
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

    var testTaskOptions = new TaskOptions(new Dictionary<string, string>
                                          {
                                            {
                                              "testOptionKey", "testOptionValue"
                                            },
                                          },
                                          TimeSpan.FromHours(2),
                                          2,
                                          1,
                                          "testPartition",
                                          "testApplication",
                                          "testVersion",
                                          "testNamespace",
                                          "testService",
                                          "testEngineType");
    var testMessages = new List<MessageData>
                       {
                         new("msg1",
                             "session1",
                             testTaskOptions),
                         new("msg2",
                             "session1",
                             testTaskOptions),
                         new("msg3",
                             "session1",
                             testTaskOptions),
                         new("msg4",
                             "session1",
                             testTaskOptions),
                         new("msg5",
                             "session1",
                             testTaskOptions),
                       };
    /* Push 5 messages to the queue to test the pull */
    await PushQueueStorage.PushMessagesAsync(testMessages,
                                             "testPartition",
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    /* Pull 3 messages from the queue, their default status being pending means that
     they should be pushed again to the queue */
    var messages = PullQueueStorage.PullMessagesAsync("testPartition", 3,
                                                      CancellationToken.None);

    await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                      .ConfigureAwait(false))
    {
      Assert.That(qmh.Status,
                  Is.EqualTo(QueueMessageStatus.Waiting));
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }

    /* Pull 2 messages from the queue and change their status to processing; this means that
     these two should be treated as dequeued  by the broker and the remaining three
     as Pending if the test passes */
    var messages2 = PullQueueStorage.PullMessagesAsync("testPartition", 2,
                                                       CancellationToken.None);

    await foreach (var qmh in messages2.WithCancellation(CancellationToken.None)
                                       .ConfigureAwait(false))
    {
      Assert.That(qmh.Status,
                  Is.EqualTo(QueueMessageStatus.Waiting));
      qmh.Status = QueueMessageStatus.Processed;
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task PullMessagesAsyncSucceeds()
  {
    await PullQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    await PushQueueStorage!.Init(CancellationToken.None)
                           .ConfigureAwait(false);

    var testTaskOptions = new TaskOptions(new Dictionary<string, string>
                                          {
                                            {
                                              "testOptionKey", "testOptionValue"
                                            },
                                          },
                                          TimeSpan.FromHours(2),
                                          2,
                                          1,
                                          "testPartition",
                                          "testApplication",
                                          "testVersion",
                                          "testNamespace",
                                          "testService",
                                          "testEngineType");
    var testMessages = new List<MessageData>
                       {
                         new("msg1",
                             "session1",
                             testTaskOptions),
                         new("msg2",
                             "session1",
                             testTaskOptions),
                         new("msg3",
                             "session1",
                             testTaskOptions),
                         new("msg4",
                             "session1",
                             testTaskOptions),
                         new("msg5",
                             "session1",
                             testTaskOptions),
                       };
    await PushQueueStorage.PushMessagesAsync(testMessages,
                                             "testPartition",
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    var messages = PullQueueStorage.PullMessagesAsync("testPartition", 5,
                                                      CancellationToken.None);

    await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                      .ConfigureAwait(false))
    {
      qmh!.Status = QueueMessageStatus.Processed;
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }
  }

  #endregion
}

