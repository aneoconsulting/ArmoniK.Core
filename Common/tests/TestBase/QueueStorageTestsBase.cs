// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class QueueStorageTestsBase
{
  [TearDown]
  public virtual void TearDown()
  {
    PushQueueStorage = null;
    PullQueueStorage = null;
    RunTests         = false;
  }

  [SetUp]
  public void Setup()
  {
    Options = CreateDefaultOptions();
    GetQueueStorageInstance()
      .Wait();
  }

  /* Interfaces to test */
  protected IPushQueueStorage? PushQueueStorage;
  protected IPullQueueStorage? PullQueueStorage;


  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of QueueAdaptorSettings to the corresponding interface implementation */
  protected virtual Task GetQueueStorageInstance()
    => Task.CompletedTask;

  protected Adapters.QueueCommon.Amqp? Options;

  protected static Adapters.QueueCommon.Amqp CreateDefaultOptions()
    => new()
       {
         Host              = "localhost",
         User              = "guest",
         Password          = "guest",
         Port              = 5672,
         CaPath            = "somePath",
         Scheme            = "someScheme",
         CredentialsPath   = "somePath",
         PartitionId       = "TestPartition",
         MaxPriority       = 10,
         MaxRetries        = 5,
         AllowHostMismatch = false,
         LinkCredit        = 1,
       };


  [Test]
  public async Task CreatePushQueueStorageShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PushQueueStorage!.Check(HealthCheckTag.Liveness)
                                                 .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PushQueueStorage.Check(HealthCheckTag.Readiness)
                                                .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PushQueueStorage.Check(HealthCheckTag.Startup)
                                                .ConfigureAwait(false)).Status);

      await PushQueueStorage.Init(CancellationToken.None)
                            .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await PushQueueStorage.Check(HealthCheckTag.Liveness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PushQueueStorage.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PushQueueStorage.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task CreatePullQueueStorageShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PullQueueStorage!.Check(HealthCheckTag.Liveness)
                                                 .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                                .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PullQueueStorage.Check(HealthCheckTag.Startup)
                                                .ConfigureAwait(false)).Status);

      await PullQueueStorage.Init(CancellationToken.None)
                            .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await PullQueueStorage.Check(HealthCheckTag.Liveness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PullQueueStorage.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task PushMessagesAsyncSucceeds()
  {
    if (RunTests)
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
                                               Options!.PartitionId,
                                               CancellationToken.None)
                            .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task PullMessagesAsyncSucceedsOnMultipleCalls()
  {
    if (RunTests)
    {
      await PushQueueStorage!.Init(CancellationToken.None)
                             .ConfigureAwait(false);
      await PullQueueStorage!.Init(CancellationToken.None)
                             .ConfigureAwait(false);

      const int priority = 1;
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
                                               Options!.PartitionId,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

      /* Pull 3 messages from the queue, their default status being pending means that
       they should be pushed again to the queue */
      var messages = PullQueueStorage.PullMessagesAsync(3,
                                                        CancellationToken.None);

      await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                        .ConfigureAwait(false))
      {
        Assert.AreEqual(QueueMessageStatus.Waiting,
                        qmh.Status);
        await qmh.DisposeAsync()
                 .ConfigureAwait(false);
      }

      /* Pull 2 messages from the queue and change their status to processing; this means that
       these two should be treated as dequeued  by the broker and the remaining three
       as Pending if the test passes */
      var messages2 = PullQueueStorage.PullMessagesAsync(2,
                                                         CancellationToken.None);

      await foreach (var qmh in messages2.WithCancellation(CancellationToken.None)
                                         .ConfigureAwait(false))
      {
        Assert.AreEqual(QueueMessageStatus.Waiting,
                        qmh.Status);
        qmh.Status = QueueMessageStatus.Processed;
        await qmh.DisposeAsync()
                 .ConfigureAwait(false);
      }
    }
  }

  [Test]
  public async Task PullMessagesAsyncSucceeds()
  {
    if (RunTests)
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
                                               Options!.PartitionId,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

      var messages = PullQueueStorage.PullMessagesAsync(5,
                                                        CancellationToken.None);

      await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                        .ConfigureAwait(false))
      {
        qmh!.Status = QueueMessageStatus.Processed;
        await qmh.DisposeAsync()
                 .ConfigureAwait(false);
      }
    }
  }
}
