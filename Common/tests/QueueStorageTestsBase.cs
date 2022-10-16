// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

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
    RunTests = true;
  }

  /* Interfaces to test */
  protected IPushQueueStorage? PushQueueStorage;
  protected IPullQueueStorage? PullQueueStorage;


  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

/* Function be override so it returns the suitable instance
 * of QueueStorage to the corresponding interface implementation */
  protected virtual Task GetQueueStorageInstance()
    => Task.CompletedTask;

  protected Injection.Options.Amqp? Options;

  protected static Injection.Options.Amqp CreateDefaultOptions()
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
      Assert.AreNotEqual(HealthCheckResult.Healthy(),
                         await PullQueueStorage!.Check(HealthCheckTag.Liveness)
                                                .ConfigureAwait(false));
      Assert.AreNotEqual(HealthCheckResult.Healthy(),
                         await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                               .ConfigureAwait(false));
      Assert.AreNotEqual(HealthCheckResult.Healthy(),
                         await PullQueueStorage.Check(HealthCheckTag.Startup)
                                               .ConfigureAwait(false));

      await PullQueueStorage.Init(CancellationToken.None)
                            .ConfigureAwait(false);

      Assert.AreEqual(HealthCheckResult.Healthy(),
                      await PullQueueStorage.Check(HealthCheckTag.Liveness)
                                            .ConfigureAwait(false));
      Assert.AreEqual(HealthCheckResult.Healthy(),
                      await PullQueueStorage.Check(HealthCheckTag.Readiness)
                                            .ConfigureAwait(false));
      Assert.AreEqual(HealthCheckResult.Healthy(),
                      await PullQueueStorage.Check(HealthCheckTag.Startup)
                                            .ConfigureAwait(false));
    }
  }

  [Test]
  public async Task PushMessagesAsyncSucceeds()
  {
    if (RunTests)
    {
      await PushQueueStorage!.Init(CancellationToken.None)
                             .ConfigureAwait(false);

      var testMessages = new[]
                         {
                           "msg1",
                           "msg2",
                           "msg3",
                           "msg4",
                           "msg5",
                         };
      await PushQueueStorage.PushMessagesAsync(testMessages,
                                               Options!.PartitionId,
                                               1,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

      Assert.Pass();
    }
  }

  [Test]
  public async Task PullMessagesAsyncFromMultiplePartitionsSucceeds()
  {
    if (RunTests)
    {
      const int priority = 1;
      var testMessages = new[]
                         {
                           "msg1",
                           "msg2",
                           "msg3",
                           "msg4",
                           "msg5",
                         };

      await PushQueueStorage!.Init(CancellationToken.None)
                             .ConfigureAwait(false);

      await PullQueueStorage!.Init(CancellationToken.None)
                             .ConfigureAwait(false);

      for (var i = 0; i < 3; i++)
      {
        Options!.PartitionId = $"part{i}";

        await PushQueueStorage.PushMessagesAsync(testMessages,
                                                 $"part{i}",
                                                 priority,
                                                 CancellationToken.None)
                              .ConfigureAwait(false);

        var messages = PullQueueStorage.PullMessagesAsync(5,
                                                          CancellationToken.None);

        await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                          .ConfigureAwait(false))
        {
          Assert.IsTrue(qmh!.Status == QueueMessageStatus.Waiting);
          qmh.Status = QueueMessageStatus.Processed;
          await qmh.DisposeAsync()
                   .ConfigureAwait(false);
        }
      }
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
      var testMessages = new[]
                         {
                           "msg1",
                           "msg2",
                           "msg3",
                           "msg4",
                           "msg5",
                         };
      /* Push 5 messages to the queue to test the pull */
      await PushQueueStorage.PushMessagesAsync(testMessages,
                                               Options!.PartitionId,
                                               priority,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

      /* Pull 3 messages from the queue, their default status being pending means that
       they should be pushed again to the queue */
      var messages = PullQueueStorage.PullMessagesAsync(3,
                                                        CancellationToken.None);

      await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                        .ConfigureAwait(false))
      {
        Assert.IsTrue(qmh!.Status == QueueMessageStatus.Waiting);
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
        Assert.IsTrue(qmh!.Status == QueueMessageStatus.Waiting);
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

      var testMessages = new[]
                         {
                           "msg1",
                           "msg2",
                           "msg3",
                           "msg4",
                           "msg5",
                         };
      await PushQueueStorage.PushMessagesAsync(testMessages,
                                               Options!.PartitionId,
                                               1,
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
