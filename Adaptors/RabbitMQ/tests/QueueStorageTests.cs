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

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using RabbitMQ.Client;

namespace ArmoniK.Core.Adapters.RabbitMQ.Tests;

[TestFixture]
public class QueueStorageTests
{
  [SetUp]
  public void SetDefaultOptions()
  {
    options_ = CreateDefaultOptions();

    var factory = new ConnectionFactory
                  {
                    HostName = options_!.Host,
                    Port     = options_!.Port,
                    UserName = options_!.User,
                    Password = options_!.Password,
                  };

    connection_ = factory.CreateConnection();
    channel_    = connection_.CreateModel();
  }

  [TearDown]
  public void TearDown()
  {
    channel_!.Close();
    connection_!.Close();
  }

  private IModel?      channel_;
  private IConnection? connection_;

  private Common.Injection.Options.Amqp? options_;

  private static Common.Injection.Options.Amqp CreateDefaultOptions()
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
    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    var pushQueueStorage = new PushQueueStorage(options_!,
                                                provider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await pushQueueStorage.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await pushQueueStorage.Check(HealthCheckTag.Readiness)
                                              .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await pushQueueStorage.Check(HealthCheckTag.Startup)
                                              .ConfigureAwait(false)).Status);

    await pushQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.AreEqual(HealthStatus.Healthy,
                    (await pushQueueStorage.Check(HealthCheckTag.Liveness)
                                           .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await pushQueueStorage.Check(HealthCheckTag.Readiness)
                                           .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await pushQueueStorage.Check(HealthCheckTag.Startup)
                                           .ConfigureAwait(false)).Status);
  }

  [Test]
  public async Task CreatePullQueueStorageShouldSucceed()
  {
    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    var pullQueueStorage = new PullQueueStorage(options_!,
                                                provider.Object,
                                                NullLogger<PullQueueStorage>.Instance);
    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await pullQueueStorage.Check(HealthCheckTag.Liveness)
                                             .ConfigureAwait(false));
    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await pullQueueStorage.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false));
    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await pullQueueStorage.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false));

    await pullQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await pullQueueStorage.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false));
    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await pullQueueStorage.Check(HealthCheckTag.Readiness)
                                          .ConfigureAwait(false));
    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await pullQueueStorage.Check(HealthCheckTag.Startup)
                                          .ConfigureAwait(false));
  }

  [Test]
  public async Task CreatePullQueueStorageShouldFail()
  {
    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    var badOpt = CreateDefaultOptions();
    badOpt.PartitionId = "";
    Assert.Throws<ArgumentOutOfRangeException>(() => new PullQueueStorage(badOpt,
                                                                          provider.Object,
                                                                          NullLogger<PullQueueStorage>.Instance));
  }

  [Test]
  public async Task PushMessagesAsyncSucceeds()
  {
    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    var pushQueueStorage = new PushQueueStorage(options_!,
                                                provider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    await pushQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };
    await pushQueueStorage.PushMessagesAsync(testMessages,
                                             options_!.PartitionId,
                                             1,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.Pass();
  }

  [Test]
  public async Task PullMessagesAsyncSucceeds()
  {
    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    var pullQueueStorage = new PullQueueStorage(options_!,
                                                provider.Object,
                                                NullLogger<PullQueueStorage>.Instance);

    await pullQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    var pushQueueStorage = new PushQueueStorage(options_!,
                                                provider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    await pushQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };

    await pushQueueStorage.PushMessagesAsync(testMessages,
                                             options_!.PartitionId,
                                             1,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    var messages = await pullQueueStorage.PullMessages(1,
                                                       CancellationToken.None)
                                         .ConfigureAwait(false);

    foreach (var qmh in messages)
    {
      Assert.IsTrue(qmh.Status == QueueMessageStatus.Waiting);
      qmh.Status = QueueMessageStatus.Processed;
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }
  }
}
