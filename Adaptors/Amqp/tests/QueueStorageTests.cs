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
using System.Collections;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Amqp.Tests;

[TestFixture]
public class QueueStorageTests
{
  [SetUp]
  public void SetDefaultOptions()
    /* These options are only to feed the QueueStorage constructor
     * and they do not play any role in the how the connection is created,
     * the later is defined in the  SimpleAmqpClientHelper class */
    => Options = CreateDefaultOptions();

  public Options.Amqp? Options;

  private static Options.Amqp CreateDefaultOptions()
    => new()
       {
         Host              = "localhost",
         User              = "guest",
         Password          = "guest",
         Port              = 5672,
         CaPath            = "somePath",
         Scheme            = "someScheme",
         CredentialsPath   = "somePath",
         PartitionId       = "part1",
         MaxPriority       = 5,
         MaxRetries        = 5,
         AllowHostMismatch = false,
         LinkCredit        = 2,
       };

  [Test]
  public async Task CreatePushQueueStorageShouldSucceed()
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IPushSessionAmqp>();

    provider.Setup(sp => sp.Session)
            .Returns(helper.Session);

    var pushQueueStorage = new PushQueueStorage(Options!,
                                                provider.Object,
                                                NullLogger<PushQueueStorage>.Instance);
    await pushQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.IsTrue(await pushQueueStorage.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false));
    Assert.IsTrue(await pushQueueStorage.Check(HealthCheckTag.Readiness)
                                        .ConfigureAwait(false));
    Assert.IsTrue(await pushQueueStorage.Check(HealthCheckTag.Startup)
                                        .ConfigureAwait(false));
  }

  [Test]
  public async Task CreatePullQueueStorageShouldSucceed()
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IPullSessionAmqp>();

    provider.Setup(sp => sp.Session)
            .Returns(helper.Session);

    var pullQueueStorage = new PullQueueStorage(Options!,
                                                provider.Object,
                                                NullLogger<PullQueueStorage>.Instance);
    await pullQueueStorage.Init(CancellationToken.None)
                          .ConfigureAwait(false);

    Assert.IsTrue(await pullQueueStorage.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false));
    Assert.IsTrue(await pullQueueStorage.Check(HealthCheckTag.Readiness)
                                        .ConfigureAwait(false));
    Assert.IsTrue(await pullQueueStorage.Check(HealthCheckTag.Startup)
                                        .ConfigureAwait(false));
  }

  public static IEnumerable TestCasesBadOptions
  {
    get
    {
      var badHostOpt = CreateDefaultOptions();
      badHostOpt.Host = "";
      var badHost = new TestCaseData(badHostOpt);
      badHost.SetArgDisplayNames("InvalidHost");
      yield return badHost;

      var badUserOpt = CreateDefaultOptions();
      badUserOpt.User = "";
      var badUser = new TestCaseData(badUserOpt);
      badUser.SetArgDisplayNames("InvalidUser");
      yield return badUser;

      var badPswdOpt = CreateDefaultOptions();
      badPswdOpt.Password = "";
      var badPswd = new TestCaseData(badPswdOpt);
      badPswd.SetArgDisplayNames("InvalidPassword");
      yield return badPswd;

      var badPartitionOpt = CreateDefaultOptions();
      badPartitionOpt.PartitionId = "";
      var badPartition = new TestCaseData(badPartitionOpt);
      badPartition.SetArgDisplayNames("InvalidPartition");
      yield return badPartition;

      var badPortOpt = CreateDefaultOptions();
      badPortOpt.Port = 0;
      var badPort = new TestCaseData(badPortOpt);
      badPort.SetArgDisplayNames("InvalidPort");
      yield return badPort;

      var badPriorityOpt = CreateDefaultOptions();
      badPriorityOpt.MaxPriority = 0;
      var badPriority = new TestCaseData(badPriorityOpt);
      badPriority.SetArgDisplayNames("InvalidMaxPriority");
      yield return badPriority;

      var badMaxRetryOpt = CreateDefaultOptions();
      badMaxRetryOpt.MaxRetries = 0;
      var badMaxRetry = new TestCaseData(badMaxRetryOpt);
      badMaxRetry.SetArgDisplayNames("InvalidMaxRetry");
      yield return badMaxRetry;

      var badLinkCreditOpt = CreateDefaultOptions();
      badLinkCreditOpt.LinkCredit = 0;
      var badLinkCredit = new TestCaseData(badLinkCreditOpt);
      badLinkCredit.SetArgDisplayNames("InvalidLinkCredit");
      yield return badLinkCredit;
    }
  }

  [TestCaseSource(nameof(TestCasesBadOptions))]
  public async Task CreateQueueStorageShouldThrowIfBadOptionsGiven(Options.Amqp options)
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<ISessionAmqp>();

    provider.Setup(sp => sp.Session)
            .Returns(helper.Session);

    Assert.Throws<ArgumentOutOfRangeException>(() => new QueueStorage(options,
                                                                      provider.Object));
  }

  [Test]
  public async Task PushMessagesAsyncSucceeds()
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IPushSessionAmqp>();

    provider.Setup(sp => sp.Session)
            .Returns(helper.Session);

    var pushQueueStorage = new PushQueueStorage(Options!,
                                                provider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    var priority = 15; // InternalMaxPriority = 10
    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };
    await pushQueueStorage.PushMessagesAsync(testMessages,
                                             "part1",
                                             priority,
                                             CancellationToken.None)
                          .ConfigureAwait(false);
  }

  [Test]
  public async Task PullMessagesAsyncSucceeds()
  {
    await using var helper       = new SimpleAmqpClientHelper();
    var             pullProvider = new Mock<IPullSessionAmqp>();
    var             pushProvider = new Mock<IPushSessionAmqp>();

    pullProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);

    pushProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);

    var pushQueueStorage = new PushQueueStorage(Options!,
                                                pushProvider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    var pullQueueStorage = new PullQueueStorage(Options!,
                                                pullProvider.Object,
                                                NullLogger<PullQueueStorage>.Instance);

    var priority = 1;
    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };
    await pushQueueStorage.PushMessagesAsync(testMessages,
                                             Options!.PartitionId,
                                             priority,
                                             CancellationToken.None)
                          .ConfigureAwait(false);


    var messages = pullQueueStorage.PullMessagesAsync(5,
                                                      CancellationToken.None);

    await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                      .ConfigureAwait(false))
    {
      Assert.IsTrue(qmh.Status == QueueMessageStatus.Waiting);
      qmh.Status = QueueMessageStatus.Processed;
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task PullMessagesAsyncFromMultiplePartitionsSucceeds()
  {
    await using var helper       = new SimpleAmqpClientHelper();
    var             pullProvider = new Mock<IPullSessionAmqp>();
    var             pushProvider = new Mock<IPushSessionAmqp>();

    pullProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);

    pushProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);
    var pushQueueStorage = new PushQueueStorage(Options!,
                                                pushProvider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    var priority = 1;
    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };

    for (var i = 0; i < 3; i++)
    {
      Options!.PartitionId = $"part{i}";
      var pullQueueStorage = new PullQueueStorage(Options!,
                                                  pullProvider.Object,
                                                  NullLogger<PullQueueStorage>.Instance);

      await pushQueueStorage.PushMessagesAsync(testMessages,
                                               $"part{i}",
                                               priority,
                                               CancellationToken.None)
                            .ConfigureAwait(false);


      var messages = pullQueueStorage.PullMessagesAsync(5,
                                                        CancellationToken.None);

      await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                        .ConfigureAwait(false))
      {
        Assert.IsTrue(qmh.Status == QueueMessageStatus.Waiting);
        qmh.Status = QueueMessageStatus.Processed;
        await qmh.DisposeAsync()
                 .ConfigureAwait(false);
      }
    }
  }

  [Test]
  public async Task PullMessagesAsyncSucceedsOnMultipleCalls()
  {
    await using var helper       = new SimpleAmqpClientHelper();
    var             pullProvider = new Mock<IPullSessionAmqp>();
    var             pushProvider = new Mock<IPushSessionAmqp>();

    pullProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);

    pushProvider.Setup(sp => sp.Session)
                .Returns(helper.Session);

    var pushQueueStorage = new PushQueueStorage(Options!,
                                                pushProvider.Object,
                                                NullLogger<PushQueueStorage>.Instance);

    var pullQueueStorage = new PullQueueStorage(Options!,
                                                pullProvider.Object,
                                                NullLogger<PullQueueStorage>.Instance);

    var priority = 1;
    var testMessages = new[]
                       {
                         "msg1",
                         "msg2",
                         "msg3",
                         "msg4",
                         "msg5",
                       };
    /* Push 5 messages to the queue to test the pull */
    await pushQueueStorage.PushMessagesAsync(testMessages,
                                             Options!.PartitionId,
                                             priority,
                                             CancellationToken.None)
                          .ConfigureAwait(false);

    /* Pull 3 messages from the queue, their default status being pending means that
     they should be pushed again to the queue */
    var messages = pullQueueStorage.PullMessagesAsync(3,
                                                      CancellationToken.None);

    await foreach (var qmh in messages.WithCancellation(CancellationToken.None)
                                      .ConfigureAwait(false))
    {
      Assert.IsTrue(qmh.Status == QueueMessageStatus.Waiting);
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }

    /* Pull 2 messages from the queue and change their status to processing; this means that
     these two should be treated as dequeued  by the broker and the remaining three
     as Pending if the test passes */
    var messages2 = pullQueueStorage.PullMessagesAsync(2,
                                                       CancellationToken.None);

    await foreach (var qmh in messages2.WithCancellation(CancellationToken.None)
                                       .ConfigureAwait(false))
    {
      Assert.IsTrue(qmh.Status == QueueMessageStatus.Waiting);
      qmh.Status = QueueMessageStatus.Processed;
      await qmh.DisposeAsync()
               .ConfigureAwait(false);
    }
  }
}
