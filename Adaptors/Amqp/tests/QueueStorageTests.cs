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

using Amqp;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Adapters.Amqp.Tests;

[TestFixture]
public class QueueStorageTests
{
  private Options.Amqp options_;
  public Options.Amqp Options_ { get => options_; set => options_ = value; }

  private static Options.Amqp CreateDefaultOptions()
  {
    return new Options.Amqp()
    {
      Host = "localhost",
      User = "guest",
      Password = "guest",
      Port = 5672,
      CaPath = "somePath",
      Scheme = "someScheme",
      CredentialsPath = "somePath",
      MaxPriority = 5,
      AllowHostMismatch = false
    };
  }

  [SetUp]
  public void SetDefaultOptions()
  {
    /* These options are only to feed the QueueStorage constructor
     * and they do not play any role in the how the connection is created,
     * the later is defined in the  SimpleAmqpClientHelper class */
    Options_ = CreateDefaultOptions();
  }

  [Test]
  public async Task SimpleBrokerTest()
  {
    var timeout = TimeSpan.FromMilliseconds(100);
    await using var helper = new SimpleAmqpClientHelper();

    var sender_ = new SenderLink(helper.Session, "sender-link", "q1");
    var sendMsg = new Message("Hello AMQP!");
    await sender_.SendAsync(sendMsg)
      .ConfigureAwait(false);
    Console.WriteLine("Sent " + sendMsg.Body.ToString());
    await sender_.CloseAsync()
      .ConfigureAwait(false);

    var receiver_ = new ReceiverLink(helper.Session, "receiver-link", "q1");
    Console.WriteLine("Receiver connected to broker.");
    var receiveMsg = await receiver_.ReceiveAsync(timeout)
      .ConfigureAwait(false);
    Console.WriteLine("Received " + receiveMsg.Body.ToString());
    receiver_.Accept(receiveMsg);
    await receiver_.CloseAsync()
      .ConfigureAwait(false);

    Assert.AreEqual(receiveMsg.Body, sendMsg.Body);
  }

  [Test]
  public async Task CreateQueueStorageShouldSucceed()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    var queueStorage = new QueueStorage(Options_, provider.Object,
                                          NullLogger<QueueStorage>.Instance);
    await queueStorage.Init(CancellationToken.None)
      .ConfigureAwait(false);

    Assert.IsTrue(await queueStorage.Check(HealthCheckTag.Liveness)
      .ConfigureAwait(false));
    Assert.IsTrue(await queueStorage.Check(HealthCheckTag.Readiness)
      .ConfigureAwait(false));
    Assert.IsTrue(await queueStorage.Check(HealthCheckTag.Startup)
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
    }
  }


  [TestCaseSource(nameof(TestCasesBadOptions))]
  public async Task CreateQueueStorageShouldThrowIfBadOptionsGiven(Options.Amqp options)
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    Assert.Throws<ArgumentOutOfRangeException>(() =>
     new QueueStorage(options, provider.Object,
                                        NullLogger<QueueStorage>.Instance)
    );
  }

  [Test]
  public async Task EnqueueMessagesAsyncThrowsOnTooBigPriority()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    var queueStorage = new QueueStorage(Options_, provider.Object,
                                          NullLogger<QueueStorage>.Instance);
    await queueStorage.Init(CancellationToken.None)
      .ConfigureAwait(false);

    var priority = 11; // InternalMaxPriority = 10
    var testMessages = new[] { "msg1", "msg2", "msg3" };
    Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await
     queueStorage.EnqueueMessagesAsync(testMessages, priority, CancellationToken.None)
      .ConfigureAwait(false));
  }

  [Test]
  public async Task EnqueueMessagesAsyncSucceds()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    var queueStorage = new QueueStorage(Options_, provider.Object,
                                          NullLogger<QueueStorage>.Instance);
    await queueStorage.Init(CancellationToken.None)
      .ConfigureAwait(false);

    var priority = 3;
    var testMessages = new[] { "msg1", "msg2", "msg3" };
    await queueStorage.EnqueueMessagesAsync(testMessages, priority, CancellationToken.None)
      .ConfigureAwait(false);
  }

  [Test]
  public async Task PullAsyncAsyncSucceds()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    var queueStorage = new QueueStorage(Options_, provider.Object,
                                          NullLogger<QueueStorage>.Instance);
    await queueStorage.Init(CancellationToken.None)
      .ConfigureAwait(false);

    var priority = 1;
    var testMessages = new[] { "msg1", "msg2", "msg3" };
    await queueStorage.EnqueueMessagesAsync(testMessages, priority, CancellationToken.None)
      .ConfigureAwait(false);

    await foreach (var qm in queueStorage.PullAsync(3, CancellationToken.None)
      .ConfigureAwait(false))
    {
      Assert.IsTrue(qm.Status == Common.Storage.QueueMessageStatus.Waiting);
    }
  }
}
