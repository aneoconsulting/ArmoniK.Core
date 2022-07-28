// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Adapters.Amqp.Tests;

[TestFixture]
public class QueueStorageTests
{
  public Options.Amqp options_;

  [SetUp]
  public void SetDefaultOptions()
  {
    /* These options are only to feed the QueueStorage constructor
     * and they do not play any role in the how the connection is created,
     * the later is defined in the  SimpleAmqpClientHelper class */
    options_ = new Options.Amqp()
    {
      Host = "localhost",
      User = "guest",
      Password = "guest",
      Port = 5672,
      CaPath = "somePath",
      Scheme = "someScheme",
      CredentialsPath = "somePath",
      MaxPriority = 21,
      AllowHostMismatch = false
    };
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

    var queueStorage = new QueueStorage(options_, provider.Object,
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

  [Test]
  public async Task CreateQueueStorageShouldSFailIfNoHostIsProvided()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    options_.Host = "";
    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    Assert.Throws<ArgumentOutOfRangeException>(() =>
     new QueueStorage(options_, provider.Object,
                                        NullLogger<QueueStorage>.Instance)
    );
  }

  [Test]
  public async Task CreateQueueStorageShouldSFailIfNoPortDefined()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    options_.Port = 0;
    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    Assert.Throws<ArgumentOutOfRangeException>(() =>
     new QueueStorage(options_, provider.Object,
                                        NullLogger<QueueStorage>.Instance)
    );
  }

  [Test]
  public async Task CreateQueueStorageShouldSFailIfMaxPriorityLessThanOne()
  {
    await using var helper = new SimpleAmqpClientHelper();
    var provider = new Mock<IProviderBase<Session>>();

    options_.MaxPriority = 0;
    provider.Setup(sp => sp.Get()).Returns(helper.Session);

    Assert.Throws<ArgumentOutOfRangeException>(() =>
     new QueueStorage(options_, provider.Object,
                                        NullLogger<QueueStorage>.Instance)
    );
  }
}
