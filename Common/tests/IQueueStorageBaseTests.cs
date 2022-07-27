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

using ArmoniK.Core.Common.Tests.Helpers;

using NUnit.Framework;

using System;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class IQueueStorageBaseTests
{
  private ReceiverLink receiver_;
  private SenderLink sender_;

  [Test]
  public async Task SimpleBrokerTest()
  {
    var timeout = TimeSpan.FromMilliseconds(100);
    await using var helper = new SimpleAmqpClientHelper();

    sender_ = new SenderLink(helper.Session, "sender-link", "q1");
    var sendMsg = new Message("Hello AMQP!");
    await sender_.SendAsync(sendMsg)
      .ConfigureAwait(false);
    Console.WriteLine("Sent " + sendMsg.Body.ToString());
    await sender_.CloseAsync()
      .ConfigureAwait(false);

    receiver_ = new ReceiverLink(helper.Session, "receiver-link", "q1");
    Console.WriteLine("Receiver connected to broker.");
    var receiveMsg = await receiver_.ReceiveAsync(timeout)
      .ConfigureAwait(false);
    Console.WriteLine("Received " + receiveMsg.Body.ToString());
    receiver_.Accept(receiveMsg);
    await receiver_.CloseAsync()
      .ConfigureAwait(false);

    Assert.AreEqual(receiveMsg.Body, sendMsg.Body);
  }
}
