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
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using RabbitMQ.Client;

namespace ArmoniK.Core.Adapters.RabbitMQ.Tests;

[TestFixture]
public class QueueStorageTests : QueueStorageTestsBase
{
  public override Task GetQueueStorageInstance()
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

    var provider = new Mock<IConnectionRabbit>();
    provider.Setup(cp => cp.Channel)
            .Returns(channel_);

    PullQueueStorage = new PullQueueStorage(options_!,
                                            provider.Object,
                                            NullLogger<PullQueueStorage>.Instance);

    PushQueueStorage = new PushQueueStorage(options_!,
                                            provider.Object,
                                            NullLogger<PushQueueStorage>.Instance);
    RunTests = true;

    return Task.CompletedTask;
  }

  private IModel?      channel_;
  private IConnection? connection_;

  private Common.Injection.Options.Amqp? options_;

  [Test]
  public void CreatePullQueueStorageShouldFail()
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
}
