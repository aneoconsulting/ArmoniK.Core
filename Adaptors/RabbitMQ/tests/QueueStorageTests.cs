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
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.RabbitMQ.Tests;

[TestFixture]
public class QueueStorageTests : QueueStorageTestsBase
{
  protected override Task GetQueueStorageInstance()
  {
    options_ = CreateDefaultOptions();
    using var pullClient = new SimpleRabbitClient();

    PullQueueStorage = new PullQueueStorage(options_!,
                                            pullClient,
                                            NullLogger<PullQueueStorage>.Instance);

    using var pushClient = new SimpleRabbitClient();

    PushQueueStorage = new PushQueueStorage(options_!,
                                            pushClient,
                                            NullLogger<PushQueueStorage>.Instance);
    RunTests = true;

    return Task.CompletedTask;
  }

  private Common.Injection.Options.Amqp? options_;

  [Test]
  public void CreatePullQueueStorageShouldFail()
  {
    using var pullClient = new SimpleRabbitClient();

    var badOpt = CreateDefaultOptions();
    badOpt.PartitionId = "";
    Assert.Throws<ArgumentOutOfRangeException>(() =>
                                               {
                                                 var _ = new PullQueueStorage(badOpt,
                                                                              pullClient,
                                                                              NullLogger<PullQueueStorage>.Instance);
                                               });
  }
}
