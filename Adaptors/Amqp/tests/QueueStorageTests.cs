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
using System.Collections;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Amqp.Tests;

[TestFixture]
public class QueueStorageTests : QueueStorageTestsBase
{
  protected override async Task GetQueueStorageInstance()
  {
    await using var pushClient = new SimpleAmqpClient();

    PushQueueStorage = new PushQueueStorage(Options!,
                                            pushClient,
                                            NullLogger<PushQueueStorage>.Instance);

    /* Our implementation uses separate connections for pushing and pulling */
    await using var pullClient = new SimpleAmqpClient();

    PullQueueStorage = new PullQueueStorage(Options!,
                                            pullClient,
                                            NullLogger<PullQueueStorage>.Instance);
    RunTests = true;
  }

  [Test]
  public async Task CreatePullQueueStorageShouldFail()
  {
    await using var pullClient = new SimpleAmqpClient();

    var badOpts = CreateDefaultOptions();
    badOpts.PartitionId = "";
    Assert.Throws<ArgumentOutOfRangeException>(() =>
                                               {
                                                 var _ = new PullQueueStorage(badOpts,
                                                                              pullClient,
                                                                              NullLogger<PullQueueStorage>.Instance);
                                               });
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
  public async Task CreateQueueStorageShouldThrowIfBadOptionsGiven(Common.Injection.Options.Amqp options)
  {
    await using var client = new SimpleAmqpClient();

    Assert.Throws<ArgumentOutOfRangeException>(() =>
                                               {
                                                 var _ = new QueueStorage(options,
                                                                          client);
                                               });
  }
}
