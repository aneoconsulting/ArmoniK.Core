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
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Amqp.Tests;

[TestFixture]
public class QueueStorageTests : QueueStorageTestsBase
{
  public Common.Injection.Options.Amqp? Options;

  public override async Task GetQueueStorageInstance()
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IConnectionAmqp>();

    provider.Setup(cp => cp.Connection)
            .Returns(helper.Connection);

    PushQueueStorage = new PushQueueStorage(Options!,
                                            provider.Object,
                                            NullLogger<PushQueueStorage>.Instance);

    PullQueueStorage = new PullQueueStorage(Options!,
                                            provider.Object,
                                            NullLogger<PullQueueStorage>.Instance);
    RunTests = true;
  }

  [Test]
  public async Task CreatePullQueueStorageShouldFail()
  {
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IConnectionAmqp>();

    provider.Setup(sp => sp.Connection)
            .Returns(helper.Connection);

    var badOpts = CreateDefaultOptions();
    badOpts.PartitionId = "";
    Assert.Throws<ArgumentOutOfRangeException>(() => new PullQueueStorage(badOpts,
                                                                          provider.Object,
                                                                          NullLogger<PullQueueStorage>.Instance));
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
    await using var helper   = new SimpleAmqpClientHelper();
    var             provider = new Mock<IConnectionAmqp>();

    provider.Setup(sp => sp.Connection)
            .Returns(helper.Connection);

    Assert.Throws<ArgumentOutOfRangeException>(() => new QueueStorage(options,
                                                                      provider.Object));
  }
}
