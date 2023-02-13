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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class WatchToGrpcTests
{
  [Test]
  public void WatchShouldSucceed()
  {
    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

    var watchToGrpcInstance = new WatchToGrpc(new SimpleTaskTable(),
                                              new SimpleTaskWatcher(),
                                              new SimpleResultTable(),
                                              new SimpleResultWatcher(),
                                              NullLogger.Instance);


    var list = new List<EventSubscriptionResponse>();

    Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                   {
                                                     // Simple* that are used to create this instance do not check the session in their implementation
                                                     await foreach (var eventSubscriptionResponse in watchToGrpcInstance.GetEvents("",
                                                                                                                                   cts.Token)
                                                                                                                        .WithCancellation(cts.Token)
                                                                                                                        .ConfigureAwait(false))
                                                     {
                                                       Console.WriteLine(eventSubscriptionResponse);
                                                       list.Add(eventSubscriptionResponse);
                                                     }
                                                   });
    Assert.AreEqual(9,
                    list.Count);
  }

  [Test]
  [TestCase(3)]
  [TestCase(6)]
  public async Task MultipleWatchShouldSucceed(int nTries)
  {
    var cts      = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    var list     = new List<EventSubscriptionResponse>();
    var taskList = new List<Task>();

    for (var i = 0; i < nTries; i++)
    {
      taskList.Add(Task.Factory.StartNew(() =>
                                         {
                                           var watchToGrpcInstance = new WatchToGrpc(new SimpleTaskTable(),
                                                                                     new SimpleTaskWatcher(),
                                                                                     new SimpleResultTable(),
                                                                                     new SimpleResultWatcher(),
                                                                                     NullLogger.Instance);

                                           Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                                                          {
                                                                                            // Simple* that are used to create this instance do not check the session in their implementation
                                                                                            await foreach (var eventSubscriptionResponse in watchToGrpcInstance
                                                                                                                                            .GetEvents("",
                                                                                                                                                       cts.Token)
                                                                                                                                            .WithCancellation(cts.Token)
                                                                                                                                            .ConfigureAwait(false))
                                                                                            {
                                                                                              Console.WriteLine(eventSubscriptionResponse);
                                                                                              list.Add(eventSubscriptionResponse);
                                                                                            }
                                                                                          });
                                         },
                                         CancellationToken.None));
    }

    await taskList.WhenAll()
                  .ConfigureAwait(false);

    Assert.AreEqual(9 * nTries,
                    list.Count);
  }
}
