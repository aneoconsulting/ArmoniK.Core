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

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Threading;

using ArmoniK.Api.gRPC.V1.Graphs;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging.Abstractions;

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

    // Simple* that are used to create this instance do not check the session in their implementation
    var res = watchToGrpcInstance.GetGraph("",
                                           cts.Token)
                                 .GetAsyncEnumerator(cts.Token);

    var list = new List<GraphContentResponse>();

    Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                   {
                                                     while (await res.MoveNextAsync()
                                                                     .ConfigureAwait(false))
                                                     {
                                                       Console.WriteLine(res.Current);
                                                       list.Add(res.Current);
                                                     }
                                                   });
    Assert.AreEqual(9,
                    list.Count);
  }
}
