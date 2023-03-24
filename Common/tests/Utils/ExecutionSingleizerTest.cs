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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Utils;

[TestFixture(TestOf = typeof(ExecutionSingleizer<int>))]
public class ExecutionSingleizerTest
{
  [SetUp]
  public void SetUp()
  {
    single_ = new ExecutionSingleizer<int>();
    val_    = 0;
  }

  [TearDown]
  public void TearDown()
    => single_ = null;

  private ExecutionSingleizer<int>? single_;
  private int                       val_;

  [Test]
  public async Task SingleExecutionShouldSucceed()
  {
    var i = await single_!.Call(ct => Set(1,
                                          0,
                                          ct))
                          .ConfigureAwait(false);
    Assert.AreEqual(1,
                    i);
    Assert.AreEqual(1,
                    val_);
  }

  [Test]
  public async Task RepeatedExecutionShouldSucceed()
  {
    for (var t = 1; t <= 10; ++t)
    {
      var t2 = t;
      var i = await single_!.Call(ct => Set(t2,
                                            0,
                                            ct))
                            .ConfigureAwait(false);
      Assert.AreEqual(t,
                      i);
      Assert.AreEqual(t,
                      val_);
    }
  }

  [Test]
  public async Task ConcurrentExecutionShouldSucceed()
  {
    var ti = single_!.Call(ct => Set(1,
                                     10,
                                     ct));
    var tj = single_!.Call(ct => Set(2,
                                     10,
                                     ct));
    var i = await ti.ConfigureAwait(false);
    var j = await tj.ConfigureAwait(false);
    Assert.AreEqual(1,
                    i);
    Assert.AreEqual(1,
                    j);
    Assert.AreEqual(1,
                    val_);
  }

  [Test]
  public async Task RepeatedConcurrentExecutionShouldSucceed()
  {
    for (var t = 1; t <= 10 * 2; t += 2)
    {
      var t2 = t;
      var ti = single_!.Call(ct => Set(t2,
                                       10,
                                       ct));
      var tj = single_!.Call(ct => Set(t2 + 1,
                                       10,
                                       ct));
      var i = await ti.ConfigureAwait(false);
      var j = await tj.ConfigureAwait(false);
      Assert.AreEqual(t,
                      i);
      Assert.AreEqual(t,
                      j);
      Assert.AreEqual(t,
                      val_);
    }
  }

  [Test]
  public async Task ManyConcurrentExecutionShouldSucceed()
  {
    var n     = 1000000;
    var tasks = new Task<int>[n];

    for (var i = 0; i < n; ++i)
    {
      var i2 = i;
      tasks[i] = single_!.Call(ct => Set(i2,
                                         0,
                                         ct));
    }

    for (var i = 0; i < n; ++i)
    {
      var j = await tasks[i]
                .ConfigureAwait(false);
      Assert.GreaterOrEqual(i,
                            j);
    }
  }

  [Test]
  public void CancelExecutionShouldFail()
  {
    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                var cts = new CancellationTokenSource();
                                                var task = single_!.Call(ct => Set(1,
                                                                                   1000,
                                                                                   ct),
                                                                         cts.Token);
                                                cts.Cancel();
                                                await task.ConfigureAwait(false);
                                              });
    Assert.AreEqual(0,
                    val_);
    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                var cts = new CancellationTokenSource();
                                                cts.Cancel();
                                                var task = single_!.Call(ct => Set(1,
                                                                                   1000,
                                                                                   ct),
                                                                         cts.Token);
                                                await task.ConfigureAwait(false);
                                              });
    Assert.AreEqual(0,
                    val_);
  }

  [Test]
  public async Task ConcurrentPartialCancelExecutionShouldSucceed()
  {
    var cts = new CancellationTokenSource();
    var t1 = single_!.Call(ct => Set(1,
                                     100,
                                     ct),
                           cts.Token);
    var t2 = single_!.Call(ct => Set(2,
                                     100,
                                     ct),
                           CancellationToken.None);
    cts.Cancel();
    Assert.ThrowsAsync<TaskCanceledException>(async () => await t1.ConfigureAwait(false));

    var j = await t2.ConfigureAwait(false);
    Assert.AreEqual(1,
                    j);
    Assert.AreEqual(1,
                    val_);
  }

  [Test]
  public void ConcurrentCancelExecutionShouldFail()
  {
    var cts = new CancellationTokenSource();
    var t1 = single_!.Call(ct => Set(1,
                                     1000,
                                     ct),
                           cts.Token);
    var t2 = single_!.Call(ct => Set(2,
                                     1000,
                                     ct),
                           cts.Token);
    cts.Cancel();
    Assert.ThrowsAsync<TaskCanceledException>(async () => await t1.ConfigureAwait(false));
    Assert.ThrowsAsync<TaskCanceledException>(async () => await t2.ConfigureAwait(false));

    Assert.AreEqual(0,
                    val_);
  }


  [Test]
  public async Task CheckExpire()
  {
    var single = new ExecutionSingleizer<int>(TimeSpan.FromMilliseconds(100));
    var i = await single.Call(ct => Set(1,
                                        0,
                                        ct))
                        .ConfigureAwait(false);
    Assert.AreEqual(1,
                    i);
    Assert.AreEqual(1,
                    val_);

    i = await single.Call(ct => Set(2,
                                    0,
                                    ct))
                    .ConfigureAwait(false);
    Assert.AreEqual(1,
                    i);
    Assert.AreEqual(1,
                    val_);

    await Task.Delay(150)
              .ConfigureAwait(false);

    i = await single.Call(ct => Set(3,
                                    0,
                                    ct))
                    .ConfigureAwait(false);
    Assert.AreEqual(3,
                    i);
    Assert.AreEqual(3,
                    val_);
  }

  private async Task<int> Set(int               i,
                              int               delay,
                              CancellationToken cancellationToken)
  {
    if (delay > 0)
    {
      await Task.Delay(delay,
                       cancellationToken)
                .ConfigureAwait(false);
    }

    val_ = i;
    return i;
  }
}
