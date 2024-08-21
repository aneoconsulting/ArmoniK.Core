// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RendezvousChannel<>))]
public class RendezvousChannelTest
{
  [Test]
  [Timeout(10000)]
  [Repeat(1000)]
  public async Task WriteShouldWork([Values(0,
                                            1,
                                            2,
                                            3)]
                                    int nbRead,
                                    [Values(0,
                                            1,
                                            2,
                                            3)]
                                    int nbWrite)
  {
    var queue = new RendezvousChannel<int>();

    var reader = ReadAsync(queue,
                           nbRead)
                 .ToListAsync(CancellationToken.None)
                 .AsTask();
    var writer = WriteAsync(queue,
                            nbWrite);

    var n = int.Min(nbRead,
                    nbWrite);

    Assert.That(() => Task.WhenAll(reader,
                                   writer),
                Throws.Nothing);

    var read    = await reader.ConfigureAwait(false);
    var written = await writer.ConfigureAwait(false);

    Assert.That(read,
                Has.Count.EqualTo(n));
    Assert.That(written,
                Is.EqualTo(n));
    Assert.That(read,
                Is.EqualTo(Enumerable.Range(0,
                                            n)
                                     .ToList()));
  }

  [Test]
  [Timeout(10000)]
  [Repeat(20)]
  public void TimeoutNoCounterParty([Values(0,
                                            15,
                                            45)]
                                    int timeout,
                                    [Values] bool isReader,
                                    [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();

    var t0 = DateTime.UtcNow;

    Assert.That(() => Interact(queue,
                               isReader,
                               useCancellation,
                               timeout),
                Throws.InstanceOf(ExceptionType(useCancellation)));

    var t1      = DateTime.UtcNow;
    var elapsed = (t1 - t0).TotalMilliseconds;

    Assert.That(() => Interact(queue,
                               !isReader),
                Throws.InstanceOf(ExceptionType()));

    Console.WriteLine($"Timeout {timeout,7} ms    got {elapsed,7} ms");

    Assert.That(elapsed,
                Is.GreaterThanOrEqualTo(timeout / 2));
    Assert.That(elapsed,
                Is.LessThanOrEqualTo(timeout * 3 + 50));
  }


  [Test]
  [Timeout(1000)]
  [Repeat(200)]
  public void TimeoutCounterParty([Values] bool isReader,
                                  [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();

    var task = Interact(queue,
                        isReader,
                        useCancellation,
                        100);

    Assert.That(() => Interact(queue,
                               !isReader),
                Throws.Nothing);
    Assert.That(() => task,
                Throws.Nothing);
    Assert.That(task.Result,
                Is.EqualTo(3));
  }

  [Test]
  [Timeout(1000)]
  [Repeat(200)]
  public void TimeoutClose([Values] bool isReader,
                           [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();

    var task = Interact(queue,
                        isReader,
                        useCancellation,
                        100);

    Assert.That(() => Close(queue,
                            !isReader),
                Throws.Nothing);
    Assert.That(() => task,
                Throws.InstanceOf<ChannelClosedException>());
    Assert.That(() => Interact(queue,
                               isReader,
                               timeout: 100),
                Throws.InstanceOf<ChannelClosedException>());
  }

  [Test]
  [Timeout(10000)]
  [Repeat(200)]
  public async Task TimeoutRaceCounterParty([Values] bool isReader,
                                            [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();

    var waiter = Interact(queue,
                          isReader,
                          useCancellation,
                          30,
                          true);

    var triggerer = Interact(queue,
                             !isReader,
                             close: true);

    var waitException = await WaitException(waiter)
                          .ConfigureAwait(false);
    var triggerException = await WaitException(triggerer)
                             .ConfigureAwait(false);

    Assert.That(waitException,
                triggerException is null
                  ? Is.Null
                  : Is.InstanceOf(ExceptionType(useCancellation)));
    Assert.That(triggerException,
                waitException is null
                  ? Is.Null
                  : Is.InstanceOf(ExceptionType()));

    if (waitException is null)
    {
      Assert.That(waiter.Result,
                  Is.EqualTo(3));
      Assert.That(triggerer.Result,
                  Is.EqualTo(3));
    }
  }

  [Test]
  [Timeout(10000)]
  [Repeat(50)]
  public async Task TimeoutRaceClose([Values] bool isReader,
                                     [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();

    var waiter = Interact(queue,
                          isReader,
                          useCancellation,
                          30);

    await Task.Delay(30)
              .ConfigureAwait(false);
    Assert.That(() => Close(queue,
                            !isReader),
                Throws.Nothing);
    Assert.That(() => waiter,
                Throws.InstanceOf<ChannelClosedException>()
                      .Or.InstanceOf(ExceptionType(useCancellation)));
    Assert.That(() => Interact(queue,
                               isReader,
                               timeout: 100),
                Throws.InstanceOf<ChannelClosedException>());
  }

  private static async IAsyncEnumerable<int> ReadAsync(RendezvousChannel<int> queue,
                                                       int                    closeAfter)
  {
    var nbRead = 0;
    while (nbRead < closeAfter)
    {
      int x;
      try
      {
        x = await queue.ReadAsync(Timeout.InfiniteTimeSpan,
                                  CancellationToken.None)
                       .ConfigureAwait(false);
      }
      catch (ChannelClosedException)
      {
        break;
      }

      yield return x;
      nbRead++;
    }

    queue.CloseReader();
  }

  private static async Task<int> WriteAsync(RendezvousChannel<int> queue,
                                            int                    closeAfter)
  {
    var nbWritten = 0;

    while (nbWritten < closeAfter)
    {
      try
      {
        await queue.WriteAsync(nbWritten,
                               Timeout.InfiniteTimeSpan,
                               CancellationToken.None)
                   .ConfigureAwait(false);
        nbWritten++;
      }
      catch (ChannelClosedException)
      {
        break;
      }
    }

    queue.CloseWriter();

    return nbWritten;
  }

  private static async Task<Exception?> WaitException(Task task)
  {
    try
    {
      await task.ConfigureAwait(false);
      return null;
    }
    catch (Exception e)
    {
      return e;
    }
  }

  private static async Task<int> Interact(RendezvousChannel<int> queue,
                                          bool                   isReader,
                                          bool                   useCancellation = false,
                                          int                    timeout         = 0,
                                          bool                   close           = false)
  {
    using var cts = useCancellation
                      ? new CancellationTokenSource(timeout)
                      : null;
    var token = CancellationToken.None;
    var span  = TimeSpan.FromMilliseconds(timeout);

    if (cts is not null)
    {
      span  = Timeout.InfiniteTimeSpan;
      token = cts.Token;
    }

    var x = 3;

    if (isReader)
    {
      x = await queue.ReadAsync(span,
                                token)
                     .ConfigureAwait(false);
    }
    else
    {
      await queue.WriteAsync(x,
                             span,
                             token)
                 .ConfigureAwait(false);
    }

    if (close)
    {
      Close(queue,
            isReader);
    }

    return x;
  }

  private static void Close(RendezvousChannel<int> queue,
                            bool                   isReader)
  {
    if (isReader)
    {
      queue.CloseReader();
    }
    else
    {
      queue.CloseWriter();
    }
  }

  private static Type ExceptionType(bool useCancellation = false)
    => useCancellation
         ? typeof(OperationCanceledException)
         : typeof(TimeoutException);
}
