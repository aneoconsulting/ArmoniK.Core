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
using System.Diagnostics;
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
  [Timeout(1000)]
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
  [Timeout(1000)]
  [Repeat(10)]
  public void WriteTimeoutNoReader([Values(0,
                                           15,
                                           30,
                                           45)]
                                   int timeout)
  {
    var queue = new RendezvousChannel<int>();

    var sw = Stopwatch.StartNew();

    Assert.That(() => queue.WriteAsync(0,
                                       TimeSpan.FromMilliseconds(timeout)),
                Throws.InstanceOf<TimeoutException>());

    sw.Stop();

    Assert.That(sw.Elapsed.TotalMilliseconds,
                Is.GreaterThanOrEqualTo(timeout - 5));
    Assert.That(sw.Elapsed.TotalMilliseconds,
                Is.LessThanOrEqualTo(timeout + 20));
  }

  [Test]
  [Timeout(1000)]
  [Repeat(10)]
  public void ReadTimeoutNoWriter([Values(0,
                                          15,
                                          30,
                                          45)]
                                  int timeout)
  {
    var queue = new RendezvousChannel<int>();
    var sw    = Stopwatch.StartNew();

    Assert.That(() => queue.ReadAsync(TimeSpan.FromMilliseconds(timeout)),
                Throws.InstanceOf<TimeoutException>());

    sw.Stop();

    Assert.That(sw.Elapsed.TotalMilliseconds,
                Is.GreaterThanOrEqualTo(timeout - 5));
    Assert.That(sw.Elapsed.TotalMilliseconds,
                Is.LessThanOrEqualTo(timeout + 20));
  }


  [Test]
  [Timeout(1000)]
  [Repeat(20)]
  public async Task WriteTimeout([Values(0,
                                         15,
                                         45)]
                                 int writeTimeout,
                                 [Values(0,
                                         15,
                                         45)]
                                 int readWait)
  {
    var queue = new RendezvousChannel<int>();

    var times = new TimeSpan[2];
    var tcs   = new TaskCompletionSource();
    var reader = Task.Run(async () =>
                          {
                            var sw = Stopwatch.StartNew();
                            try
                            {
                              await tcs.Task.ConfigureAwait(false);
                              await Task.Delay(TimeSpan.FromMilliseconds(readWait))
                                        .ConfigureAwait(false);

                              var x = await queue.ReadAsync(TimeSpan.Zero)
                                                 .ConfigureAwait(false);

                              queue.CloseReader();

                              return x;
                            }
                            finally
                            {
                              sw.Stop();
                              times[0] = sw.Elapsed;
                            }
                          });
    var writer = Task.Run(async () =>
                          {
                            var sw = Stopwatch.StartNew();
                            try
                            {
                              var write = queue.WriteAsync(3,
                                                           TimeSpan.FromMilliseconds(writeTimeout));
                              // Ensure read occurs after write has started
                              tcs.SetResult();
                              await write.ConfigureAwait(true);
                              queue.CloseWriter();
                            }
                            finally
                            {
                              sw.Stop();
                              times[1] = sw.Elapsed;
                            }
                          });

    try
    {
      await Task.WhenAll(reader,
                         writer)
                .ConfigureAwait(false);
    }
    catch
    {
    }

    Console.WriteLine($"Read ({queue.IsReaderClosed,-5}): {times[0].TotalMilliseconds,-7} ms    Write ({queue.IsWriterClosed,-5}): {times[1].TotalMilliseconds,-7} ms");

    switch (writeTimeout - readWait)
    {
      case < 0:
        Assert.That(() => reader,
                    Throws.InstanceOf<TimeoutException>());
        Assert.That(() => writer,
                    Throws.InstanceOf<TimeoutException>());
        break;
      case > 0:
        Assert.That(() => reader,
                    Throws.Nothing);
        Assert.That(() => writer,
                    Throws.Nothing);
        Assert.That(await reader.ConfigureAwait(false),
                    Is.EqualTo(3));
        break;
      default:
        Exception? readException  = null;
        Exception? writeException = null;
        try
        {
          await reader.ConfigureAwait(false);
        }
        catch (Exception? e)
        {
          readException = e;
        }

        try
        {
          await writer.ConfigureAwait(false);
        }
        catch (Exception? e)
        {
          writeException = e;
        }

        // Ensure that either both succeed or both fail
        Assert.That(writeException,
                    readException is null
                      ? Is.Null
                      : Is.InstanceOf<TimeoutException>());
        Assert.That(readException,
                    writeException is null
                      ? Is.Null
                      : Is.InstanceOf<TimeoutException>());
        break;
    }
  }

  [Test]
  [Timeout(1000)]
  [Repeat(20)]
  public async Task ReadTimeout([Values(0,
                                        15,
                                        45)]
                                int readTimeout,
                                [Values(0,
                                        15,
                                        45)]
                                int writeWait)
  {
    var queue = new RendezvousChannel<int>();

    var times = new TimeSpan[2];
    var tcs   = new TaskCompletionSource();
    var reader = Task.Run(async () =>
                          {
                            var sw = Stopwatch.StartNew();
                            try
                            {
                              var read = queue.ReadAsync(TimeSpan.FromMilliseconds(readTimeout));

                              // Ensure write occurs after reading has started
                              tcs.SetResult();

                              var x = await read.ConfigureAwait(true);
                              queue.CloseReader();

                              return x;
                            }
                            finally
                            {
                              sw.Stop();
                              times[0] = sw.Elapsed;
                            }
                          });
    var writer = Task.Run(async () =>
                          {
                            var sw = Stopwatch.StartNew();
                            try
                            {
                              await tcs.Task.ConfigureAwait(false);
                              await Task.Delay(TimeSpan.FromMilliseconds(writeWait))
                                        .ConfigureAwait(false);

                              await queue.WriteAsync(3,
                                                     TimeSpan.Zero)
                                         .ConfigureAwait(true);
                              queue.CloseWriter();
                            }
                            finally
                            {
                              sw.Stop();
                              times[1] = sw.Elapsed;
                            }
                          });

    try
    {
      await Task.WhenAll(reader,
                         writer)
                .ConfigureAwait(false);
    }
    catch
    {
    }

    Console.WriteLine($"Read ({queue.IsReaderClosed,-5}): {times[0].TotalMilliseconds,-7} ms    Write ({queue.IsWriterClosed,-5}): {times[1].TotalMilliseconds,-7} ms");

    switch (readTimeout - writeWait)
    {
      case < 0:
        Assert.That(() => reader,
                    Throws.InstanceOf<TimeoutException>());
        Assert.That(() => writer,
                    Throws.InstanceOf<TimeoutException>());
        break;
      case > 0:
        Assert.That(() => reader,
                    Throws.Nothing);
        Assert.That(() => writer,
                    Throws.Nothing);
        Assert.That(await reader.ConfigureAwait(false),
                    Is.EqualTo(3));
        break;
      default:
        Exception? readException  = null;
        Exception? writeException = null;
        try
        {
          await reader.ConfigureAwait(false);
        }
        catch (Exception? e)
        {
          readException = e;
        }

        try
        {
          await writer.ConfigureAwait(false);
        }
        catch (Exception? e)
        {
          writeException = e;
        }

        // Ensure that either both succeed or both fail
        Assert.That(writeException,
                    readException is null
                      ? Is.Null
                      : Is.InstanceOf<TimeoutException>());
        Assert.That(readException,
                    writeException is null
                      ? Is.Null
                      : Is.InstanceOf<TimeoutException>());
        break;
    }
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
}
