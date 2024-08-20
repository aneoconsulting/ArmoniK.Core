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
  public void TimeoutNoCounterParty([Values(0,
                                            15,
                                            30,
                                            45)]
                                    int timeout,
                                    [Values] bool isReader)
  {
    // Warmup Nunit runtime
    Assert.That(() => Task.Delay(0),
                Throws.Nothing);

    var queue = new RendezvousChannel<int>();

    var sw = Stopwatch.StartNew();

    Assert.That(() => isReader
                        ? queue.ReadAsync(TimeSpan.FromMilliseconds(timeout))
                        : queue.WriteAsync(0,
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
  [Repeat(20)]
  public async Task Timeout([Values(0,
                                    15,
                                    45)]
                            int timeout,
                            [Values(0,
                                    15,
                                    45)]
                            int wait,
                            [Values] bool isReader,
                            [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();
    var actor = new QueueActor(useCancellation,
                               timeout,
                               wait,
                               new TaskCompletionSource());

    var readerTask = RunWithStopWatch(() => actor.Interact(isReader,
                                                           (timeSpan,
                                                            token) => queue.ReadAsync(timeSpan,
                                                                                      token),
                                                           queue.CloseReader));
    var writerTask = RunWithStopWatch(() => actor.Interact(!isReader,
                                                           (timeSpan,
                                                            token) => TaskWithValue(queue.WriteAsync(3,
                                                                                                     timeSpan,
                                                                                                     token)),
                                                           queue.CloseWriter));

    var reader = await readerTask.ConfigureAwait(false);
    var writer = await writerTask.ConfigureAwait(false);

    var readExceptionType  = actor.ExceptionType(isReader);
    var writeExceptionType = actor.ExceptionType(!isReader);

    Console.WriteLine($"Read ({queue.IsReaderClosed,-5}): {reader.duration.TotalMilliseconds,-7} ms    Write ({queue.IsWriterClosed,-5}): {writer.duration.TotalMilliseconds,-7} ms");

    switch (timeout - wait)
    {
      case < 0:
        Assert.That(() => reader.task,
                    Throws.InstanceOf(readExceptionType));
        Assert.That(() => writer.task,
                    Throws.InstanceOf(writeExceptionType));
        break;
      case > 0:
        Assert.That(() => reader.task,
                    Throws.Nothing);
        Assert.That(() => writer.task,
                    Throws.Nothing);
        Assert.That(await reader.task.ConfigureAwait(false),
                    Is.EqualTo(3));
        break;
      default:
        var readException = await WaitException(reader.task)
                              .ConfigureAwait(false);
        var writeException = await WaitException(writer.task)
                               .ConfigureAwait(false);

        // Ensure that either both succeed or both fail
        Assert.That(writeException,
                    readException is null
                      ? Is.Null
                      : Is.InstanceOf(writeExceptionType));
        Assert.That(readException,
                    writeException is null
                      ? Is.Null
                      : Is.InstanceOf(readExceptionType));
        break;
    }
  }


  [Test]
  [Timeout(1000)]
  [Repeat(20)]
  public async Task TimeoutClose([Values(0,
                                         15,
                                         45)]
                                 int timeout,
                                 [Values(0,
                                         15,
                                         45)]
                                 int wait,
                                 [Values] bool isReader,
                                 [Values] bool useCancellation)
  {
    var queue = new RendezvousChannel<int>();
    var actor = new QueueActor(useCancellation,
                               timeout,
                               wait);

    var readerTask = RunWithStopWatch(() => actor.Interact(isReader,
                                                           (timeSpan,
                                                            token) => queue.ReadAsync(timeSpan,
                                                                                      token),
                                                           queue.CloseReader,
                                                           () =>
                                                           {
                                                             queue.CloseReader();
                                                             return Task.FromResult(0);
                                                           }));
    var writerTask = RunWithStopWatch(() => actor.Interact(!isReader,
                                                           (timeSpan,
                                                            token) => TaskWithValue(queue.WriteAsync(3,
                                                                                                     timeSpan,
                                                                                                     token)),
                                                           queue.CloseWriter,
                                                           () =>
                                                           {
                                                             queue.CloseWriter();
                                                             return Task.FromResult(new ValueTuple());
                                                           }));

    var reader = await readerTask.ConfigureAwait(false);
    var writer = await writerTask.ConfigureAwait(false);

    Console.WriteLine($"Read ({queue.IsReaderClosed,-5}): {reader.duration.TotalMilliseconds,-7} ms    Write ({queue.IsWriterClosed,-5}): {writer.duration.TotalMilliseconds,-7} ms");

    Task task = isReader
                  ? reader.task
                  : writer.task;

    var exceptionType = actor.ExceptionType();

    switch (timeout - wait)
    {
      case < 0:
        Assert.That(() => task,
                    Throws.InstanceOf(exceptionType));
        break;
      case > 0:
        Assert.That(() => task,
                    Throws.InstanceOf<ChannelClosedException>());
        break;
      default:
        Assert.That(() => task,
                    Throws.InstanceOf<ChannelClosedException>()
                          .Or.InstanceOf(exceptionType));
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
        x = await queue.ReadAsync(System.Threading.Timeout.InfiniteTimeSpan,
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
                               System.Threading.Timeout.InfiniteTimeSpan,
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

  private static Task<(Task<T> task, TimeSpan duration)> RunWithStopWatch<T>(Func<Task<T>> f)
    => Task.Run(async () =>
                {
                  var sw   = Stopwatch.StartNew();
                  var task = f();
                  try
                  {
                    await task.ConfigureAwait(false);
                  }
                  catch
                  {
                    // ignored
                  }

                  sw.Stop();

                  return (task, sw.Elapsed);
                });

  private async Task<ValueTuple> TaskWithValue(Task task)
  {
    await task.ConfigureAwait(false);
    return new ValueTuple();
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

  private class QueueActor(bool                  useCancellation,
                           int                   timeout,
                           int                   wait,
                           TaskCompletionSource? tcs = null)
  {
    public async Task<T> Interact<T>(bool                                       isActive,
                                     Func<TimeSpan, CancellationToken, Task<T>> interact,
                                     Action                                     close,
                                     Func<Task<T>>?                             interactPassive = null)
    {
      using var cts = new CancellationTokenSource();
      Task<T>   task;
      if (isActive)
      {
        if (useCancellation)
        {
          cts.CancelAfter(timeout);
          task = interact(System.Threading.Timeout.InfiniteTimeSpan,
                          cts.Token);
        }
        else
        {
          task = interact(TimeSpan.FromMilliseconds(timeout),
                          CancellationToken.None);
        }

        // Ensure counterparty starts after the interaction has begun
        tcs?.SetResult();
      }
      else
      {
        if (tcs is not null)
        {
          await tcs.Task.ConfigureAwait(false);
        }

        await Task.Delay(wait,
                         CancellationToken.None)
                  .ConfigureAwait(false);

        interactPassive ??= () => interact(TimeSpan.Zero,
                                           CancellationToken.None);

        task = interactPassive();
      }

      var x = await task.ConfigureAwait(false);

      close();

      return x;
    }

    public Type ExceptionType(bool isActive = true)
      => isActive && useCancellation
           ? typeof(OperationCanceledException)
           : typeof(TimeoutException);
  }
}
