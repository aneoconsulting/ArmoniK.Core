// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Memory.Tests;

[TestFixture]
public class QueueStorageTests
{
  [SetUp]
  public void SetUp()
    => queue_ = new QueueStorage(PullWaitTime);

  private static readonly TimeSpan PullWaitTime = TimeSpan.FromMilliseconds(100);

  private QueueStorage? queue_;

  private static TaskOptions Options(int priority)
    => new(new Dictionary<string, string>(),
           TimeSpan.FromHours(1),
           1,
           priority,
           "Partition",
           "Application",
           "Version",
           "Namespace",
           "Service",
           "Engine");

  private Task Push(string          partition,
                    int             priority,
                    params string[] taskIds)
    => queue_!.PushMessagesAsync(taskIds.Select(taskId => new MessageData(taskId,
                                                                          "SessionId",
                                                                          Options(priority))),
                                 partition);

  private Task<List<IQueueMessageHandler>> Pull(string partition,
                                                int    count)
    => queue_!.PullMessagesAsync(partition,
                                 count)
              .ToListAsync()
              .AsTask();

  /// <summary>
  ///   Pull every message of the partition, and remove them from the queue
  /// </summary>
  private async Task<List<string>> Drain(string partition)
  {
    var messages = await Pull(partition,
                              int.MaxValue)
                     .ConfigureAwait(false);
    foreach (var message in messages)
    {
      message.Status = QueueMessageStatus.Processed;
      await message.DisposeAsync()
                   .ConfigureAwait(false);
    }

    return messages.Select(message => message.TaskId)
                   .ToList();
  }

  [Test]
  public async Task MessagesShouldBePulledByPriorityThenPushOrder()
  {
    await Push("partition",
               1,
               "low1",
               "low2")
      .ConfigureAwait(false);
    await Push("partition",
               5,
               "high1",
               "high2")
      .ConfigureAwait(false);
    await Push("partition",
               3,
               "medium")
      .ConfigureAwait(false);

    Assert.That(await Drain("partition")
                  .ConfigureAwait(false),
                Is.EqualTo(new[]
                           {
                             "high1",
                             "high2",
                             "medium",
                             "low1",
                             "low2",
                           }));
  }

  [Test]
  public async Task PartitionsShouldBeIsolated()
  {
    await Push("partition1",
               1,
               "task1")
      .ConfigureAwait(false);
    await Push("partition2",
               1,
               "task2")
      .ConfigureAwait(false);

    Assert.Multiple(async () =>
                    {
                      Assert.That(await Drain("partition1")
                                    .ConfigureAwait(false),
                                  Is.EqualTo(new[]
                                             {
                                               "task1",
                                             }));
                      Assert.That(await Drain("partition2")
                                    .ConfigureAwait(false),
                                  Is.EqualTo(new[]
                                             {
                                               "task2",
                                             }));
                      Assert.That(await Drain("unknown")
                                    .ConfigureAwait(false),
                                  Is.Empty);
                    });
  }

  [Test]
  public async Task PullShouldReturnAtMostTheRequestedNumberOfMessages()
  {
    await Push("partition",
               1,
               "task1",
               "task2",
               "task3")
      .ConfigureAwait(false);

    Assert.Multiple(async () =>
                    {
                      Assert.That(await Pull("partition",
                                             2)
                                    .ConfigureAwait(false),
                                  Has.Count.EqualTo(2));
                      Assert.That(await Pull("partition",
                                             2)
                                    .ConfigureAwait(false),
                                  Has.Count.EqualTo(1));
                    });
  }

  [Test]
  public async Task PulledMessageShouldBeInvisibleUntilDisposed()
  {
    await Push("partition",
               1,
               "task")
      .ConfigureAwait(false);

    var message = (await Pull("partition",
                              1)
                     .ConfigureAwait(false)).Single();

    Assert.That(await Pull("partition",
                           1)
                  .ConfigureAwait(false),
                Is.Empty);

    await message.DisposeAsync()
                 .ConfigureAwait(false);

    Assert.That((await Pull("partition",
                            1)
                   .ConfigureAwait(false)).Single()
                                          .TaskId,
                Is.EqualTo("task"));
  }

  [TestCase(QueueMessageStatus.Waiting,
            new[]
            {
              "first",
              "second",
            })]
  [TestCase(QueueMessageStatus.Running,
            new[]
            {
              "first",
              "second",
            })]
  [TestCase(QueueMessageStatus.Failed,
            new[]
            {
              "first",
              "second",
            })]
  [TestCase(QueueMessageStatus.Postponed,
            new[]
            {
              "second",
              "first",
            })]
  [TestCase(QueueMessageStatus.Processed,
            new[]
            {
              "second",
            })]
  [TestCase(QueueMessageStatus.Cancelled,
            new[]
            {
              "second",
            })]
  [TestCase(QueueMessageStatus.Poisonous,
            new[]
            {
              "second",
            })]
  public async Task DisposedMessageShouldFollowItsStatus(QueueMessageStatus status,
                                                         string[]           expected)
  {
    await Push("partition",
               1,
               "first",
               "second")
      .ConfigureAwait(false);

    var first = (await Pull("partition",
                            1)
                   .ConfigureAwait(false)).Single();
    first.Status = status;
    await first.DisposeAsync()
               .ConfigureAwait(false);

    Assert.That(await Drain("partition")
                  .ConfigureAwait(false),
                Is.EqualTo(expected));
  }

  [TestCase(QueueMessageStatus.Waiting)]
  [TestCase(QueueMessageStatus.Failed)]
  [TestCase(QueueMessageStatus.Postponed)]
  public async Task RedeliveredMessageShouldKeepItsId(QueueMessageStatus status)
  {
    await Push("partition",
               1,
               "first",
               "second")
      .ConfigureAwait(false);

    var first = (await Pull("partition",
                            1)
                   .ConfigureAwait(false)).Single();
    var id = first.MessageId;
    first.Status = status;
    await first.DisposeAsync()
               .ConfigureAwait(false);

    var redelivered = (await Pull("partition",
                                  2)
                         .ConfigureAwait(false)).Single(message => message.TaskId == "first");

    Assert.That(redelivered.MessageId,
                Is.EqualTo(id));
  }

  [Test]
  public async Task DisposingTwiceShouldReleaseTheMessageOnce()
  {
    await Push("partition",
               1,
               "task")
      .ConfigureAwait(false);

    var message = (await Pull("partition",
                              1)
                     .ConfigureAwait(false)).Single();
    await message.DisposeAsync()
                 .ConfigureAwait(false);
    await message.DisposeAsync()
                 .ConfigureAwait(false);

    Assert.That(await Drain("partition")
                  .ConfigureAwait(false),
                Is.EqualTo(new[]
                           {
                             "task",
                           }));
  }

  [Test]
  public async Task TaskPushedTwiceShouldGiveTwoMessages()
  {
    await Push("partition",
               1,
               "task",
               "task")
      .ConfigureAwait(false);

    var messages = await Pull("partition",
                              10)
                     .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(messages.Select(message => message.TaskId),
                                  Is.EqualTo(new[]
                                             {
                                               "task",
                                               "task",
                                             }));
                      Assert.That(messages.Select(message => message.MessageId)
                                          .Distinct(),
                                  Has.Exactly(2)
                                     .Items);
                    });
  }

  [Test]
  public async Task PullOnAnEmptyQueueShouldWaitForAPush()
  {
    var pull = Pull("partition",
                    1);

    await Task.Delay(PullWaitTime / 4)
              .ConfigureAwait(false);
    Assert.That(pull.IsCompleted,
                Is.False);

    await Push("partition",
               1,
               "task")
      .ConfigureAwait(false);

    Assert.That((await pull.ConfigureAwait(false)).Single()
                                                  .TaskId,
                Is.EqualTo("task"));
  }

  [Test]
  public async Task PullOnAnEmptyQueueShouldReturnNothingAfterTheWaitTime()
  {
    var watch = Stopwatch.StartNew();

    var messages = await Pull("partition",
                              1)
                     .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(messages,
                                  Is.Empty);
                      Assert.That(watch.Elapsed,
                                  Is.GreaterThanOrEqualTo(PullWaitTime * 0.9));
                    });
  }

  [Test]
  public async Task MessagesNotEnumeratedShouldBePutBack()
  {
    await Push("partition",
               1,
               "task1",
               "task2",
               "task3")
      .ConfigureAwait(false);

    // Take only the first message of a pull of three
    var first = await queue_!.PullMessagesAsync("partition",
                                                3)
                             .FirstAsync()
                             .ConfigureAwait(false);
    first.Status = QueueMessageStatus.Processed;
    await first.DisposeAsync()
               .ConfigureAwait(false);

    Assert.That(await Drain("partition")
                  .ConfigureAwait(false),
                Is.EqualTo(new[]
                           {
                             "task2",
                             "task3",
                           }));
  }

  [Test]
  [Repeat(10)]
  public async Task ConcurrentPushesAndPullsShouldNeitherLoseNorDuplicateMessages()
  {
    const int producers         = 8;
    const int messagesPerPusher = 100;
    const int total             = producers * messagesPerPusher;

    var processed = new ConcurrentBag<string>();
    var start     = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    var pushers = Enumerable.Range(0,
                                   producers)
                            .Select(p => Task.Run(async () =>
                                                  {
                                                    await start.Task.ConfigureAwait(false);
                                                    for (var i = 0; i < messagesPerPusher; i++)
                                                    {
                                                      await Push("partition",
                                                                 i % 3,
                                                                 $"task{p}-{i}")
                                                        .ConfigureAwait(false);
                                                    }
                                                  }))
                            .ToList();

    var pullers = Enumerable.Range(0,
                                   producers)
                            .Select(c => Task.Run(async () =>
                                                  {
                                                    await start.Task.ConfigureAwait(false);
                                                    var round = 0;
                                                    while (processed.Count < total)
                                                    {
                                                      foreach (var message in await Pull("partition",
                                                                                         3)
                                                                                .ConfigureAwait(false))
                                                      {
                                                        // Release some messages without processing them, to mix the statuses
                                                        if ((round++ + c) % 4 == 0)
                                                        {
                                                          message.Status = QueueMessageStatus.Postponed;
                                                        }
                                                        else
                                                        {
                                                          message.Status = QueueMessageStatus.Processed;
                                                          processed.Add(message.TaskId);
                                                        }

                                                        await message.DisposeAsync()
                                                                     .ConfigureAwait(false);
                                                      }

                                                      await Task.Yield();
                                                    }
                                                  }))
                            .ToList();

    start.SetResult();
    await Task.WhenAll(pushers.Concat(pullers))
              .WaitAsync(TimeSpan.FromSeconds(30))
              .ConfigureAwait(false);

    Assert.Multiple(async () =>
                    {
                      Assert.That(processed,
                                  Is.EquivalentTo(Enumerable.Range(0,
                                                                   producers)
                                                            .SelectMany(p => Enumerable.Range(0,
                                                                                              messagesPerPusher)
                                                                                       .Select(i => $"task{p}-{i}"))));
                      Assert.That(await Drain("partition")
                                    .ConfigureAwait(false),
                                  Is.Empty);
                    });
  }
}
