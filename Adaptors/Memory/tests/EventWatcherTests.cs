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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory.Tests;

/// <summary>
///   Checks the behaviours of the Memory watchers that come from the in-process change events
/// </summary>
[TestFixture]
public class EventWatcherTests
{
  [SetUp]
  public void SetUp()
  {
    provider_    = MemoryTablesProvider.Build();
    taskTable_   = provider_.GetRequiredService<TaskTable>();
    taskWatcher_ = provider_.GetRequiredService<ITaskWatcher>();
  }

  [TearDown]
  public void TearDown()
    => provider_?.Dispose();

  private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

  private ServiceProvider? provider_;
  private TaskTable?       taskTable_;
  private ITaskWatcher?    taskWatcher_;

  private static readonly TaskOptions Options = new(new Dictionary<string, string>(),
                                                    TimeSpan.FromHours(1),
                                                    1,
                                                    1,
                                                    "Partition",
                                                    "Application",
                                                    "Version",
                                                    "Namespace",
                                                    "Service",
                                                    "Engine");

  private static TaskData CreateTask(string taskId)
    => new("SessionId",
           taskId,
           "",
           "",
           $"{taskId}-payload",
           "CreatedBy",
           new List<string>(),
           new List<string>(),
           new List<string>(),
           new List<string>(),
           TaskStatus.Submitted,
           Options,
           new Output(OutputStatus.Success,
                      ""));

  private Task SetStatus(string     taskId,
                         TaskStatus status,
                         string     message = "")
    => taskTable_!.UpdateOneTask(taskId,
                                 null,
                                 new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                      status)
                                                                 .Set(data => data.StatusMessage,
                                                                      message),
                                 false);

  /// <summary>
  ///   Read events until <paramref name="count" /> events are received
  /// </summary>
  private static async Task<List<T>> Take<T>(IAsyncEnumerable<T> events,
                                             int                 count)
  {
    using var cts = new CancellationTokenSource(Timeout);
    return await events.Take(count)
                       .ToListAsync(cts.Token)
                       .ConfigureAwait(false);
  }

  [Test]
  public async Task ChangesMadeBeforeEnumerationShouldBeReported()
  {
    await taskTable_!.CreateTasks([CreateTask("task")])
                     .ConfigureAwait(false);

    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => data.SessionId == "SessionId")
                                     .ConfigureAwait(false);

    // The change is made after the subscription, but before the enumeration starts
    await SetStatus("task",
                    TaskStatus.Dispatched)
      .ConfigureAwait(false);

    Assert.That(await Take(updates,
                           1)
                  .ConfigureAwait(false),
                Is.EqualTo(new[]
                           {
                             new TaskStatusUpdate("SessionId",
                                                  "task",
                                                  TaskStatus.Dispatched),
                           }));
  }

  [Test]
  public async Task UpdatesThatKeepTheStatusShouldNotBeReported()
  {
    await taskTable_!.CreateTasks([CreateTask("task")])
                     .ConfigureAwait(false);

    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => data.SessionId == "SessionId")
                                     .ConfigureAwait(false);

    await SetStatus("task",
                    TaskStatus.Dispatched)
      .ConfigureAwait(false);
    // Same status, other field changed: no event
    await SetStatus("task",
                    TaskStatus.Dispatched,
                    "message")
      .ConfigureAwait(false);
    await SetStatus("task",
                    TaskStatus.Processing)
      .ConfigureAwait(false);

    Assert.That((await Take(updates,
                            2)
                   .ConfigureAwait(false)).Select(update => update.Status),
                Is.EqualTo(new[]
                           {
                             TaskStatus.Dispatched,
                             TaskStatus.Processing,
                           }));
  }

  [Test]
  public async Task FilterThatThrowsShouldNotBreakWritesNorOtherWatches()
  {
    await taskTable_!.CreateTasks([CreateTask("task")])
                     .ConfigureAwait(false);

    // Evaluating this filter throws, since the option does not exist
    var throwing = await taskWatcher_!.GetTaskStatusUpdates(data => data.Options.Options["missing"] == "value")
                                      .ConfigureAwait(false);
    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => data.SessionId == "SessionId")
                                     .ConfigureAwait(false);

    Assert.That(() => SetStatus("task",
                                TaskStatus.Dispatched),
                Throws.Nothing);

    Assert.That((await Take(updates,
                            1)
                   .ConfigureAwait(false)).Single()
                                          .Status,
                Is.EqualTo(TaskStatus.Dispatched));
    GC.KeepAlive(throwing);
  }

  [Test]
  public async Task CancellingAWatchShouldRemoveItsHandler()
  {
    using var cts = new CancellationTokenSource();
    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => true,
                                                           cts.Token)
                                     .ConfigureAwait(false);

    Assert.That(taskTable_!.TaskChangedHandlerCount,
                Is.EqualTo(1));

    await cts.CancelAsync()
             .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(taskTable_.TaskChangedHandlerCount,
                                  Is.Zero);
                      Assert.That(async () => await updates.ToListAsync()
                                                           .ConfigureAwait(false),
                                  Throws.InstanceOf<OperationCanceledException>());
                    });
  }

  [Test]
  public async Task EndingTheEnumerationShouldRemoveTheHandler()
  {
    await taskTable_!.CreateTasks([CreateTask("task")])
                     .ConfigureAwait(false);

    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => true)
                                     .ConfigureAwait(false);

    await SetStatus("task",
                    TaskStatus.Dispatched)
      .ConfigureAwait(false);

    await Take(updates,
               1)
      .ConfigureAwait(false);

    Assert.That(taskTable_.TaskChangedHandlerCount,
                Is.Zero);
  }

  [Test]
  [Repeat(10)]
  public async Task EventsShouldFollowTheOrderOfTheWrites()
  {
    const int writes = 64;
    await taskTable_!.CreateTasks([CreateTask("task")])
                     .ConfigureAwait(false);

    var updates = await taskWatcher_!.GetTaskStatusUpdates(data => true)
                                     .ConfigureAwait(false);

    // Concurrent writes alternate between two statuses, so that each write changes the status
    var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var tasks = Enumerable.Range(0,
                                 writes)
                          .Select(_ => Task.Run(async () =>
                                                {
                                                  await start.Task.ConfigureAwait(false);
                                                  await taskTable_.UpdateOneTask("task",
                                                                                 null,
                                                                                 new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                                                      TaskStatus.Processing),
                                                                                 false)
                                                                  .ConfigureAwait(false);
                                                  await taskTable_.UpdateOneTask("task",
                                                                                 data => data.Status == TaskStatus.Processing,
                                                                                 new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                                                      TaskStatus.Dispatched),
                                                                                 false)
                                                                  .ConfigureAwait(false);
                                                }))
                          .ToList();
    start.SetResult();
    await Task.WhenAll(tasks)
              .ConfigureAwait(false);

    // Sentinel: the last write, after all the others
    await SetStatus("task",
                    TaskStatus.Completed)
      .ConfigureAwait(false);

    var       received = new List<TaskStatus>();
    using var cts      = new CancellationTokenSource(Timeout);
    await foreach (var update in updates.WithCancellation(cts.Token)
                                        .ConfigureAwait(false))
    {
      received.Add(update.Status);
      if (update.Status == TaskStatus.Completed)
      {
        break;
      }
    }

    // In write order, consecutive events never repeat a status, since an event is only produced when the status changes
    Assert.That(received.Zip(received.Skip(1))
                        .Where(pair => pair.First == pair.Second),
                Is.Empty);
  }
}
