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

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory.Tests;

/// <summary>
///   Checks that each call to the in-memory tables is atomic when calls run concurrently
/// </summary>
[TestFixture]
public class ConcurrencyTests
{
  [SetUp]
  public void SetUp()
  {
    var services = new ServiceCollection();
    services.AddMemoryTables();
    services.AddLogging();

    provider_    = services.BuildServiceProvider(true);
    taskTable_   = provider_.GetRequiredService<ITaskTable>();
    resultTable_ = provider_.GetRequiredService<IResultTable>();
  }

  [TearDown]
  public void TearDown()
    => provider_?.Dispose();

  private const int Concurrency = 64;

  private ServiceProvider? provider_;
  private ITaskTable?      taskTable_;
  private IResultTable?    resultTable_;

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

  private static TaskData CreateTask(string        taskId,
                                     TaskStatus    status,
                                     IList<string> dataDependencies)
    => new("SessionId",
           taskId,
           "",
           "",
           $"{taskId}-payload",
           "CreatedBy",
           new List<string>(),
           dataDependencies,
           new List<string>(),
           new List<string>(),
           status,
           Options,
           new Output(OutputStatus.Success,
                      ""));

  private static Result CreateResult(string resultId,
                                     string ownerTaskId)
    => new("SessionId",
           resultId,
           "",
           "CreatedBy",
           "",
           ownerTaskId,
           ResultStatus.Created,
           new List<string>(),
           DateTime.UtcNow,
           null,
           0,
           [],
           false);

  /// <summary>
  ///   Run the given function concurrently, starting all the calls at the same time
  /// </summary>
  private static async Task<T[]> RunConcurrently<T>(Func<int, Task<T>> func)
  {
    var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var tasks = Enumerable.Range(0,
                                 Concurrency)
                          .Select(i => Task.Run(async () =>
                                                {
                                                  await start.Task.ConfigureAwait(false);
                                                  return await func(i)
                                                           .ConfigureAwait(false);
                                                }))
                          .ToList();
    start.SetResult();
    return await Task.WhenAll(tasks)
                     .ConfigureAwait(false);
  }

  [Test]
  [Repeat(10)]
  public async Task ConcurrentAcquisitionsOfATaskShouldHaveASingleWinner()
  {
    await taskTable_!.CreateTasks([
                       CreateTask("task",
                                  TaskStatus.Submitted,
                                  []),
                     ])
                     .ConfigureAwait(false);

    var results = await RunConcurrently(i => taskTable_.UpdateOneTask("task",
                                                                      data => data.Status == TaskStatus.Submitted,
                                                                      new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                                           TaskStatus.Dispatched)
                                                                                                      .Set(data => data.OwnerPodId,
                                                                                                           $"agent{i}"),
                                                                      true))
                    .ConfigureAwait(false);

    var winners = results.Where(r => r is not null)
                         .ToList();
    var task = await taskTable_.ReadTaskAsync("task",
                                              data => data)
                               .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(winners,
                                  Has.Count.EqualTo(1));
                      Assert.That(winners.Single()!.Status,
                                  Is.EqualTo(TaskStatus.Submitted));
                      Assert.That(task.Status,
                                  Is.EqualTo(TaskStatus.Dispatched));
                    });
  }

  [Test]
  [Repeat(10)]
  public async Task UpdateManyShouldNotUpdateTasksThatNoLongerMatch()
  {
    var ids = Enumerable.Range(0,
                               Concurrency)
                        .Select(i => $"task{i}")
                        .ToList();
    await taskTable_!.CreateTasks(ids.Select(id => CreateTask(id,
                                                              TaskStatus.Submitted,
                                                              [])))
                     .ConfigureAwait(false);

    // Half of the calls acquire one task each, the other half cancel every task still submitted.
    var results = await RunConcurrently(async i =>
                                        {
                                          if (i % 2 == 0)
                                          {
                                            var before = await taskTable_.UpdateOneTask(ids[i],
                                                                                        data => data.Status == TaskStatus.Submitted,
                                                                                        new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                                                             TaskStatus.Dispatched),
                                                                                        true)
                                                                         .ConfigureAwait(false);
                                            return (acquired: before is null
                                                                ? 0L
                                                                : 1L, cancelled: 0L);
                                          }

                                          var cancelled = await taskTable_.UpdateManyTasks(data => data.Status == TaskStatus.Submitted,
                                                                                           new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                                                                TaskStatus.Cancelled))
                                                                          .ConfigureAwait(false);
                                          return (acquired: 0L, cancelled);
                                        })
                    .ConfigureAwait(false);

    var counts = await taskTable_.CountTasksAsync(data => true)
                                 .ConfigureAwait(false);
    var byStatus = counts.ToDictionary(c => c.Status,
                                       c => (long)c.Count);

    Assert.Multiple(() =>
                    {
                      // Each task is changed exactly once: either acquired or cancelled, never both.
                      Assert.That(byStatus.GetValueOrDefault(TaskStatus.Dispatched),
                                  Is.EqualTo(results.Sum(r => r.acquired)));
                      Assert.That(byStatus.GetValueOrDefault(TaskStatus.Cancelled),
                                  Is.EqualTo(results.Sum(r => r.cancelled)));
                      Assert.That(byStatus.GetValueOrDefault(TaskStatus.Submitted),
                                  Is.Zero);
                    });
  }

  [Test]
  [Repeat(10)]
  public async Task ConcurrentDependencyRemovalsShouldReportTheTaskOnce()
  {
    var dependencies = Enumerable.Range(0,
                                        Concurrency - 1)
                                 .Select(i => $"dependency{i}")
                                 .ToList();
    var task = CreateTask("task",
                          TaskStatus.Creating,
                          dependencies);
    await taskTable_!.CreateTasks([task])
                     .ConfigureAwait(false);

    // The payload is also a remaining dependency: Concurrency dependencies in total, one removed per call.
    var toRemove = dependencies.Append(task.PayloadId)
                               .ToList();

    var results = await RunConcurrently(i => taskTable_.RemoveRemainingDataDependenciesAsync(["task"],
                                                                                             [toRemove[i]],
                                                                                             data => data.TaskId)
                                                       .ToListAsync()
                                                       .AsTask())
                    .ConfigureAwait(false);

    Assert.That(results.SelectMany(ids => ids)
                       .ToList(),
                Is.EqualTo(new[]
                           {
                             "task",
                           }));
  }

  [Test]
  public async Task DependencyRemovalShouldNotModifyPreviouslyReadTasks()
  {
    await taskTable_!.CreateTasks([
                       CreateTask("task",
                                  TaskStatus.Creating,
                                  ["dependency"]),
                     ])
                     .ConfigureAwait(false);

    var before = await taskTable_.ReadTaskAsync("task",
                                                data => data)
                                 .ConfigureAwait(false);

    await taskTable_.RemoveRemainingDataDependenciesAsync(["task"],
                                                          ["dependency"],
                                                          data => data.TaskId)
                    .ToListAsync()
                    .ConfigureAwait(false);

    Assert.That(before.RemainingDataDependencies.Keys,
                Does.Contain("dependency"));
  }

  [Test]
  public async Task ChangeResultOwnershipShouldOnlyTransferTheRequestedKeys()
  {
    await resultTable_!.Create([
                         CreateResult("result1",
                                      "oldTask"),
                         CreateResult("result2",
                                      "oldTask"),
                         CreateResult("result3",
                                      "oldTask"),
                         CreateResult("result4",
                                      "otherTask"),
                       ])
                       .ConfigureAwait(false);

    await resultTable_.ChangeResultOwnership("oldTask",
                                             [
                                               new IResultTable.ChangeResultOwnershipRequest(["result1"],
                                                                                             "newTask1"),
                                               new IResultTable.ChangeResultOwnershipRequest(["result2", "result4"],
                                                                                             "newTask2"),
                                             ],
                                             CancellationToken.None)
                      .ConfigureAwait(false);

    var owners = await resultTable_.GetResults(result => true,
                                               result => new
                                                         {
                                                           result.ResultId,
                                                           result.OwnerTaskId,
                                                         })
                                   .ToDictionaryAsync(r => r.ResultId,
                                                      r => r.OwnerTaskId)
                                   .ConfigureAwait(false);

    Assert.That(owners,
                Is.EquivalentTo(new Dictionary<string, string>
                                {
                                  {
                                    "result1", "newTask1"
                                  },
                                  {
                                    "result2", "newTask2"
                                  },
                                  {
                                    "result3", "oldTask"
                                  },
                                  {
                                    // Not owned by oldTask: left unchanged
                                    "result4", "otherTask"
                                  },
                                }));
  }

  [Test]
  [Repeat(10)]
  public async Task ConcurrentTaskDependenciesShouldAllBeRecorded()
  {
    await resultTable_!.Create([
                         CreateResult("result",
                                      "owner"),
                       ])
                       .ConfigureAwait(false);

    await RunConcurrently(async i =>
                          {
                            await resultTable_.AddTaskDependencies(new Dictionary<string, ICollection<string>>
                                                                   {
                                                                     {
                                                                       "result", [$"task{i}"]
                                                                     },
                                                                   })
                                              .ConfigureAwait(false);
                            return i;
                          })
      .ConfigureAwait(false);

    var result = await resultTable_.GetResult("result")
                                   .ConfigureAwait(false);

    Assert.That(result.DependentTasks,
                Is.EquivalentTo(Enumerable.Range(0,
                                                 Concurrency)
                                          .Select(i => $"task{i}")));
  }
}
