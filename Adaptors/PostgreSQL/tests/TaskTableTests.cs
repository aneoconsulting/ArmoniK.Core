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
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

[TestFixture]
public class TaskTableTests : TaskTableTestBase
{
  private PostgresDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  public override void GetTaskTableInstance()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    TaskTable = provider.GetRequiredService<ITaskTable>();
    RunTests  = true;
  }

  [Test]
  public async Task ReadTaskAsyncShouldNotReturnRemainingDataDependencies()
  {
    if (RunTests)
    {
      // Postgres-specific: RemainingDataDependencies lives in a separate join table, so
      // ReadTaskAsync only loads it when the selector explicitly reads it, avoiding the join
      // query otherwise (no production caller needs it through the identity selector, e.g.
      // Pollster's TaskHandler, or through TaskDataMask projections). Memory/MongoDB always
      // have it already in the stored object regardless of selector shape, so this contract
      // does not hold there.
      var result = await TaskTable!.ReadTaskAsync("TaskCreatingId",
                                                  CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.That(result.RemainingDataDependencies,
                  Is.Empty);
    }
  }

  /// <summary>
  ///   Several agents complete, at the same time, the results awaited by the same tasks, while another
  ///   agent updates these tasks one statement per task, in the opposite order (as BulkUpdateTasks does).
  ///   Dependency removal must neither deadlock with these updates (40P01) nor forget a task: every task
  ///   whose dependencies are all removed must be reported ready by at least one call.
  /// </summary>
  [Test]
  [Repeat(3)]
  public async Task ConcurrentDependencyRemovalShouldNotDeadlockNorForgetTasks()
  {
    if (!RunTests)
    {
      return;
    }

    const int nTasks        = 50;
    const int nDependencies = 4;
    const int nRounds       = 10;

    var options = new TaskOptions(new Dictionary<string, string>(),
                                  TimeSpan.MaxValue,
                                  5,
                                  1,
                                  "part1",
                                  "applicationName",
                                  "applicationVersion",
                                  "applicationNamespace",
                                  "applicationService",
                                  "engineType");

    for (var round = 0; round < nRounds; round++)
    {
      var dependencies = Enumerable.Range(0,
                                          nDependencies)
                                   .Select(i => $"dependency{round}-{i}")
                                   .ToArray();
      var taskIds = Enumerable.Range(0,
                                     nTasks)
                              .Select(_ => Guid.NewGuid()
                                               .ToString())
                              .ToArray();

      await TaskTable!.CreateTasks(taskIds.Select(taskId => new TaskData("SessionConcurrent",
                                                                         taskId,
                                                                         "",
                                                                         "",
                                                                         $"payload{round}",
                                                                         "CreatedBy",
                                                                         Array.Empty<string>(),
                                                                         dependencies,
                                                                         new[]
                                                                         {
                                                                           $"output-{taskId}",
                                                                         },
                                                                         Array.Empty<string>(),
                                                                         TaskStatus.Pending,
                                                                         options,
                                                                         new Output(OutputStatus.Error,
                                                                                    ""))))
                      .ConfigureAwait(false);

      using var start = new ManualResetEventSlim();

      // One call per dependency (the payload counts as one), each one listing the tasks in a different order
      var removals = dependencies.Append($"payload{round}")
                                 .Select((dependency,
                                          i) => Task.Run(async () =>
                                                         {
                                                           start.Wait();
                                                           return await TaskTable.RemoveRemainingDataDependenciesAsync(taskIds.Skip(i * nTasks / (nDependencies + 1))
                                                                                                                              .Concat(taskIds.Take(i * nTasks /
                                                                                                                                                   (nDependencies + 1)))
                                                                                                                              .ToArray(),
                                                                                                                       new[]
                                                                                                                       {
                                                                                                                         dependency,
                                                                                                                       },
                                                                                                                       td => td.TaskId,
                                                                                                                       CancellationToken.None)
                                                                                 .ToListAsync(CancellationToken.None)
                                                                                 .ConfigureAwait(false);
                                                         }))
                                 .ToList();

      // Row-by-row updates of the same tasks in the reverse order, in a single transaction
      var updates = Task.Run(async () =>
                             {
                               start.Wait();
                               await TaskTable.BulkUpdateTasks(taskIds.Reverse()
                                                                      .Select(taskId => (taskId, new UpdateDefinition<TaskData>().Set(td => td.StatusMessage,
                                                                                                                                      "touched"))),
                                                               CancellationToken.None)
                                              .ConfigureAwait(false);
                             });

      start.Set();

      var ready = (await Task.WhenAll(removals)
                             .ConfigureAwait(false)).SelectMany(ids => ids)
                                                    .ToHashSet();
      await updates.ConfigureAwait(false);

      Assert.That(ready,
                  Is.EquivalentTo(taskIds),
                  $"round {round}");
    }
  }
}
