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

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class TaskTableTests : TaskTableTestBase
{
  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  private MongoDatabaseProvider? tableProvider_;


  public override void GetTaskTableInstance()
  {
    tableProvider_ = new MongoDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    TaskTable = provider.GetRequiredService<ITaskTable>();
    RunTests  = true;
  }

  /// <summary>
  ///   Several agents remove, at the same time, the dependencies of the same tasks, each dependency being removed twice
  ///   (as when the Submitter and an Agent both see the same dependency completed).
  ///   Every task whose dependencies are all removed must be reported ready by exactly one call.
  /// </summary>
  [Test]
  [Repeat(3)]
  public async Task ConcurrentDependencyRemovalShouldReportReadyTasksOnce()
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

      // Released once every call is started, so that they all run concurrently
      var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      // Two calls per dependency (the payload counts as one)
      var removals = dependencies.Append($"payload{round}")
                                 .SelectMany(dependency => new[]
                                                           {
                                                             dependency,
                                                             dependency,
                                                           })
                                 .Select(dependency => Task.Run(async () =>
                                                                {
                                                                  await start.Task.ConfigureAwait(false);
                                                                  return await TaskTable.RemoveRemainingDataDependenciesAsync(taskIds,
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

      start.SetResult();

      var ready = (await Task.WhenAll(removals)
                             .ConfigureAwait(false)).SelectMany(ids => ids)
                                                    .ToList();

      Assert.That(ready,
                  Is.EquivalentTo(taskIds),
                  $"round {round}");
    }
  }
}
