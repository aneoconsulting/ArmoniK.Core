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

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

namespace ArmoniK.Samples.Graph.Common.Tests;

[TestFixture(TestOf = typeof(GraphStats))]
public class GraphStatsTest
{
  private static GraphSettings Settings(string graphTasks,
                                        int    levels,
                                        string tasksPerLevel,
                                        string dependenciesPerTask = "const:1")
    => GraphSettings.Parse(1,
                           graphTasks,
                           levels,
                           tasksPerLevel,
                           dependenciesPerTask,
                           "uniform",
                           0,
                           "const:100",
                           "const:1");

  [Test]
  public void FanOutStats()
  {
    // One task on level 0, three tasks on level 1 depending on it
    var stats = GraphStats.Compute(Settings("const:4",
                                            2,
                                            "1,100"),
                                   4);

    Assert.That(stats,
                Is.EqualTo(new GraphStats(4,
                                          1,
                                          0,
                                          4,
                                          2,
                                          3,
                                          1,
                                          3,
                                          0,
                                          2,
                                          200,
                                          400,
                                          4 * 1024)));
  }

  [Test]
  public void ExpandingTaskShouldDelegateItsOutput()
  {
    // A single task with budget 2: it expands into a sub-graph made of one task
    var stats = GraphStats.Compute(Settings("const:1",
                                            3,
                                            "uniform"),
                                   2);

    Assert.That(stats,
                Is.EqualTo(new GraphStats(2,
                                          2,
                                          1,
                                          1,
                                          1,
                                          0,
                                          0,
                                          0,
                                          1,
                                          2,
                                          200,
                                          200,
                                          1024)));
  }

  [TestCase(1)]
  [TestCase(3)]
  [TestCase(7)]
  [TestCase(50)]
  public void ChainsShouldBeFullySequential(int nTasks)
  {
    // One task per level and one dependency per task: every graph is a chain, and since the dependents of an
    // expanding task wait for the end of its sub-graph, the whole session is a single chain of nTasks tasks
    var stats = GraphStats.Compute(Settings("const:3",
                                            3,
                                            "uniform"),
                                   nTasks);

    Assert.That(stats.TotalTasks,
                Is.EqualTo(nTasks));
    Assert.That(stats.CriticalPathTasks,
                Is.EqualTo(nTasks));
    Assert.That(stats.MinimumDurationMs,
                Is.EqualTo(nTasks * 100));
    Assert.That(stats.TotalDurationMs,
                Is.EqualTo(nTasks * 100));
    Assert.That(stats.MaxDependents,
                Is.EqualTo(nTasks > 1
                             ? 1
                             : 0));
    Assert.That(stats.Graphs,
                Is.EqualTo(stats.ExpandingTasks + 1));
  }

  [Test]
  public void DeepNestingShouldNotOverflow()
  {
    // Every graph holds a single task: each task expands into a sub-graph of one task, NTasks - 1 levels deep.
    // The statistics do not build the positions, that grow with the depth, so the memory stays linear in NTasks.
    const int nTasks = 20000;
    var stats = GraphStats.Compute(Settings("const:1",
                                            3,
                                            "uniform"),
                                   nTasks);

    Assert.That(stats.TotalTasks,
                Is.EqualTo(nTasks));
    Assert.That(stats.NestingDepth,
                Is.EqualTo(nTasks - 1));
    Assert.That(stats.ExpandingTasks,
                Is.EqualTo(nTasks - 1));
    Assert.That(stats.CriticalPathTasks,
                Is.EqualTo(nTasks));
    Assert.That(stats.TotalOutputBytes,
                Is.EqualTo(1024));
  }

  [Test]
  public void PayloadShouldRoundTrip()
  {
    var settings = Settings("const:6",
                            3,
                            "uniform",
                            "const:2");
    var graph = GraphGenerator.GenerateRoot(settings,
                                            6);
    var ids = graph.Tasks.Select(t => $"result-{t.Id}")
                   .ToList();
    var task = graph.Tasks[^1];
    var payload = TaskPayload.Create(graph,
                                     task,
                                     ids);

    Assert.That(payload.Dependencies.Keys,
                Is.EquivalentTo(task.Dependencies.Select(d => ids[d])));
    Assert.That(payload.Dependencies.Values,
                Has.All.EqualTo(1024));

    var read = TaskPayload.Deserialize(payload.Serialize());
    Assert.That(read.Position,
                Is.EqualTo(task.Position));
    Assert.That(read.Budget,
                Is.EqualTo(task.Budget));
    Assert.That(read.DurationMs,
                Is.EqualTo(task.DurationMs));
    Assert.That(read.OutputSize,
                Is.EqualTo(task.OutputSize));
    Assert.That(read.Dependencies,
                Is.EquivalentTo(payload.Dependencies));
  }

  [Test]
  public void SettingsShouldRoundTripThroughTaskOptions()
  {
    var settings = GraphSettings.Parse(-12,
                                       "lognormal:10:1:max=50",
                                       4,
                                       "1,10,100",
                                       "poisson:2",
                                       "zipf:1.1",
                                       0.25,
                                       "normal:100:20:min=0",
                                       "const:0");
    var options = new Dictionary<string, string>(settings.ToOptions())
                  {
                    {
                      "Unrelated", "value"
                    },
                  };
    var read = GraphSettings.FromOptions(options);

    Assert.That(read.ToOptions(),
                Is.EquivalentTo(settings.ToOptions()));
    Assert.That(read.Seed,
                Is.EqualTo(settings.Seed));
    Assert.That(GraphStats.Compute(read,
                                   500),
                Is.EqualTo(GraphStats.Compute(settings,
                                              500)));
  }
}
