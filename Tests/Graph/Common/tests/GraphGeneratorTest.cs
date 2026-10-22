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
using System.Text;

using NUnit.Framework;

namespace ArmoniK.Samples.Graph.Common.Tests;

[TestFixture(TestOf = typeof(GraphGenerator))]
public class GraphGeneratorTest
{
  private static GraphSettings Settings(long   seed                = 1,
                                        string graphTasks          = "uniform:5:20",
                                        int    levels              = 3,
                                        string tasksPerLevel       = "uniform",
                                        string dependenciesPerTask = "uniform:1:4",
                                        string parentChoice        = "uniform",
                                        double skipLevels          = 0,
                                        string taskDuration        = "const:100",
                                        string outputSize          = "const:1")
    => GraphSettings.Parse(seed,
                           graphTasks,
                           levels,
                           tasksPerLevel,
                           dependenciesPerTask,
                           parentChoice,
                           skipLevels,
                           taskDuration,
                           outputSize);

  /// <summary>
  ///   Generates every graph of the session (client graph first, then the sub-graphs in depth first order)
  /// </summary>
  private static IEnumerable<TaskGraph> AllGraphs(GraphSettings settings,
                                                  int           nTasks)
  {
    var stack = new Stack<TaskGraph>();
    stack.Push(GraphGenerator.GenerateRoot(settings,
                                           nTasks));
    while (stack.Count > 0)
    {
      var graph = stack.Pop();
      yield return graph;
      foreach (var task in graph.Tasks.Where(t => t.IsExpanding)
                                .Reverse())
      {
        stack.Push(GraphGenerator.GenerateSubGraph(settings,
                                                   task));
      }
    }
  }

  /// <summary>
  ///   Textual description of the full graph, to compare graphs
  /// </summary>
  private static string Describe(GraphSettings settings,
                                 int           nTasks)
  {
    var builder = new StringBuilder();
    foreach (var graph in AllGraphs(settings,
                                    nTasks))
    {
      builder.Append('[')
             .Append(graph.Position)
             .Append(']')
             .Append(string.Join(",",
                                 graph.LevelSizes))
             .Append('\n');
      foreach (var task in graph.Tasks)
      {
        builder.Append(task.Position)
               .Append(' ')
               .Append(task.Budget)
               .Append(' ')
               .Append(task.DurationMs)
               .Append(' ')
               .Append(task.OutputSize)
               .Append(" <- ")
               .Append(string.Join(",",
                                   task.Dependencies))
               .Append('\n');
      }
    }

    return builder.ToString();
  }

  private static IEnumerable<TestCaseData> BudgetCases()
  {
    foreach (var nTasks in new[]
                           {
                             1,
                             2,
                             3,
                             17,
                             100,
                             1000,
                           })
    {
      foreach (var graphTasks in new[]
                                 {
                                   "const:1",
                                   "const:2",
                                   "uniform:5:20",
                                   "lognormal:10:1",
                                   "const:1000",
                                 })
      {
        foreach (var seed in new[]
                             {
                               1L,
                               42L,
                               -7L,
                             })
        {
          yield return new TestCaseData(seed,
                                        nTasks,
                                        graphTasks);
        }
      }
    }
  }

  [TestCaseSource(nameof(BudgetCases))]
  public void TotalTasksShouldBeNTasks(long   seed,
                                       int    nTasks,
                                       string graphTasks)
  {
    var settings = Settings(seed,
                            graphTasks);
    var graphs = AllGraphs(settings,
                           nTasks)
      .ToList();

    Assert.That(graphs.Sum(g => g.Tasks.Count),
                Is.EqualTo(nTasks));
    Assert.That(GraphStats.Compute(settings,
                                   nTasks)
                          .TotalTasks,
                Is.EqualTo(nTasks));

    foreach (var graph in graphs)
    {
      // The tasks of a graph share its budget
      Assert.That(graph.Tasks.Sum(t => t.Budget),
                  Is.EqualTo(graph.Budget));
      Assert.That(graph.Tasks,
                  Has.All.Matches<GraphTask>(t => t.Budget >= 1));
    }
  }

  [Test]
  public void SameSeedShouldGiveSameGraph()
  {
    var first = Describe(Settings(skipLevels: 0.3,
                                  parentChoice: "zipf:1.2",
                                  taskDuration: "lognormal:100:1",
                                  outputSize: "exponential:2"),
                         500);
    var second = Describe(Settings(skipLevels: 0.3,
                                   parentChoice: "zipf:1.2",
                                   taskDuration: "lognormal:100:1",
                                   outputSize: "exponential:2"),
                          500);
    Assert.That(second,
                Is.EqualTo(first));
  }

  [Test]
  public void DifferentSeedShouldGiveDifferentGraph()
    => Assert.That(Describe(Settings(2),
                            500),
                   Is.Not.EqualTo(Describe(Settings(),
                                           500)));

  [Test]
  public void GraphShouldNotChange()
  {
    // Pins the generated graph: if this value changes, graphs generated by older clients or workers are not the same
    // anymore. Only update it on purpose.
    var description = Describe(Settings(skipLevels: 0.3,
                                        parentChoice: "zipf:1.2",
                                        taskDuration: "uniform:0:200",
                                        outputSize: "uniform:0:4"),
                               300);
    Assert.That(StableHash.Fnv1A64(description),
                Is.EqualTo(GraphFingerprint));
  }

  private const ulong GraphFingerprint = 17260837842181036809UL;

  [Test]
  public void StableHashShouldMatchReferenceValues()
  {
    // Reference values of FNV-1a 64
    Assert.That(StableHash.Fnv1A64(""),
                Is.EqualTo(0xcbf29ce484222325UL));
    Assert.That(StableHash.Fnv1A64("a"),
                Is.EqualTo(0xaf63dc4c8601ec8cUL));
    Assert.That(StableHash.Fnv1A64("foobar"),
                Is.EqualTo(0x85944171f73967e8UL));
  }

  [Test]
  public void RandomShouldBeDeterministic()
  {
    var a = new DeterministicRandom(123);
    var b = new DeterministicRandom(123);
    var c = new DeterministicRandom(124);
    var seqA = Enumerable.Range(0,
                                100)
                         .Select(_ => a.NextUInt64())
                         .ToList();
    Assert.That(Enumerable.Range(0,
                                 100)
                          .Select(_ => b.NextUInt64()),
                Is.EqualTo(seqA));
    Assert.That(Enumerable.Range(0,
                                 100)
                          .Select(_ => c.NextUInt64()),
                Is.Not.EqualTo(seqA));
  }

  [Test]
  public void DependenciesShouldPointToEarlierLevelsOfTheSameGraph([Values("uniform", "zipf:1.5", "zipf:5", "local")] string parentChoice,
                                                                   [Values(0, 0.5, 1)]                                double skipLevels)
  {
    var settings = Settings(graphTasks: "uniform:1:60",
                            levels: 5,
                            dependenciesPerTask: "uniform:0:12",
                            parentChoice: parentChoice,
                            skipLevels: skipLevels);

    foreach (var graph in AllGraphs(settings,
                                    2000))
    {
      foreach (var task in graph.Tasks)
      {
        Assert.That(graph.Tasks[task.Id],
                    Is.SameAs(task));
        Assert.That(task.Dependencies,
                    Is.Unique);
        foreach (var dependency in task.Dependencies)
        {
          Assert.That(dependency,
                      Is.InRange(0,
                                 graph.Tasks.Count - 1));
          var level = graph.Tasks[dependency].Level;
          Assert.That(level,
                      Is.LessThan(task.Level));
          if (skipLevels == 0)
          {
            Assert.That(level,
                        Is.EqualTo(task.Level - 1));
          }
        }

        if (task.Level == 0)
        {
          Assert.That(task.Dependencies,
                      Is.Empty);
        }
      }
    }
  }

  [Test]
  public void DependencyCountShouldBeClampedToCandidates()
  {
    var settings = Settings(graphTasks: "const:6",
                            levels: 3,
                            dependenciesPerTask: "const:100");
    var graph = GraphGenerator.GenerateRoot(settings,
                                            6);
    Assert.That(graph.LevelSizes,
                Is.EqualTo(Enumerable.Repeat(2,
                                             3)));
    Assert.That(graph.Tasks.Where(t => t.Level > 0),
                Has.All.Matches<GraphTask>(t => t.Dependencies.Count == 2));

    var skipping = GraphGenerator.GenerateRoot(settings with
                                               {
                                                 SkipLevels = 0.5,
                                               },
                                               6);
    Assert.That(skipping.Tasks.Where(t => t.Level == 2),
                Has.All.Matches<GraphTask>(t => t.Dependencies.Count == 4));
  }

  [Test]
  public void SubGraphShouldEndWithASingleAggregator([Values(1, 2, 3, 6)] int levels)
  {
    var settings = Settings(graphTasks: "uniform:1:30",
                            levels: levels,
                            dependenciesPerTask: "uniform:0:3");

    var subGraphs = 0;
    foreach (var parentGraph in AllGraphs(settings,
                                          3000))
    {
      foreach (var task in parentGraph.Tasks.Where(t => t.IsExpanding))
      {
        var graph = GraphGenerator.GenerateSubGraph(settings,
                                                    task);
        subGraphs++;

        Assert.That(graph.IsSubGraph,
                    Is.True);
        Assert.That(graph.Budget,
                    Is.EqualTo(task.Budget - 1));
        Assert.That(graph.LevelSizes[^1],
                    Is.EqualTo(1));
        Assert.That(graph.Aggregator.Level,
                    Is.EqualTo(graph.LevelSizes.Count - 1));
        Assert.That(graph.Aggregator.OutputSize,
                    Is.EqualTo(task.OutputSize));
        Assert.That(graph.Aggregator.Position,
                    Does.StartWith(task.Position + "/"));
        Assert.That(graph.LevelSizes.Count,
                    Is.LessThanOrEqualTo(Math.Max(2,
                                                  levels)));
        if (graph.Tasks.Count == 1)
        {
          Assert.That(graph.LevelSizes,
                      Has.Count.EqualTo(1));
        }
        else
        {
          Assert.That(graph.LevelSizes,
                      Has.Count.GreaterThanOrEqualTo(2));
          Assert.That(graph.Aggregator.Dependencies,
                      Is.Not.Empty);
        }
      }
    }

    Assert.That(subGraphs,
                Is.GreaterThan(10));
  }

  [Test]
  public void SubGraphShouldOnlyDependOnSeedPositionAndBudget()
  {
    var settings = Settings(skipLevels: 0.2);

    var reference = GraphGenerator.GenerateSubGraph(settings,
                                                    "1.4/0.2",
                                                    57,
                                                    2048);

    // Generating other graphs before must not change anything
    _ = AllGraphs(settings,
                  1000)
      .ToList();
    var again = GraphGenerator.GenerateSubGraph(settings,
                                                "1.4/0.2",
                                                57,
                                                2048);
    Assert.That(Describe(again),
                Is.EqualTo(Describe(reference)));

    // The tasks of the sub-graph are positioned below the expanding task
    Assert.That(reference.Tasks.Select(t => t.Position),
                Has.All.StartWith("1.4/0.2/"));

    // Each of the inputs changes the sub-graph
    Assert.That(Describe(GraphGenerator.GenerateSubGraph(settings,
                                                         "1.4/0.3",
                                                         57,
                                                         2048)),
                Is.Not.EqualTo(Describe(reference)
                                 .Replace("1.4/0.2/",
                                          "1.4/0.3/")));
    Assert.That(Describe(GraphGenerator.GenerateSubGraph(settings,
                                                         "1.4/0.2",
                                                         58,
                                                         2048)),
                Is.Not.EqualTo(Describe(reference)));
    Assert.That(Describe(GraphGenerator.GenerateSubGraph(settings with
                                                         {
                                                           Seed = 2,
                                                         },
                                                         "1.4/0.2",
                                                         57,
                                                         2048)),
                Is.Not.EqualTo(Describe(reference)));
  }

  [Test]
  public void PositionHashShouldBeTheHashOfThePosition()
  {
    var settings = Settings();
    foreach (var graph in AllGraphs(settings,
                                    500))
    {
      foreach (var task in graph.Tasks)
      {
        Assert.That(task.PositionHash,
                    Is.EqualTo(StableHash.Fnv1A64(task.Position)));

        if (task.IsExpanding)
        {
          // The sub-graph generated for the statistics only differs by the missing positions
          var full = GraphGenerator.GenerateSubGraph(settings,
                                                     task);
          var compact = GraphGenerator.GenerateSubGraphWithoutPositions(settings,
                                                                        task);
          Assert.That(compact.Tasks.Select(t => t.Position),
                      Has.All.Empty);
          Assert.That(compact.Tasks.Select(DescribeWithoutPosition),
                      Is.EqualTo(full.Tasks.Select(DescribeWithoutPosition)));
        }
      }
    }
  }

  [Test]
  public void ZipfShouldConcentrateDependencies()
  {
    var settings = Settings(graphTasks: "const:1000",
                            levels: 2,
                            tasksPerLevel: "1,1",
                            dependenciesPerTask: "const:3");
    var uniform = GraphStats.Compute(settings,
                                     1000);
    var zipf = GraphStats.Compute(settings with
                                  {
                                    ParentChoice = ParentChoice.Parse("zipf:1.5"),
                                  },
                                  1000);
    Assert.That(zipf.MaxDependents,
                Is.GreaterThan(5 * uniform.MaxDependents));
    Assert.That(zipf.DependencyEdges,
                Is.EqualTo(uniform.DependencyEdges));
  }

  [Test]
  public void LocalShouldChooseNeighbours()
  {
    var settings = Settings(graphTasks: "const:20",
                            levels: 2,
                            tasksPerLevel: "1,1",
                            dependenciesPerTask: "const:1",
                            parentChoice: "local");
    var graph = GraphGenerator.GenerateRoot(settings,
                                            20);
    // Same number of tasks on both levels: each task depends on the task at the same index
    Assert.That(graph.Tasks.Where(t => t.Level == 1)
                     .Select(t => graph.Tasks[t.Dependencies.Single()].Index),
                Is.EqualTo(Enumerable.Range(0,
                                            10)));
  }

  private static string DescribeWithoutPosition(GraphTask task)
    => $"{task.PositionHash} {task.Level}.{task.Index} {task.Budget} {task.DurationMs} {task.OutputSize} <- {string.Join(",", task.Dependencies)}";

  private static string Describe(TaskGraph graph)
    => string.Join("\n",
                   graph.Tasks.Select(t => $"{t.Position} {t.Budget} {t.DurationMs} {t.OutputSize} <- {string.Join(",", t.Dependencies)}"));
}
