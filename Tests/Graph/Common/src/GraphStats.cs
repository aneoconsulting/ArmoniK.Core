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

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Statistics of a full graph: the client graph and all its nested sub-graphs
/// </summary>
/// <param name="TotalTasks">Number of tasks of the session, should be NTasks</param>
/// <param name="Graphs">Number of graphs: the client graph plus one sub-graph per expanding task</param>
/// <param name="ExpandingTasks">Number of tasks with a budget greater than 1, that submit a sub-graph</param>
/// <param name="ClientTasks">Number of tasks submitted by the client</param>
/// <param name="ClientLevels">Number of levels of the client graph</param>
/// <param name="DependencyEdges">Number of data dependencies, all tasks included</param>
/// <param name="MaxDependencies">Largest number of data dependencies of a task</param>
/// <param name="MaxDependents">Largest number of tasks waiting on the same output</param>
/// <param name="NestingDepth">Deepest nesting of sub-graphs, 0 when no task expands</param>
/// <param name="CriticalPathTasks">Number of tasks of the longest chain of tasks that must run one after the other</param>
/// <param name="MinimumDurationMs">
///   Theoretical minimum duration of the session with infinite resources and no overhead: the longest path in
///   duration, where the output of an expanding task is ready after its own duration and the critical path of its
///   sub-graph
/// </param>
/// <param name="TotalDurationMs">Sum of the durations of all the tasks</param>
/// <param name="TotalOutputBytes">Sum of the sizes of the outputs written by the tasks</param>
public sealed record GraphStats(long TotalTasks,
                                long Graphs,
                                long ExpandingTasks,
                                int  ClientTasks,
                                int  ClientLevels,
                                long DependencyEdges,
                                int  MaxDependencies,
                                int  MaxDependents,
                                int  NestingDepth,
                                long CriticalPathTasks,
                                long MinimumDurationMs,
                                long TotalDurationMs,
                                long TotalOutputBytes)
{
  /// <summary>
  ///   Generates the whole graph (client graph and every nested sub-graph) and computes its statistics
  /// </summary>
  /// <param name="settings">Graph settings</param>
  /// <param name="nTasks">Total number of tasks of the session</param>
  /// <returns>
  ///   The statistics of the full graph
  /// </returns>
  public static GraphStats Compute(GraphSettings settings,
                                   int           nTasks)
    => Compute(settings,
               GraphGenerator.GenerateRoot(settings,
                                           nTasks));

  /// <summary>
  ///   Generates every nested sub-graph of <paramref name="root" /> and computes the statistics of the full graph
  /// </summary>
  /// <param name="settings">Graph settings</param>
  /// <param name="root">Client graph</param>
  /// <returns>
  ///   The statistics of the full graph
  /// </returns>
  public static GraphStats Compute(GraphSettings settings,
                                   TaskGraph     root)
  {
    var  totalTasks       = 0L;
    var  graphs           = 0L;
    var  expanding        = 0L;
    var  edges            = 0L;
    var  maxDependencies  = 0;
    var  maxDependents    = 0;
    var  nestingDepth     = 0;
    var  totalDurationMs  = 0L;
    var  totalOutputBytes = 0L;
    Path rootPath         = default;

    // Explicit stack instead of recursion: the nesting can be as deep as NTasks (e.g. GraphTasks=const:1).
    // Only the graphs on the path from the client graph to the current one are in memory, and they are disjoint, so
    // the memory is bounded by O(NTasks) small tasks (positions are not built, see GenerateSubGraphWithoutPositions).
    var stack = new Stack<Frame>();
    stack.Push(new Frame(root,
                         0));

    while (stack.Count > 0)
    {
      var frame = stack.Peek();

      // Go down into the next expanding task whose sub-graph has not been analysed yet
      var tasks = frame.Graph.Tasks;
      while (frame.Next < tasks.Count && !tasks[frame.Next].IsExpanding)
      {
        frame.Next++;
      }

      if (frame.Next < tasks.Count)
      {
        var task = tasks[frame.Next];
        frame.Next++;
        // Positions are not built: they grow with the depth and would make the memory quadratic in the depth
        stack.Push(new Frame(GraphGenerator.GenerateSubGraphWithoutPositions(settings,
                                                                             task),
                             frame.Depth + 1)
                   {
                     ParentTask = task.Id,
                   });
        continue;
      }

      // Every sub-graph of this graph has been analysed
      stack.Pop();

      graphs++;
      nestingDepth = Math.Max(nestingDepth,
                              frame.Depth);
      var dependents = new int[tasks.Count];
      foreach (var task in tasks)
      {
        totalTasks++;
        totalDurationMs += task.DurationMs;
        edges           += task.Dependencies.Count;
        maxDependencies =  Math.Max(maxDependencies,
                                    task.Dependencies.Count);
        foreach (var dependency in task.Dependencies)
        {
          dependents[dependency]++;
        }

        if (task.IsExpanding)
        {
          expanding++;
        }
        else
        {
          totalOutputBytes += task.OutputSize;
        }
      }

      if (dependents.Length > 0)
      {
        maxDependents = Math.Max(maxDependents,
                                 dependents.Max());
      }

      var path = LongestPaths(frame.Graph,
                              frame.SubGraphPaths);

      if (stack.Count == 0)
      {
        rootPath = path;
      }
      else
      {
        var parent = stack.Peek();
        parent.SubGraphPaths ??= new Dictionary<int, Path>();
        parent.SubGraphPaths[frame.ParentTask] = path;
      }
    }

    return new GraphStats(totalTasks,
                          graphs,
                          expanding,
                          root.Tasks.Count,
                          root.LevelSizes.Count,
                          edges,
                          maxDependencies,
                          maxDependents,
                          nestingDepth,
                          rootPath.DoneTasks,
                          rootPath.DoneMs,
                          totalDurationMs,
                          totalOutputBytes);
  }

  /// <summary>
  ///   Longest paths of a graph, relative to the moment its tasks are submitted
  /// </summary>
  /// <param name="graph">The graph</param>
  /// <param name="subGraphs">Longest paths of the sub-graphs of its expanding tasks, by task id</param>
  /// <returns>
  ///   When the output of the last task is ready and when every task (sub-graphs included) is done, in tasks and in ms
  /// </returns>
  private static Path LongestPaths(TaskGraph              graph,
                                   Dictionary<int, Path>? subGraphs)
  {
    var ready = new Path[graph.Tasks.Count];
    var done  = default(Path);

    foreach (var task in graph.Tasks)
    {
      var startTasks = 0L;
      var startMs    = 0L;
      foreach (var dependency in task.Dependencies)
      {
        startTasks = Math.Max(startTasks,
                              ready[dependency].ReadyTasks);
        startMs = Math.Max(startMs,
                           ready[dependency].ReadyMs);
      }

      var endTasks = startTasks + 1;
      var endMs    = startMs    + task.DurationMs;
      var sub = task.IsExpanding
                  ? subGraphs![task.Id]
                  : default;

      ready[task.Id] = new Path(endTasks + sub.ReadyTasks,
                                endMs    + sub.ReadyMs,
                                endTasks + sub.DoneTasks,
                                endMs    + sub.DoneMs);

      done = new Path(0,
                      0,
                      Math.Max(done.DoneTasks,
                               ready[task.Id].DoneTasks),
                      Math.Max(done.DoneMs,
                               ready[task.Id].DoneMs));
    }

    var aggregator = ready[^1];
    return new Path(aggregator.ReadyTasks,
                    aggregator.ReadyMs,
                    done.DoneTasks,
                    done.DoneMs);
  }

  /// <summary>
  ///   Times (in number of tasks and in ms) at which an output is ready and at which everything is done
  /// </summary>
  private readonly record struct Path(long ReadyTasks,
                                      long ReadyMs,
                                      long DoneTasks,
                                      long DoneMs);

  private sealed class Frame
  {
    public Frame(TaskGraph graph,
                 int       depth)
    {
      Graph = graph;
      Depth = depth;
    }

    public TaskGraph Graph { get; }

    public int Depth { get; }

    public int Next { get; set; }

    public int ParentTask { get; init; }

    /// <summary>Longest paths of the sub-graphs already analysed, by task id; created on the first one</summary>
    public Dictionary<int, Path>? SubGraphPaths { get; set; }
  }
}
