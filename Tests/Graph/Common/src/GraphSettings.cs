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
using System.Globalization;

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Settings that define the shape of every graph (the client graph and all the sub-graphs).
/// </summary>
/// <remarks>
///   They are stored in the default task options of the session (see <see cref="ToOptions" />) so that the workers,
///   whose sub-tasks inherit the session options, generate the sub-graphs with exactly the same settings as the client.
/// </remarks>
/// <param name="Seed">Seed of the whole graph</param>
/// <param name="GraphTasks">Number of tasks of a graph, before being limited by its budget</param>
/// <param name="Levels">Maximum number of levels of a graph</param>
/// <param name="TasksPerLevel">How the tasks are spread over the levels</param>
/// <param name="DependenciesPerTask">Number of data dependencies of a task that is not on the first level</param>
/// <param name="ParentChoice">How the dependencies are chosen inside a level</param>
/// <param name="SkipLevels">Probability that a dependency is taken from an older level than the previous one</param>
/// <param name="TaskDuration">Duration of a task in milliseconds</param>
/// <param name="OutputSize">Size of the output of a task in kilobytes</param>
public sealed record GraphSettings(ulong        Seed,
                                   Distribution GraphTasks,
                                   int          Levels,
                                   LevelShape   TasksPerLevel,
                                   Distribution DependenciesPerTask,
                                   ParentChoice ParentChoice,
                                   double       SkipLevels,
                                   Distribution TaskDuration,
                                   Distribution OutputSize)
{
  /// <summary>Key of <see cref="Seed" /> in the task options</summary>
  public const string SeedKey = "GraphSeed";

  /// <summary>Key of <see cref="GraphTasks" /> in the task options</summary>
  public const string GraphTasksKey = "GraphTasks";

  /// <summary>Key of <see cref="Levels" /> in the task options</summary>
  public const string LevelsKey = "GraphLevels";

  /// <summary>Key of <see cref="TasksPerLevel" /> in the task options</summary>
  public const string TasksPerLevelKey = "GraphTasksPerLevel";

  /// <summary>Key of <see cref="DependenciesPerTask" /> in the task options</summary>
  public const string DependenciesPerTaskKey = "GraphDependenciesPerTask";

  /// <summary>Key of <see cref="ParentChoice" /> in the task options</summary>
  public const string ParentChoiceKey = "GraphParentChoice";

  /// <summary>Key of <see cref="SkipLevels" /> in the task options</summary>
  public const string SkipLevelsKey = "GraphSkipLevels";

  /// <summary>Key of <see cref="TaskDuration" /> in the task options</summary>
  public const string TaskDurationKey = "GraphTaskDuration";

  /// <summary>Key of <see cref="OutputSize" /> in the task options</summary>
  public const string OutputSizeKey = "GraphOutputSize";

  /// <summary>
  ///   Parses and validates the settings
  /// </summary>
  /// <param name="seed">Seed of the graph</param>
  /// <param name="graphTasks">Distribution of the number of tasks of a graph</param>
  /// <param name="levels">Maximum number of levels of a graph</param>
  /// <param name="tasksPerLevel">Shape of the levels</param>
  /// <param name="dependenciesPerTask">Distribution of the number of dependencies of a task</param>
  /// <param name="parentChoice">Choice of the dependencies inside a level</param>
  /// <param name="skipLevels">Probability to take a dependency from an older level</param>
  /// <param name="taskDuration">Distribution of the durations in milliseconds</param>
  /// <param name="outputSize">Distribution of the output sizes in kilobytes</param>
  /// <returns>
  ///   The validated settings
  /// </returns>
  /// <exception cref="FormatException">A setting is invalid</exception>
  public static GraphSettings Parse(long   seed,
                                    string graphTasks,
                                    int    levels,
                                    string tasksPerLevel,
                                    string dependenciesPerTask,
                                    string parentChoice,
                                    double skipLevels,
                                    string taskDuration,
                                    string outputSize)
  {
    if (levels < 1)
    {
      throw new FormatException($"Levels should be at least 1, got {levels}");
    }

    if (!(skipLevels is >= 0 and <= 1))
    {
      throw new FormatException($"SkipLevels should be a probability between 0 and 1, got {skipLevels}");
    }

    return new GraphSettings(unchecked((ulong)seed),
                             Distribution.Parse(graphTasks),
                             levels,
                             LevelShape.Parse(tasksPerLevel),
                             Distribution.Parse(dependenciesPerTask),
                             Common.ParentChoice.Parse(parentChoice),
                             skipLevels,
                             Distribution.Parse(taskDuration),
                             Distribution.Parse(outputSize));
  }

  /// <summary>
  ///   Reads the settings from task options written by <see cref="ToOptions" />
  /// </summary>
  /// <param name="options">Task options</param>
  /// <returns>
  ///   The settings
  /// </returns>
  /// <exception cref="InvalidOperationException">A setting is missing</exception>
  /// <exception cref="FormatException">A setting is invalid</exception>
  public static GraphSettings FromOptions(IDictionary<string, string> options)
  {
    ArgumentNullException.ThrowIfNull(options);

    string Get(string key)
      => options.TryGetValue(key,
                             out var value)
           ? value
           : throw new InvalidOperationException($"{key} should be defined in task options");

    return Parse(long.Parse(Get(SeedKey),
                            CultureInfo.InvariantCulture),
                 Get(GraphTasksKey),
                 int.Parse(Get(LevelsKey),
                           CultureInfo.InvariantCulture),
                 Get(TasksPerLevelKey),
                 Get(DependenciesPerTaskKey),
                 Get(ParentChoiceKey),
                 double.Parse(Get(SkipLevelsKey),
                              CultureInfo.InvariantCulture),
                 Get(TaskDurationKey),
                 Get(OutputSizeKey));
  }

  /// <summary>
  ///   Writes the settings into task options
  /// </summary>
  /// <returns>
  ///   The task options holding the settings
  /// </returns>
  public Dictionary<string, string> ToOptions()
    => new()
       {
         {
           SeedKey, unchecked((long)Seed).ToString(CultureInfo.InvariantCulture)
         },
         {
           GraphTasksKey, GraphTasks.ToString()
         },
         {
           LevelsKey, Levels.ToString(CultureInfo.InvariantCulture)
         },
         {
           TasksPerLevelKey, TasksPerLevel.ToString()
         },
         {
           DependenciesPerTaskKey, DependenciesPerTask.ToString()
         },
         {
           ParentChoiceKey, ParentChoice.ToString()
         },
         {
           SkipLevelsKey, SkipLevels.ToString("R",
                                              CultureInfo.InvariantCulture)
         },
         {
           TaskDurationKey, TaskDuration.ToString()
         },
         {
           OutputSizeKey, OutputSize.ToString()
         },
       };
}
