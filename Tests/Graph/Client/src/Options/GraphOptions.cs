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

using JetBrains.Annotations;

namespace ArmoniK.Samples.Graph.Client.Options;

/// <summary>
///   Options of the graph test client, given through the environment variables <c>GraphOptions__&lt;Name&gt;</c>
/// </summary>
[PublicAPI]
public class GraphOptions
{
  /// <summary>
  ///   Name of the section in dotnet options
  /// </summary>
  public const string SettingSection = nameof(GraphOptions);

  /// <summary>
  ///   Total number of tasks of the session, sub-tasks included
  /// </summary>
  public int NTasks { get; set; } = 100;

  /// <summary>
  ///   Seed of the graph: the same seed with the same options gives the same graph
  /// </summary>
  public long Seed { get; set; } = 1;

  /// <summary>
  ///   Distribution of the number of tasks of a graph (limited by its budget)
  /// </summary>
  public string GraphTasks { get; set; } = "uniform:5:20";

  /// <summary>
  ///   Maximum number of levels of a graph
  /// </summary>
  public int Levels { get; set; } = 3;

  /// <summary>
  ///   Spread of the tasks over the levels: uniform, increasing, decreasing or a comma separated list of weights
  /// </summary>
  public string TasksPerLevel { get; set; } = "uniform";

  /// <summary>
  ///   Distribution of the number of data dependencies of a task that is not on the first level
  /// </summary>
  public string DependenciesPerTask { get; set; } = "uniform:1:4";

  /// <summary>
  ///   Choice of the dependencies inside a level: uniform, zipf:S or local
  /// </summary>
  public string ParentChoice { get; set; } = "uniform";

  /// <summary>
  ///   Probability that a dependency is taken from a uniformly chosen older level instead of the previous one
  /// </summary>
  public double SkipLevels { get; set; }

  /// <summary>
  ///   Distribution of the duration of the tasks in milliseconds
  /// </summary>
  public string TaskDuration { get; set; } = "const:100";

  /// <summary>
  ///   Distribution of the size of the outputs in kilobytes
  /// </summary>
  public string OutputSize { get; set; } = "const:1";

  /// <summary>
  ///   Partition in which to submit the tasks
  /// </summary>
  public string Partition { get; set; } = string.Empty;

  /// <summary>
  ///   Maximum number of retries of a task
  /// </summary>
  public int MaxRetries { get; set; } = 1;

  /// <summary>
  ///   Number of tasks submitted by request
  /// </summary>
  public int BatchSize { get; set; } = 100;

  /// <summary>
  ///   Order of submission of the client graph: reverse (last level first, so that the dependents always wait for
  ///   their dependencies) or natural (first level first)
  /// </summary>
  public string SubmitOrder { get; set; } = "reverse";

  /// <summary>
  ///   Maximum time to wait for the end of the tasks
  /// </summary>
  public TimeSpan Timeout { get; set; } = TimeSpan.FromHours(1);

  /// <summary>
  ///   Purge data from the created session
  /// </summary>
  public bool PurgeData { get; set; } = true;
}
