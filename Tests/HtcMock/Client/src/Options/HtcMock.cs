// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Api.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Samples.HtcMock.Client.Options;

/// <summary>
///   Class containing options for HtcMock
/// </summary>
[PublicAPI]
public class HtcMock
{
  /// <summary>
  ///   Name of the section in dotnet options
  /// </summary>
  public const string SettingSection = nameof(HtcMock);

  /// <summary>
  ///   Number of computing tasks (there are some supplementary aggregation tasks)
  /// </summary>
  public int NTasks { get; set; } = 100;

  /// <summary>
  ///   Total computation time for the computing tasks
  /// </summary>
  public TimeSpan TotalCalculationTime { get; set; } = TimeSpan.FromMilliseconds(100);

  /// <summary>
  ///   Size of the task outputs
  /// </summary>
  public int DataSize { get; set; } = 1;

  /// <summary>
  ///   Size of the memory used by the task during its execution
  /// </summary>
  public int MemorySize { get; set; } = 1;

  /// <summary>
  ///   Number of sub tasks levels
  /// </summary>
  public int SubTasksLevels { get; set; } = 4;

  /// <summary>
  ///   Whether the computing tasks will take the time parameter into consideration.
  ///   Always used to generate the graph of task dependencies.
  /// </summary>
  public bool EnableFastCompute { get; set; } = true;

  /// <summary>
  ///   Whether the computing tasks will take the memory usage parameter into consideration.
  ///   Always used to generate the graph of task dependencies.
  /// </summary>
  public bool EnableUseLowMem { get; set; } = true;

  /// <summary>
  ///   Whether the computing tasks will take the output size parameter into consideration.
  ///   Always used to generate the graph of task dependencies.
  /// </summary>
  public bool EnableSmallOutput { get; set; } = true;

  /// <summary>
  ///   Raise RpcException when task id ends by this string, ignored if empty string
  /// </summary>
  public string TaskRpcException { get; set; } = string.Empty;

  /// <summary>
  ///   Finish task with Output of type <see cref="Output.TypeOneofCase.Error" /> when task id ends by this string, ignored
  ///   if empty string
  /// </summary>
  public string TaskError { get; set; } = string.Empty;

  /// <summary>
  ///   Partition in which to submit the tasks
  /// </summary>
  public string Partition { get; set; } = string.Empty;
}
