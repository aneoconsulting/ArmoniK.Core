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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Payload of a task: what the task must do. The graph settings are not in the payload but in the task options.
/// </summary>
/// <param name="Position">Logical position of the task, see <see cref="GraphTask.Position" /></param>
/// <param name="Budget">Budget of the task, see <see cref="GraphTask.Budget" /></param>
/// <param name="DurationMs">Time the task sleeps, in milliseconds</param>
/// <param name="OutputSize">
///   Size in bytes of the output of the task (written by the aggregator of its sub-graph when it
///   expands)
/// </param>
/// <param name="Dependencies">Expected size in bytes of each data dependency, by result id</param>
public sealed record TaskPayload(string                   Position,
                                 int                      Budget,
                                 int                      DurationMs,
                                 long                     OutputSize,
                                 Dictionary<string, long> Dependencies)
{
  private static readonly JsonSerializerOptions Options = new()
                                                          {
                                                            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
                                                          };

  /// <summary>
  ///   Serializes the payload
  /// </summary>
  /// <returns>
  ///   The UTF-8 JSON bytes of the payload
  /// </returns>
  public byte[] Serialize()
    => JsonSerializer.SerializeToUtf8Bytes(this,
                                           Options);

  /// <summary>
  ///   Deserializes a payload
  /// </summary>
  /// <param name="data">UTF-8 JSON bytes</param>
  /// <returns>
  ///   The payload
  /// </returns>
  /// <exception cref="JsonException">The data is not a valid payload</exception>
  public static TaskPayload Deserialize(ReadOnlySpan<byte> data)
    => JsonSerializer.Deserialize<TaskPayload>(data,
                                               Options) ?? throw new JsonException("Empty payload");

  /// <summary>
  ///   Builds the payload of a task of a graph
  /// </summary>
  /// <param name="graph">Graph of the task</param>
  /// <param name="task">The task</param>
  /// <param name="resultIds">Result id of the output of each task of the graph, by task id</param>
  /// <returns>
  ///   The payload of the task
  /// </returns>
  public static TaskPayload Create(TaskGraph             graph,
                                   GraphTask             task,
                                   IReadOnlyList<string> resultIds)
  {
    var dependencies = new Dictionary<string, long>(task.Dependencies.Count);
    foreach (var dependency in task.Dependencies)
    {
      dependencies[resultIds[dependency]] = graph.Tasks[dependency].OutputSize;
    }

    return new TaskPayload(task.Position,
                           task.Budget,
                           task.DurationMs,
                           task.OutputSize,
                           dependencies);
  }
}
