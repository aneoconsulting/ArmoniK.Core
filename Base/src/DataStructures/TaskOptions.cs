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
using System.Collections.Generic;
using System.Linq;

namespace ArmoniK.Core.Base.DataStructures;

/// <summary>
///   Options to set up task execution
/// </summary>
/// <param name="Options">Custom defined options transmitted to the task from the client</param>
/// <param name="MaxDuration">Max duration of the task</param>
/// <param name="MaxRetries">Number of retries allowed to the task</param>
/// <param name="Priority">Priority of the task</param>
/// <param name="PartitionId">Partition in which the task is executed</param>
/// <param name="ApplicationName">Application Name field transmitted to the task from the client</param>
/// <param name="ApplicationVersion">Application Version field transmitted to the task from the client</param>
/// <param name="ApplicationNamespace">Application Namespace field transmitted to the task from the client</param>
/// <param name="ApplicationService">Application Service field transmitted to the task from the client</param>
/// <param name="EngineType">Engine Type field transmitted to the task from the client</param>
public record TaskOptions(IDictionary<string, string> Options,
                          TimeSpan                    MaxDuration,
                          int                         MaxRetries,
                          int                         Priority,
                          string                      PartitionId,
                          string                      ApplicationName,
                          string                      ApplicationVersion,
                          string                      ApplicationNamespace,
                          string                      ApplicationService,
                          string                      EngineType)
{
  /// <summary>
  ///   Creates a default <see cref="TaskOptions" /> with empty data
  /// </summary>
  public TaskOptions()
    : this(new Dictionary<string, string>(),
           TimeSpan.Zero,
           0,
           0,
           "",
           "",
           "",
           "",
           "",
           "")
  {
  }

  /// <inheritdoc />
  public virtual bool Equals(TaskOptions? other)
    => !ReferenceEquals(null,
                        other) && Options.SequenceEqual(other.Options) && MaxDuration.Equals(other.MaxDuration) && MaxRetries.Equals(other.MaxRetries) &&
       Priority.Equals(other.Priority) && PartitionId.Equals(other.PartitionId) && ApplicationName.Equals(other.ApplicationName) &&
       ApplicationVersion.Equals(other.ApplicationVersion) && ApplicationNamespace.Equals(other.ApplicationNamespace) &&
       ApplicationService.Equals(other.ApplicationService) && EngineType.Equals(other.EngineType);

  /// <inheritdoc />
  public override int GetHashCode()
  {
    var hashCode = new HashCode();
    hashCode.Add(Options);
    hashCode.Add(MaxDuration);
    hashCode.Add(MaxRetries);
    hashCode.Add(Priority);
    hashCode.Add(PartitionId);
    hashCode.Add(ApplicationName);
    hashCode.Add(ApplicationVersion);
    hashCode.Add(ApplicationNamespace);
    hashCode.Add(ApplicationService);
    hashCode.Add(EngineType);
    return hashCode.ToHashCode();
  }

  /// <summary>
  ///   Creates new <see cref="TaskOptions" /> based on given task options with the given default values
  /// </summary>
  /// <param name="taskOption">Base options</param>
  /// <param name="defaultOption">Default values</param>
  /// <returns>
  ///   The merged options
  /// </returns>
  public static TaskOptions Merge(TaskOptions? taskOption,
                                  TaskOptions  defaultOption)
  {
    if (taskOption is null)
    {
      return defaultOption;
    }

    var options = new Dictionary<string, string>(defaultOption.Options);
    foreach (var option in taskOption.Options)
    {
      options[option.Key] = option.Value;
    }

    return new TaskOptions(options,
                           taskOption.MaxDuration == TimeSpan.Zero
                             ? taskOption.MaxDuration
                             : defaultOption.MaxDuration,
                           taskOption.MaxRetries == 0
                             ? taskOption.MaxRetries
                             : defaultOption.MaxRetries,
                           taskOption.Priority,
                           taskOption.PartitionId != string.Empty
                             ? taskOption.PartitionId
                             : defaultOption.PartitionId,
                           taskOption.ApplicationName != string.Empty
                             ? taskOption.ApplicationName
                             : defaultOption.ApplicationName,
                           taskOption.ApplicationVersion != string.Empty
                             ? taskOption.ApplicationVersion
                             : defaultOption.ApplicationVersion,
                           taskOption.ApplicationNamespace != string.Empty
                             ? taskOption.ApplicationNamespace
                             : defaultOption.ApplicationNamespace,
                           taskOption.ApplicationService != string.Empty
                             ? taskOption.ApplicationService
                             : defaultOption.ApplicationService,
                           taskOption.EngineType != string.Empty
                             ? taskOption.EngineType
                             : defaultOption.EngineType);
  }
}
