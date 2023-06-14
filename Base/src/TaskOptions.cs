// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

namespace ArmoniK.Core.Base;

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
