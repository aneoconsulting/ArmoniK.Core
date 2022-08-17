// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.Storage;

public record TaskOptions(IDictionary<string, string> Options,
                          TimeSpan                    MaxDuration,
                          int                         MaxRetries,
                          int                         Priority,
                          string                      PartitionId,
                          string ApplicationName,
                          string ApplicationVersion)
{
  public static implicit operator Api.gRPC.V1.TaskOptions(TaskOptions taskOption)
    => new()
       {
         MaxDuration = Duration.FromTimeSpan(taskOption.MaxDuration),
         MaxRetries  = taskOption.MaxRetries,
         Priority    = taskOption.Priority,
         PartitionId = taskOption.PartitionId,
         Options =
         {
           taskOption.Options,
         },
         ApplicationName = taskOption.ApplicationName,
         ApplicationVersion = taskOption.ApplicationVersion,
       };

  public static implicit operator TaskOptions(Api.gRPC.V1.TaskOptions taskOption)
    => new(taskOption.Options,
           taskOption.MaxDuration.ToTimeSpan(),
           taskOption.MaxRetries,
           taskOption.Priority,
           taskOption.PartitionId,
           taskOption.ApplicationName,
           taskOption.ApplicationVersion);

  public static TaskOptions Merge(TaskOptions taskOption,
                                  TaskOptions defaultOption)
  {
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
                             : defaultOption.ApplicationVersion);
  }
}
