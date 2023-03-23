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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Helper to manage Tasks lifecycle
/// </summary>
public static class TaskLifeCycleHelper
{
  /// <summary>
  ///   Represents the status of the dependencies of a task
  /// </summary>
  public enum DependenciesStatus
  {
    /// <summary>
    ///   One of the dependency is aborted
    /// </summary>
    Aborted,

    /// <summary>
    ///   All the dependencies are available
    /// </summary>
    Available,

    /// <summary>
    ///   Some of the dependencies are still being created
    /// </summary>
    Processing,
  }

  /// <summary>
  ///   Check the status of the dependencies from a task
  /// </summary>
  /// <param name="taskData">The metadata of the task for which to check dependencies</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The status of the dependencies
  /// </returns>
  public static async Task<DependenciesStatus> CheckTaskDependencies(TaskData          taskData,
                                                                     IResultTable      resultTable,
                                                                     ILogger           logger,
                                                                     CancellationToken cancellationToken)
  {
    if (!taskData.DataDependencies.Any())
    {
      return DependenciesStatus.Available;
    }

    var dependencies = await resultTable.AreResultsAvailableAsync(taskData.SessionId,
                                                                  taskData.DataDependencies,
                                                                  cancellationToken)
                                        .ConfigureAwait(false);

    var dictionary = dependencies.GroupBy(resultStatusCount => resultStatusCount.Status)
                                 .ToDictionary(counts => counts.Key,
                                               counts => counts.Sum(count => count.Count));

    if (dictionary.GetValueOrDefault(ResultStatus.Completed,
                                     0) == taskData.DataDependencies.Count)
    {
      return DependenciesStatus.Available;
    }

    return dictionary.GetValueOrDefault(ResultStatus.Aborted,
                                        0) > 0
             ? DependenciesStatus.Aborted
             : DependenciesStatus.Processing;
  }
}
