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

using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

public static class TaskLifeCycleHelper
{
  public enum DependenciesStatus
  {
    Aborted,
    Available,
    Processing,
  }

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

    if (dependencies.SingleOrDefault(i => i.Status == ResultStatus.Completed,
                                     new ResultStatusCount(ResultStatus.Completed,
                                                           0))
                    .Count == taskData.DataDependencies.Count)
    {
      return DependenciesStatus.Available;
    }

    return dependencies.SingleOrDefault(i => i.Status == ResultStatus.Aborted,
                                        new ResultStatusCount(ResultStatus.Aborted,
                                                              0))
                       .Count > 0
             ? DependenciesStatus.Aborted
             : DependenciesStatus.Processing;
  }
}
