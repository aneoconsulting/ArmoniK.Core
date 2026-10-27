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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

/// <summary>
///   Checker used to determine if tasks are running on other pods
/// </summary>
public interface ITaskProcessingChecker
{
  /// <summary>
  ///   Get the queue messages from which the task is running on the given pod
  /// </summary>
  /// <param name="taskId">id of the task to check</param>
  /// <param name="ownerPodId">Id of the pod which should execute the task</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The ids of the messages from which the task is running on the pod, empty if the task is not running on the pod
  ///   or if the pod cannot be reached
  /// </returns>
  Task<IReadOnlyCollection<string>> GetProcessingMessageIds(string            taskId,
                                                            string            ownerPodId,
                                                            CancellationToken cancellationToken);
}
