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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

namespace ArmoniK.Core.Standalone;

/// <summary>
///   Reaches the agents running in the same process directly, instead of over HTTP: it reads the tasks they process,
///   and asks them to stop the tasks that are cancelled.
/// </summary>
public sealed class InProcessAgents : ITaskProcessingChecker, ICancelledTaskNotifier
{
  private readonly ConcurrentDictionary<string, Pollster> pollsters_ = new();

  /// <inheritdoc />
  public Task NotifyAsync(string            ownerPodId,
                          CancellationToken cancellationToken)
    => pollsters_.TryGetValue(ownerPodId,
                              out var pollster)
         ? pollster.StopCancelledTask()
         : Task.CompletedTask;

  /// <inheritdoc />
  public Task<IReadOnlyCollection<string>> GetProcessingMessageIds(string            taskId,
                                                                   string            ownerPodId,
                                                                   CancellationToken cancellationToken)
    => Task.FromResult<IReadOnlyCollection<string>>(pollsters_.TryGetValue(ownerPodId,
                                                                           out var pollster) && pollster.TaskProcessing.TryGetValue(taskId,
                                                                                                                                    out var messageIds)
                                                      ? messageIds
                                                      : []);

  /// <summary>
  ///   Register the pollster of an agent, replacing the previous one with the same identifier
  /// </summary>
  /// <param name="ownerPodId">Identifier of the agent, as recorded in the tasks it owns</param>
  /// <param name="pollster">Pollster of the agent</param>
  public void Register(string   ownerPodId,
                       Pollster pollster)
    => pollsters_[ownerPodId] = pollster;
}
