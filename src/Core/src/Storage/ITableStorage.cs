// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface ITableStorage
  {
    TimeSpan PollingDelay { get; }

    Task<SessionId> CreateSessionAsync(SessionOptions sessionOptions, CancellationToken cancellationToken = default);

    Task CloseSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task CancelSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<bool> IsSessionCancelledAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<bool> IsSessionClosedAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task DeleteSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<TaskOptions> GetDefaultTaskOption(SessionId sessionId, CancellationToken cancellationToken = default);

    public Task<IEnumerable<(TaskId id, bool HasPayload, byte[] Payload)>> InitializeTaskCreation(SessionId   session,
                                                                                                  TaskOptions options,
                                                                                                  IEnumerable<TaskRequest>
                                                                                                    requests,
                                                                                                  CancellationToken
                                                                                                    cancellationToken = default);

    Task<TaskData> ReadTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    Task UpdateTaskStatusAsync(TaskId id, TaskStatus status, CancellationToken cancellationToken = default);

    Task<int> UpdateTaskStatusAsync(TaskFilter filter, TaskStatus status, CancellationToken cancellationToken = default);

    Task IncreaseRetryCounterAsync(TaskId id, CancellationToken cancellationToken = default);

    Task DeleteTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    IAsyncEnumerable<TaskId> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);

    Task<int> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);
    Task<int> CountSubTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);
  }
}
