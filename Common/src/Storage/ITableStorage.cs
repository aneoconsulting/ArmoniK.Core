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

using ArmoniK.Api.gRPC.V1;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;


namespace ArmoniK.Core.Common.Storage;

public interface ITableStorage : IInitializable
{
  TimeSpan PollingDelay { get; }

  TimeSpan DispatchTimeToLive { get; }

  Task<CreateSessionReply> CreateSessionAsync(CreateSessionRequest sessionRequest, CancellationToken cancellationToken = default);

  Task CancelSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

  Task<bool> IsSessionCancelledAsync(SessionId sessionId, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListSessionsAsync(CancellationToken cancellationToken = default);

  Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default);

  Task<TaskOptions> GetDefaultTaskOption(string sessionId, CancellationToken cancellationToken = default);

  public Task InitializeTaskCreation(string                                                session,
                                     string                                                parentTaskId,
                                     TaskOptions                                           options,
                                     IEnumerable<CreateSmallTaskRequest.Types.TaskRequest> requests,
                                     CancellationToken                                     cancellationToken = default);
  
  Task<ITaskData> ReadTaskAsync(string id, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);

  Task UpdateTaskStatusAsync(string id, TaskStatus status, CancellationToken cancellationToken = default);

  Task<int> UpdateAllTaskStatusAsync(TaskFilter filter, TaskStatus status, CancellationToken cancellationToken = default);

  Task<IEnumerable<(TaskStatus Status, int Count)>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);

  Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default);

  Task<bool> TryAcquireDispatchAsync(string dispatchId, string taskId, DateTime ttl, string podId="", string nodeId = "", CancellationToken cancellationToken = default);

  Task DeleteDispatch(string id, CancellationToken cancellationToken = default);

  Task UpdateDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default);

  Task ExtendDispatchTtl(string id, DateTime newTtl, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListDispatchAsync(string taskId, CancellationToken cancellationToken = default);

  Task<IDispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListResultsAsync(string sessionId, CancellationToken cancellationToken = default);

  Task<IResult> GetResult(string key, CancellationToken cancellationToken = default);

  Task SetResult(string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default);

  Task DeleteResult(string key, CancellationToken cancellationToken = default);

  Task DeleteResults(string sessionId, CancellationToken cancellationToken = default);
}