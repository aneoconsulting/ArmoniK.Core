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
 public record TaskRequest(
  string               Id,
  IEnumerable<string>  ExpectedOutputKeys,
  IEnumerable<string>  DataDependencies,
  ReadOnlyMemory<byte>? PayloadChunk
 );

  TimeSpan PollingDelay { get; }

  TimeSpan DispatchTimeToLiveDuration { get; }

  TimeSpan DispatchRefreshPeriod      { get; }
  
  Task CreateSessionAsync(string id, TaskOptions defaultOptions, CancellationToken cancellationToken = default);
 
  Task CreateDispatchedSessionAsync(string rootSessionId, string parentTaskId, string dispatchId, CancellationToken cancellationToken = default);
 
  Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default);

  Task CancelDispatchAsync(string dispatchId, CancellationToken cancellationToken = default);

  Task<bool> IsSessionCancelledAsync(string sessionId, CancellationToken cancellationToken = default);

  Task<bool> IsDispatchCancelledAsync(string sessionId, string dispatchId, CancellationToken cancellationToken = default);

  Task<bool> IsTaskCancelledAsync(string taskId, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListSessionsAsync(CancellationToken cancellationToken = default);

  Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default);

  Task<TaskOptions> GetDefaultTaskOptionAsync(string sessionId, CancellationToken cancellationToken = default);

  public Task InitializeTaskCreationAsync(string                   session,
                                          string                   parentTaskId,
                                          string                   dispatchId,
                                          TaskOptions              options,
                                          IEnumerable<TaskRequest> requests,
                                          CancellationToken        cancellationToken = default);
  
  Task<ITaskData> ReadTaskAsync(string id, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);

  Task UpdateTaskStatusAsync(string id, TaskStatus status, CancellationToken cancellationToken = default);

  Task<int> UpdateAllTaskStatusAsync(TaskFilter filter, TaskStatus status, CancellationToken cancellationToken = default);

  Task<IEnumerable<(TaskStatus Status, int Count)>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);

  Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default);

  Task<bool> TryAcquireDispatchAsync(string            sessionId,
                                     string            taskId,
                                     string            dispatchId,
                                     IDictionary<string, string>    metadata, 
                                     CancellationToken cancellationToken = default);

  Task DeleteDispatch(string id, CancellationToken cancellationToken = default);

  Task AddStatusToDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default);

  Task ExtendDispatchTtl(string id, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListDispatchAsync(string taskId, CancellationToken cancellationToken = default);

  Task<IDispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListResultsAsync(string sessionId, CancellationToken cancellationToken = default);

  Task<IResult> GetResult(string sessionId, string key, CancellationToken cancellationToken = default);

  Task<bool> AreResultsAvailableAsync(string sessionId, IEnumerable<string> keys, CancellationToken cancellationToken = default);

  Task SetResult(string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default);

  Task DeleteResult(string session, string key, CancellationToken cancellationToken = default);

  Task DeleteResults(string sessionId, CancellationToken cancellationToken = default);

  Task<string> GetDispatchId(string      taskId,        CancellationToken cancellationToken = default);

  Task ChangeTaskDispatch(string   oldDispatchId, string targetDispatchId, CancellationToken cancellationToken);

  Task ChangeResultDispatch(string oldDispatchId,  string            targetDispatchId, CancellationToken cancellationToken);
}