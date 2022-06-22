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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

public record TaskRequest(string                                 Id,
                          IEnumerable<string>                    ExpectedOutputKeys,
                          IEnumerable<string>                    DataDependencies,
                          IAsyncEnumerable<ReadOnlyMemory<byte>> PayloadChunks);

public interface ISubmitter
{
  Task CancelSession(string            sessionId,
                     CancellationToken cancellationToken);

  Task CancelTasks(TaskFilter        request,
                   CancellationToken cancellationToken);

  Task<Count> CountTasks(TaskFilter        request,
                         CancellationToken cancellationToken);

  Task<CreateSessionReply> CreateSession(string            sessionId,
                                         TaskOptions       defaultTaskOptions,
                                         CancellationToken cancellationToken);

  Task<(IEnumerable<Storage.TaskRequest> requests, int priority)> CreateTasks(string                        sessionId,
                                                                              string                        parentTaskId,
                                                                              TaskOptions                   options,
                                                                              IAsyncEnumerable<TaskRequest> taskRequests,
                                                                              CancellationToken             cancellationToken);

  Task FinalizeTaskCreation(IEnumerable<Storage.TaskRequest> requests,
                            int                              priority,
                            string                           sessionId,
                            string                           parentTaskId,
                            CancellationToken                cancellationToken);

  Task StartTask(string            taskId,
                 CancellationToken cancellationToken = default);

  Task<Configuration> GetServiceConfiguration(Empty             request,
                                              CancellationToken cancellationToken);

  Task TryGetResult(ResultRequest                    request,
                    IServerStreamWriter<ResultReply> responseStream,
                    CancellationToken                cancellationToken);

  Task<Count> WaitForCompletion(WaitRequest       request,
                                CancellationToken cancellationToken);

  Task UpdateTaskStatusAsync(string            id,
                             TaskStatus        status,
                             CancellationToken cancellationToken = default);

  Task CompleteTaskAsync(TaskData          taskData,
                         bool              resubmit,
                         Output            output,
                         CancellationToken cancellationToken = default);

  Task<Output> TryGetTaskOutputAsync(ResultRequest     request,
                                     CancellationToken contextCancellationToken);

  Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                   CancellationToken contextCancellationToken);

  Task<GetTaskStatusReply> GetTaskStatusAsync(GetTaskStatusRequest request,
                                              CancellationToken    contextCancellationToken);

  Task<GetResultStatusReply> GetResultStatusAsync(GetResultStatusRequest request,
                                                  CancellationToken      contextCancellationToken);

  Task<TaskIdList> ListTasksAsync(TaskFilter        request,
                                  CancellationToken contextCancellationToken);

  Task<SessionIdList> ListSessionsAsync(SessionFilter     request,
                                        CancellationToken contextCancellationToken);

  Task SetResult(string                                 sessionId,
                 string                                 ownerTaskId,
                 string                                 key,
                 IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                 CancellationToken                      cancellationToken);
}
