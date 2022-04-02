﻿// This file is part of the ArmoniK project
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

using ArmoniK.Api.gRPC.V1;

using Grpc.Core;

using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services
{
  public record TaskRequest(
    string                                 Id,
    IEnumerable<string>                    ExpectedOutputKeys,
    IEnumerable<string>                    DataDependencies,
    IAsyncEnumerable<ReadOnlyMemory<byte>> PayloadChunks
  );

  public interface ISubmitter
  {
    Task CancelSession(string sessionId, CancellationToken cancellationToken);

    Task CancelDispatchSessionAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken);

    Task CancelTasks(TaskFilter request, CancellationToken cancellationToken);

    Task<Count> CountTasks(TaskFilter request, CancellationToken cancellationToken);

    Task<CreateSessionReply> CreateSession(string sessionId, TaskOptions defaultTaskOptions, CancellationToken cancellationToken);

    Task<CreateTaskReply> CreateTasks(string                        sessionId,
                                      string                        parentId,
                                      string                        dispatchId,
                                      TaskOptions                   options,
                                      IAsyncEnumerable<TaskRequest> taskRequests,
                                      CancellationToken             cancellationToken);

    Task<Configuration> GetServiceConfiguration(Empty request, CancellationToken cancellationToken);

    Task TryGetResult(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, CancellationToken cancellationToken);

    Task<Count> WaitForCompletion(WaitRequest request, CancellationToken cancellationToken);

    Task UpdateTaskStatusAsync(string            id,
                               TaskStatus        status,
                               CancellationToken cancellationToken = default);

    Task FinalizeDispatch(string taskId, Dispatch dispatchId, CancellationToken cancellationToken);

    Task CompleteTaskAsync(string id, Output output, CancellationToken cancellationToken = default);

    Task<Output> TryGetTaskOutputAsync(ResultRequest          request, CancellationToken contextCancellationToken);

    Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest request, CancellationToken contextCancellationToken);

    Task<GetStatusReply> GetStatusAsync(GetStatusrequest request, CancellationToken contextCancellationToken);

    Task<TaskIdList> ListTasksAsync(TaskFilter           request, CancellationToken contextCancellationToken);
  }
}