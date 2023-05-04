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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Common.gRPC.Services;

public record TaskRequest(IEnumerable<string>                    ExpectedOutputKeys,
                          IEnumerable<string>                    DataDependencies,
                          IAsyncEnumerable<ReadOnlyMemory<byte>> PayloadChunks);

public interface ISubmitter
{
  Task CancelSession(string            sessionId,
                     CancellationToken cancellationToken);

  Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                         TaskOptions       defaultTaskOptions,
                                         CancellationToken cancellationToken);

  Task<(IEnumerable<TaskCreationRequest> requests, int priority, string partitionId)> CreateTasks(string                        sessionId,
                                                                                                  string                        parentTaskId,
                                                                                                  TaskOptions?                  options,
                                                                                                  IAsyncEnumerable<TaskRequest> taskRequests,
                                                                                                  CancellationToken             cancellationToken);

  Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                            string                           sessionId,
                            string                           parentTaskId,
                            CancellationToken                cancellationToken);

  Task<Configuration> GetServiceConfiguration(Empty             request,
                                              CancellationToken cancellationToken);

  Task TryGetResult(ResultRequest                    request,
                    IServerStreamWriter<ResultReply> responseStream,
                    CancellationToken                cancellationToken);

  Task<Count> WaitForCompletion(WaitRequest       request,
                                CancellationToken cancellationToken);

  Task CompleteTaskAsync(TaskData          taskData,
                         bool              resubmit,
                         Output            output,
                         CancellationToken cancellationToken = default);

  Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                   CancellationToken contextCancellationToken);

  Task SetResult(string                                 sessionId,
                 string                                 ownerTaskId,
                 string                                 key,
                 IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                 CancellationToken                      cancellationToken);
}
