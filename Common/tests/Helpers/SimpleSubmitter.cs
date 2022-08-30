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
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.Storage.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleSubmitter : ISubmitter
{
  public Count DefaultCount;

  public SimpleSubmitter()
  {
    DefaultCount = new Count();
    var statuscount = new StatusCount
                      {
                        Count  = 1,
                        Status = TaskStatus.Completed,
                      };
    DefaultCount.Values.Add(statuscount);
  }

  public Task CancelSession(string            sessionId,
                            CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task CancelTasks(TaskFilter        request,
                          CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<Count> CountTasks(TaskFilter        request,
                                CancellationToken cancellationToken)
    => Task.FromResult(DefaultCount);

  public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                TaskOptions       defaultTaskOptions,
                                                CancellationToken cancellationToken)
    => Task.FromResult(new CreateSessionReply
                       {
                         SessionId = Guid.NewGuid()
                                         .ToString(),
                       });


  public async Task<(IEnumerable<TaskRequest> requests, int priority, string partitionId)> CreateTasks(string                                      sessionId,
                                                                                                       string                                      parentTaskId,
                                                                                                       TaskOptions                                 options,
                                                                                                       IAsyncEnumerable<gRPC.Services.TaskRequest> taskRequests,
                                                                                                       CancellationToken                           cancellationToken)
    => (await taskRequests.Select(r => new TaskRequest(Guid.NewGuid()
                                                           .ToString(),
                                                       r.ExpectedOutputKeys,
                                                       r.DataDependencies))
                          .ToArrayAsync(cancellationToken)
                          .ConfigureAwait(false), 1, "");

  public Task FinalizeTaskCreation(IEnumerable<TaskRequest> requests,
                                   int                      priority,
                                   string                   partitionId,
                                   string                   sessionId,
                                   string                   parentTaskId,
                                   CancellationToken        cancellationToken)
    => Task.CompletedTask;

  public Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<Configuration> GetServiceConfiguration(Empty             request,
                                                     CancellationToken cancellationToken)
    => Task.FromResult(new Configuration
                       {
                         DataChunkMaxSize = 80000,
                       });

  public Task TryGetResult(ResultRequest                    request,
                           IServerStreamWriter<ResultReply> responseStream,
                           CancellationToken                cancellationToken)
    => Task.CompletedTask;

  public Task<Count> WaitForCompletion(WaitRequest       request,
                                       CancellationToken cancellationToken)
    => Task.FromResult(DefaultCount);

  public Task UpdateTaskStatusAsync(string            id,
                                    TaskStatus        status,
                                    CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task CompleteTaskAsync(TaskData          taskData,
                                bool              resubmit,
                                Output            output,
                                CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<Output> TryGetTaskOutputAsync(TaskOutputRequest request,
                                            CancellationToken contextCancellationToken)
    => Task.FromResult(new Output
                       {
                         Ok = new Empty(),
                       });

  public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                          CancellationToken contextCancellationToken)
    => Task.FromResult(new AvailabilityReply
                       {
                         Ok = new Empty(),
                       });

  public Task<GetTaskStatusReply> GetTaskStatusAsync(GetTaskStatusRequest request,
                                                     CancellationToken    contextCancellationToken)
    => Task.FromResult(request.TaskIds.Aggregate(new GetTaskStatusReply(),
                                                 (tsr,
                                                  id) =>
                                                 {
                                                   tsr.IdStatuses.Add(new GetTaskStatusReply.Types.IdStatus
                                                                      {
                                                                        Status = TaskStatus.Completed,
                                                                        TaskId = id,
                                                                      });
                                                   return tsr;
                                                 }));

  public Task<GetResultStatusReply> GetResultStatusAsync(GetResultStatusRequest request,
                                                         CancellationToken      contextCancellationToken)
    => Task.FromResult(request.ResultIds.Aggregate(new GetResultStatusReply(),
                                                   (reply,
                                                    s) =>
                                                   {
                                                     reply.IdStatuses.Add(new GetResultStatusReply.Types.IdStatus
                                                                          {
                                                                            ResultId = s,
                                                                            Status   = ResultStatus.Completed,
                                                                          });
                                                     return reply;
                                                   }));

  public Task<TaskIdList> ListTasksAsync(TaskFilter        request,
                                         CancellationToken contextCancellationToken)
  {
    var reply = new TaskIdList();
    reply.TaskIds.Add("taskId1");
    return Task.FromResult(reply);
  }

  public Task<SessionIdList> ListSessionsAsync(SessionFilter     request,
                                               CancellationToken contextCancellationToken)
  {
    var reply = new SessionIdList();
    reply.SessionIds.Add("sessionId1");
    return Task.FromResult(reply);
  }

  public Task SetResult(string                                 sessionId,
                        string                                 ownerTaskId,
                        string                                 key,
                        IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                        CancellationToken                      cancellationToken)
    => Task.CompletedTask;
}
