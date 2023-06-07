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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
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

  public Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                                TaskOptions       defaultTaskOptions,
                                                CancellationToken cancellationToken)
    => Task.FromResult(new CreateSessionReply
                       {
                         SessionId = Guid.NewGuid()
                                         .ToString(),
                       });


  public async Task<ICollection<TaskCreationRequest>> CreateTasks(string                        sessionId,
                                                                  string                        parentTaskId,
                                                                  TaskOptions?                  options,
                                                                  IAsyncEnumerable<TaskRequest> taskRequests,
                                                                  CancellationToken             cancellationToken)
    => await taskRequests.Select(r =>
                                 {
                                   var id = Guid.NewGuid()
                                                .ToString();
                                   return new TaskCreationRequest(id,
                                                                  id,
                                                                  options ?? new TaskOptions(new Dictionary<string, string>(),
                                                                                             TimeSpan.FromSeconds(1),
                                                                                             5,
                                                                                             1,
                                                                                             "Partition",
                                                                                             "Application",
                                                                                             "Version",
                                                                                             "Namespace",
                                                                                             "Service",
                                                                                             "Engine"),
                                                                  r.ExpectedOutputKeys.ToList(),
                                                                  r.DataDependencies.ToList());
                                 })
                         .ToArrayAsync(cancellationToken)
                         .ConfigureAwait(false);

  public Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                                   string                           sessionId,
                                   string                           parentTaskId,
                                   CancellationToken                cancellationToken)
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

  public Task CompleteTaskAsync(TaskData          taskData,
                                bool              resubmit,
                                Output            output,
                                CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                          CancellationToken contextCancellationToken)
    => Task.FromResult(new AvailabilityReply
                       {
                         Ok = new Empty(),
                       });

  public Task SetResult(string                                 sessionId,
                        string                                 ownerTaskId,
                        string                                 key,
                        IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                        CancellationToken                      cancellationToken)
    => Task.CompletedTask;
}
