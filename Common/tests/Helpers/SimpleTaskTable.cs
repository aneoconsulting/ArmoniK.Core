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

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Task = System.Threading.Tasks.Task;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleTaskTable : ITaskTable
{
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => throw new NotImplementedException();

  public Task Init(CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public TimeSpan PollingDelayMin { get; }
  public TimeSpan PollingDelayMax { get; }
  public ILogger  Logger          { get; }

  public Task CreateTasks(IEnumerable<TaskData> tasks,
                          CancellationToken     cancellationToken = default)
    => throw new NotImplementedException();

  public Task<TaskData> ReadTaskAsync(string            taskId,
                                      CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task UpdateTaskStatusAsync(string            id,
                                    TaskStatus        status,
                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                            TaskStatus        status,
                                            CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<bool> IsTaskCancelledAsync(string            taskId,
                                         CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task CancelSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                            CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task DeleteTaskAsync(string            id,
                              CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                                 CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<IEnumerable<TaskData>> ListTasksAsync(ListTasksRequest  request,
                                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task SetTaskSuccessAsync(string            taskId,
                                  CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task SetTaskCanceledAsync(string            taskId,
                                   CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<bool> SetTaskErrorAsync(string            taskId,
                                      string            errorDetail,
                                      CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<Output> GetTaskOutput(string            taskId,
                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<TaskData> AcquireTask(string            taskId,
                                    string            ownerPodId,
                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<TaskData> ReleaseTask(string            taskId,
                                    string            ownerPodId,
                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskIds,
                                                                            CancellationToken   cancellationToken = default)
    => throw new NotImplementedException();

  public IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                              CancellationToken   cancellationToken = default)
    => throw new NotImplementedException();

  public Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                    CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<string> RetryTask(TaskData          taskData,
                                CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  public Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                        CancellationToken   cancellationToken = default)
    => throw new NotImplementedException();
}
