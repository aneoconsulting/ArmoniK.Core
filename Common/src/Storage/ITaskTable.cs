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

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

public interface ITaskTable : IInitializable
{
  TimeSpan PollingDelayMin { get; }
  TimeSpan PollingDelayMax { get; }

  public ILogger Logger { get; }

  Task CreateTasks(IEnumerable<TaskData> tasks,
                   CancellationToken     cancellationToken = default);

  Task<TaskData> ReadTaskAsync(string            taskId,
                               CancellationToken cancellationToken = default);


  Task UpdateTaskStatusAsync(string            id,
                             TaskStatus        status,
                             CancellationToken cancellationToken = default);

  Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                     TaskStatus        status,
                                     CancellationToken cancellationToken = default);

  Task<bool> IsTaskCancelledAsync(string            taskId,
                                  CancellationToken cancellationToken = default);

  Task StartTask(string            taskId,
                 CancellationToken cancellationToken = default);

  Task CancelSessionAsync(string            sessionId,
                          CancellationToken cancellationToken = default);

  Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                     CancellationToken cancellationToken = default);

  Task<int> CountAllTasksAsync(TaskStatus        status,
                               CancellationToken cancellationToken = default);

  Task DeleteTaskAsync(string            id,
                       CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                          CancellationToken cancellationToken);

  Task SetTaskSuccessAsync(string            taskId,
                           CancellationToken cancellationToken);

  Task SetTaskErrorAsync(string            taskId,
                         string            errorDetail,
                         CancellationToken cancellationToken);

  Task<Output> GetTaskOutput(string            taskId,
                             CancellationToken cancellationToken = default);

  Task<bool> AcquireTask(string            taskId,
                         CancellationToken cancellationToken = default);

  Task<TaskStatus> GetTaskStatus(string            taskId,
                                 CancellationToken cancellationToken = default);

  Task<IEnumerable<string>> GetTaskExpectedOutputKeys(string            taskId,
                                                      CancellationToken cancellationToken = default);

  Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                             CancellationToken cancellationToken);

  Task<string> RetryTask(TaskData          taskData,
                         CancellationToken cancellationToken);

  Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                 CancellationToken   cancellationToken = default);

}
