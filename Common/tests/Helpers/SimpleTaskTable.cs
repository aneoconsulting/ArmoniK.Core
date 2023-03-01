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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using LinqKit;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleTaskTable : ITaskTable
{
  public const           string      SessionId   = "MySessionId";
  public const           string      OwnerPodId  = "MyOwnerPodId";
  public const           string      PayloadId   = "MyPayloadId";
  public const           string      OutputId    = "MyOutputId";
  public const           string      TaskId      = "MyTaskId";
  public const           string      PartitionId = "MyPartitionId";
  public const           string      PodName     = "MyPodName";
  public static readonly TaskOptions TaskOptions;

  static SimpleTaskTable()
    => TaskOptions = new TaskOptions
                     {
                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                       MaxRetries  = 4,
                       Priority    = 2,
                       PartitionId = PartitionId,
                     };

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public TimeSpan PollingDelayMin { get; } = TimeSpan.FromSeconds(1);
  public TimeSpan PollingDelayMax { get; } = TimeSpan.FromSeconds(2);
  public ILogger  Logger          { get; } = new Logger<SimpleTaskTable>(new LoggerFactory());

  public Task CreateTasks(IEnumerable<TaskData> tasks,
                          CancellationToken     cancellationToken = default)
    => Task.CompletedTask;

  public Task<TaskData> ReadTaskAsync(string            taskId,
                                      CancellationToken cancellationToken = default)
    => Task.FromResult(new TaskData(SessionId,
                                    taskId,
                                    OwnerPodId,
                                    PodName,
                                    PayloadId,
                                    new List<string>(),
                                    new List<string>(),
                                    new List<string>
                                    {
                                      OutputId,
                                    },
                                    new List<string>(),
                                    TaskStatus.Completed,
                                    TaskOptions,
                                    new Output(true,
                                               "")));

  public Task UpdateTaskStatusAsync(string            id,
                                    TaskStatus        status,
                                    CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                            TaskStatus        status,
                                            CancellationToken cancellationToken = default)
    => Task.FromResult(42);

  public Task<bool> IsTaskCancelledAsync(string            taskId,
                                         CancellationToken cancellationToken = default)
    => Task.FromResult(false);

  public Task StartTask(string            taskId,
                        CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task CancelSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<IList<TaskData>> CancelTaskAsync(ICollection<string> taskIds,
                                               CancellationToken   cancellationToken = default)
    => Task.FromResult<IList<TaskData>>(taskIds.Select(s => new TaskData(SessionId,
                                                                         s,
                                                                         OwnerPodId,
                                                                         PodName,
                                                                         PayloadId,
                                                                         new List<string>(),
                                                                         new List<string>(),
                                                                         new List<string>(),
                                                                         new List<string>(),
                                                                         TaskStatus.Cancelled,
                                                                         TaskOptions,
                                                                         new Output(false,
                                                                                    "Cancelled")))
                                               .ToList());

  public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                            CancellationToken cancellationToken = default)
    => Task.FromResult<IEnumerable<TaskStatusCount>>(new List<TaskStatusCount>
                                                     {
                                                       new(TaskStatus.Completed,
                                                           42),
                                                     });

  public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                            CancellationToken                cancellationToken = default)
    => Task.FromResult<IEnumerable<TaskStatusCount>>(new List<TaskStatusCount>
                                                     {
                                                       new(TaskStatus.Completed,
                                                           42),
                                                     });

  public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
    => Task.FromResult<IEnumerable<PartitionTaskStatusCount>>(new List<PartitionTaskStatusCount>
                                                              {
                                                                new(PartitionId,
                                                                    TaskStatus.Completed,
                                                                    42),
                                                              });

  public Task<int> CountAllTasksAsync(TaskStatus        status,
                                      CancellationToken cancellationToken = default)
    => Task.FromResult(42);

  public Task DeleteTaskAsync(string            id,
                              CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                                 CancellationToken cancellationToken = default)
    => new List<string>
       {
         TaskId,
       }.ToAsyncEnumerable();

  public Task<(IEnumerable<TaskData> tasks, int totalCount)> ListTasksAsync(Expression<Func<TaskData, bool>>    filter,
                                                                            Expression<Func<TaskData, object?>> orderField,
                                                                            bool                                ascOrder,
                                                                            int                                 page,
                                                                            int                                 pageSize,
                                                                            CancellationToken                   cancellationToken = default)
    => Task.FromResult<(IEnumerable<TaskData> tasks, int totalCount)>((new[]
                                                                       {
                                                                         new TaskData(SessionId,
                                                                                      TaskId,
                                                                                      OwnerPodId,
                                                                                      PodName,
                                                                                      PayloadId,
                                                                                      new List<string>(),
                                                                                      new List<string>(),
                                                                                      new List<string>
                                                                                      {
                                                                                        OutputId,
                                                                                      },
                                                                                      new List<string>(),
                                                                                      TaskStatus.Completed,
                                                                                      TaskOptions,
                                                                                      new Output(true,
                                                                                                 "")),
                                                                       }, 1));

  public Task<IEnumerable<T>> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                                Expression<Func<TaskData, T>>    selector,
                                                CancellationToken                cancellationToken = default)
    => Task.FromResult(new List<TaskData>
                       {
                         new(SessionId,
                             TaskId,
                             OwnerPodId,
                             PodName,
                             PayloadId,
                             new List<string>(),
                             new List<string>(),
                             new List<string>
                             {
                               OutputId,
                             },
                             new List<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        "")),
                       }.Select(selector.Invoke));

  public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                             ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                             bool ascOrder,
                                                                                             int page,
                                                                                             int pageSize,
                                                                                             CancellationToken cancellationToken = default)
    => Task.FromResult<(IEnumerable<Application> applications, int totalCount)>((new[]
                                                                                 {
                                                                                   new Application(TaskOptions.ApplicationName,
                                                                                                   TaskOptions.ApplicationNamespace,
                                                                                                   TaskOptions.ApplicationVersion,
                                                                                                   TaskOptions.ApplicationService),
                                                                                 }, 1));

  public Task SetTaskSuccessAsync(string            taskId,
                                  CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task SetTaskCanceledAsync(string            taskId,
                                   CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<bool> SetTaskErrorAsync(string            taskId,
                                      string            errorDetail,
                                      CancellationToken cancellationToken = default)
    => Task.FromResult(false);

  public Task<Output> GetTaskOutput(string            taskId,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(new Output(true,
                                  ""));

  public Task<TaskData> AcquireTask(string            taskId,
                                    string            ownerPodId,
                                    string            ownerPodName,
                                    DateTime          receptionDate,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(new TaskData(SessionId,
                                    taskId,
                                    ownerPodId,
                                    ownerPodName,
                                    PayloadId,
                                    new List<string>(),
                                    new List<string>(),
                                    new List<string>
                                    {
                                      OutputId,
                                    },
                                    new List<string>(),
                                    TaskStatus.Completed,
                                    TaskOptions,
                                    new Output(true,
                                               "")));

  public Task<TaskData> ReleaseTask(string            taskId,
                                    string            ownerPodId,
                                    CancellationToken cancellationToken = default)
    => Task.FromResult(new TaskData(SessionId,
                                    taskId,
                                    ownerPodId,
                                    PodName,
                                    PayloadId,
                                    new List<string>(),
                                    new List<string>(),
                                    new List<string>
                                    {
                                      OutputId,
                                    },
                                    new List<string>(),
                                    TaskStatus.Completed,
                                    TaskOptions,
                                    new Output(true,
                                               "")));

  public Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskIds,
                                                                            CancellationToken   cancellationToken = default)
    => Task.FromResult<IEnumerable<GetTaskStatusReply.Types.IdStatus>>(new List<GetTaskStatusReply.Types.IdStatus>
                                                                       {
                                                                         new(),
                                                                       });

  public IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                              CancellationToken   cancellationToken = default)
    => new List<(string taskId, IEnumerable<string> expectedOutputKeys)>
       {
         (TaskId, new List<string>()),
       }.ToAsyncEnumerable();

  public Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                    CancellationToken cancellationToken = default)
    => Task.FromResult<IEnumerable<string>>(new List<string>
                                            {
                                              TaskId,
                                            });

  public Task<string> RetryTask(TaskData          taskData,
                                CancellationToken cancellationToken = default)
    => Task.FromResult(TaskId);

  public Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                        CancellationToken   cancellationToken = default)
    => Task.FromResult(42);
}
