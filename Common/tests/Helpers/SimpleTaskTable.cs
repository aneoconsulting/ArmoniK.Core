// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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
  public const           string      CreatedBy   = "CreatedBy";
  public static readonly TaskOptions TaskOptions;

  static SimpleTaskTable()
    => TaskOptions = new TaskOptions(new Dictionary<string, string>(),
                                     TimeSpan.FromSeconds(1),
                                     5,
                                     1,
                                     PartitionId,
                                     "",
                                     "",
                                     "",
                                     "",
                                     "");

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

  public Task<T> ReadTaskAsync<T>(string                        taskId,
                                  Expression<Func<TaskData, T>> selector,
                                  CancellationToken             cancellationToken = default)
    => Task.FromResult(selector.Compile()
                               .Invoke(new TaskData(SessionId,
                                                    taskId,
                                                    OwnerPodId,
                                                    PodName,
                                                    PayloadId,
                                                    CreatedBy,
                                                    new List<string>(),
                                                    new List<string>(),
                                                    new List<string>
                                                    {
                                                      OutputId,
                                                    },
                                                    new List<string>(),
                                                    TaskStatus.Completed,
                                                    TaskOptions,
                                                    new Output(OutputStatus.Success,
                                                               ""))));


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

  public Task DeleteTasksAsync(string            sessionId,
                               CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteTasksAsync(ICollection<string> taskIds,
                               CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  public Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                         Expression<Func<TaskData, object?>> orderField,
                                                                         Expression<Func<TaskData, T>>       selector,
                                                                         bool                                ascOrder,
                                                                         int                                 page,
                                                                         int                                 pageSize,
                                                                         CancellationToken                   cancellationToken = default)
    => Task.FromResult<(IEnumerable<T> tasks, long totalCount)>((new[]
                                                                 {
                                                                   new TaskData(SessionId,
                                                                                TaskId,
                                                                                OwnerPodId,
                                                                                PodName,
                                                                                PayloadId,
                                                                                CreatedBy,
                                                                                new List<string>(),
                                                                                new List<string>(),
                                                                                new List<string>
                                                                                {
                                                                                  OutputId,
                                                                                },
                                                                                new List<string>(),
                                                                                TaskStatus.Completed,
                                                                                TaskOptions,
                                                                                new Output(OutputStatus.Success,
                                                                                           "")),
                                                                 }.Select(selector.Compile()), 1));

  public IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                               Expression<Func<TaskData, T>>    selector,
                                               CancellationToken                cancellationToken = default)
    => new List<TaskData>
       {
         new(SessionId,
             TaskId,
             OwnerPodId,
             PodName,
             PayloadId,
             CreatedBy,
             new List<string>(),
             new List<string>(),
             new List<string>
             {
               OutputId,
             },
             new List<string>(),
             TaskStatus.Completed,
             TaskOptions,
             new Output(OutputStatus.Success,
                        "")),
       }.Where(filter.Compile())
        .Select(selector.Compile())
        .ToAsyncEnumerable();

  public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                    UpdateDefinition<TaskData>       updates,
                                    CancellationToken                cancellationToken = default)
    => Task.FromResult<long>(1);

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

  public IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>           taskIds,
                                                                     ICollection<string>           dependenciesToRemove,
                                                                     Expression<Func<TaskData, T>> selector,
                                                                     CancellationToken             cancellationToken = default)
    => AsyncEnumerable.Empty<T>();

  public Task<TaskData?> UpdateOneTask(string                            taskId,
                                       Expression<Func<TaskData, bool>>? filter,
                                       UpdateDefinition<TaskData>        updates,
                                       bool                              before,
                                       CancellationToken                 cancellationToken = default)
    => Task.FromResult<TaskData?>(new TaskData(SessionId,
                                               taskId,
                                               OwnerPodId,
                                               PodName,
                                               PayloadId,
                                               CreatedBy,
                                               new List<string>(),
                                               new List<string>(),
                                               new List<string>
                                               {
                                                 OutputId,
                                               },
                                               new List<string>(),
                                               TaskStatus.Completed,
                                               TaskOptions,
                                               new Output(OutputStatus.Success,
                                                          "")));
}
