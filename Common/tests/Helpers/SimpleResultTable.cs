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

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleResultTable : IResultTable
{
  public const string SessionId   = "MySessionId";
  public const string OwnerPodId  = "MyOwnerPodId";
  public const string PayloadId   = "MyPayloadId";
  public const string OutputId    = "MyOutputId";
  public const string TaskId      = "MyTaskId";
  public const string PartitionId = "MyPartitionId";

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public ILogger Logger { get; } = new Logger<SimpleResultTable>(new LoggerFactory());

  public Task ChangeResultOwnership(string                                                 sessionId,
                                    string                                                 oldTaskId,
                                    IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                    CancellationToken                                      cancellationToken)
    => Task.CompletedTask;

  public Task Create(IEnumerable<Result> results,
                     CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  public Task AddTaskDependency(string              sessionId,
                                ICollection<string> resultIds,
                                ICollection<string> taskIds,
                                CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteResult(string            session,
                           string            key,
                           CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteResults(string            sessionId,
                            CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                           Expression<Func<Result, T>>    convertor,
                                           CancellationToken              cancellationToken = default)
    => new List<Result>
       {
         new(SessionId,
             OutputId,
             "",
             TaskId,
             ResultStatus.Completed,
             new List<string>(),
             DateTime.Now.ToUniversalTime(),
             new byte[]
             {
               42,
             }),
       }.Select(convertor.Compile())
        .ToAsyncEnumerable();

  public Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                              Expression<Func<Result, object?>> orderField,
                                                                              bool                              ascOrder,
                                                                              int                               page,
                                                                              int                               pageSize,
                                                                              CancellationToken                 cancellationToken = default)
    => Task.FromResult((new List<Result>
                        {
                          new(SessionId,
                              "ResultName",
                              "",
                              TaskId,
                              ResultStatus.Completed,
                              new List<string>(),
                              DateTime.UtcNow,
                              new byte[]
                              {
                                42,
                              }),
                        }.AsEnumerable(), 1));

  public Task SetResult(string            sessionId,
                        string            ownerTaskId,
                        string            key,
                        byte[]            smallPayload,
                        CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task SetResult(string            sessionId,
                        string            ownerTaskId,
                        string            key,
                        CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<Result> CompleteResult(string            sessionId,
                                     string            resultId,
                                     CancellationToken cancellationToken = default)
    => Task.FromResult(new Result(SessionId,
                                  OutputId,
                                  "",
                                  TaskId,
                                  ResultStatus.Completed,
                                  new List<string>(),
                                  DateTime.Now.ToUniversalTime(),
                                  new byte[]
                                  {
                                    42,
                                  }));

  public Task SetTaskOwnership(string                                        sessionId,
                               ICollection<(string resultId, string taskId)> requests,
                               CancellationToken                             cancellationToken = default)
    => Task.CompletedTask;

  public Task AbortTaskResults(string            sessionId,
                               string            ownerTaskId,
                               CancellationToken cancellationToken = default)
    => Task.CompletedTask;
}
