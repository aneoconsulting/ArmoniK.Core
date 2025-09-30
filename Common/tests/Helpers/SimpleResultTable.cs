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

  public Task Create(ICollection<Result> results,
                     CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteResults(string            sessionId,
                            CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteResults(ICollection<string> results,
                            CancellationToken   cancellationToken = default)
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
             TaskId,
             TaskId,
             ResultStatus.Completed,
             new List<string>(),
             DateTime.Now.ToUniversalTime(),
             DateTime.Now.ToUniversalTime(),
             1,
             new byte[]
             {
               42,
             },
             false),
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
                              TaskId,
                              TaskId,
                              ResultStatus.Completed,
                              new List<string>(),
                              DateTime.UtcNow,
                              DateTime.UtcNow,
                              1,
                              new byte[]
                              {
                                42,
                              },
                              false),
                        }.AsEnumerable(), 1));

  public Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                               CancellationToken                             cancellationToken = default)
    => Task.CompletedTask;

  public Task<Result> UpdateOneResult(string                   resultId,
                                      UpdateDefinition<Result> updates,
                                      CancellationToken        cancellationToken = default)
    => Task.FromResult(new Result(SessionId,
                                  OutputId,
                                  "",
                                  TaskId,
                                  TaskId,
                                  TaskId,
                                  ResultStatus.Completed,
                                  new List<string>(),
                                  DateTime.Now.ToUniversalTime(),
                                  DateTime.Now.ToUniversalTime(),
                                  1,
                                  new byte[]
                                  {
                                    42,
                                  },
                                  false));

  public Task<long> UpdateManyResults(Expression<Func<Result, bool>> filter,
                                      UpdateDefinition<Result>       updates,
                                      CancellationToken              cancellationToken = default)
    => Task.FromResult(0L);

  public Task ChangeResultOwnership(string                                                 oldTaskId,
                                    IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                    CancellationToken                                      cancellationToken)
    => Task.CompletedTask;

  public Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                  CancellationToken                        cancellationToken = default)
    => Task.CompletedTask;

  public Task DeleteResult(string            key,
                           CancellationToken cancellationToken = default)
    => Task.CompletedTask;
}
