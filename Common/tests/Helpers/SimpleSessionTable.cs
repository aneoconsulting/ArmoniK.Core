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

public class SimpleSessionTable : ISessionTable
{
  public const string SessionId   = "MySessionId";
  public const string OwnerPodId  = "MyOwnerPodId";
  public const string PayloadId   = "MyPayloadId";
  public const string OutputId    = "MyOutputId";
  public const string TaskId      = "MyTaskId";
  public const string PartitionId = "MyPartitionId";

  public static readonly TaskOptions TaskOptions;

  static SimpleSessionTable()
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

  public ILogger Logger { get; } = new Logger<SimpleSessionTable>(new LoggerFactory());

  public Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                          TaskOptions         defaultOptions,
                                          CancellationToken   cancellationToken = default)
    => Task.FromResult(SessionId);

  public IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>> filter,
                                                  Expression<Func<SessionData, T>>    selector,
                                                  CancellationToken                   cancellationToken = default)
    => new SessionData[]
       {
         new(SessionId,
             SessionStatus.Running,
             true,
             true,
             DateTime.Today.ToUniversalTime(),
             null,
             null,
             null,
             null,
             null,
             null,
             new List<string>
             {
               PartitionId,
             },
             TaskOptions),
       }.Select(selector.Compile())
        .ToAsyncEnumerable();

  public Task DeleteSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                      Expression<Func<SessionData, object?>> orderField,
                                                                                      bool                                   ascOrder,
                                                                                      int                                    page,
                                                                                      int                                    pageSize,
                                                                                      CancellationToken                      cancellationToken = default)
    => Task.FromResult<(IEnumerable<SessionData> sessions, long totalCount)>((new List<SessionData>
                                                                              {
                                                                                new(SessionId,
                                                                                    SessionStatus.Running,
                                                                                    true,
                                                                                    true,
                                                                                    DateTime.Today.ToUniversalTime(),
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    new List<string>
                                                                                    {
                                                                                      PartitionId,
                                                                                    },
                                                                                    TaskOptions),
                                                                              }.AsEnumerable(), 1));

  public Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                  Expression<Func<SessionData, bool>>? filter,
                                                  UpdateDefinition<SessionData>        updates,
                                                  bool                                 before            = false,
                                                  CancellationToken                    cancellationToken = default)
    => Task.FromResult<SessionData?>(new SessionData(SessionId,
                                                     SessionStatus.Running,
                                                     true,
                                                     true,
                                                     DateTime.Today.ToUniversalTime(),
                                                     DateTime.Now.ToUniversalTime(),
                                                     DateTime.Now.ToUniversalTime(),
                                                     DateTime.Now.ToUniversalTime(),
                                                     DateTime.Now.ToUniversalTime(),
                                                     DateTime.Now.ToUniversalTime(),
                                                     TimeSpan.FromDays(1),
                                                     new List<string>
                                                     {
                                                       PartitionId,
                                                     },
                                                     TaskOptions));
}
