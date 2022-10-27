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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleSessionTable : ISessionTable
{
  static SimpleSessionTable()
  {
    TaskOptions = new Api.gRPC.V1.TaskOptions
                  {
                    MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                    MaxRetries  = 4,
                    Priority    = 2,
                    PartitionId = PartitionId,
                  };
  }

  public const string SessionId   = "MySessionId";
  public const string OwnerPodId  = "MyOwnerPodId";
  public const string PayloadId   = "MyPayloadId";
  public const string OutputId    = "MyOutputId";
  public const string TaskId      = "MyTaskId";
  public const string PartitionId = "MyPartitionId";

  public static readonly TaskOptions TaskOptions;

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public ILogger Logger { get; } = new Logger<SimpleSessionTable>(new LoggerFactory());

  public Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                          TaskOptions         defaultOptions,
                                          CancellationToken   cancellationToken = default)
    => Task.FromResult(SessionId);

  public Task<SessionData> GetSessionAsync(string            sessionId,
                                           CancellationToken cancellationToken = default)
    => Task.FromResult(new SessionData(SessionId,
                                       SessionStatus.Running,
                                       DateTime.Today.ToUniversalTime(),
                                       null,
                                       new List<string>
                                       {
                                         PartitionId,
                                       },
                                       TaskOptions));

  public Task<bool> IsSessionCancelledAsync(string            sessionId,
                                            CancellationToken cancellationToken = default)
    => Task.FromResult(false);

  public Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                     CancellationToken cancellationToken = default)
    => Task.FromResult(TaskOptions);

  public Task<SessionData> CancelSessionAsync(string            sessionId,
                                              CancellationToken cancellationToken = default)
    => Task.FromResult(new SessionData(SessionId,
                                       SessionStatus.Canceled,
                                       DateTime.Today.ToUniversalTime(),
                                       DateTime.Now.ToUniversalTime(), 
                                       new List<string>
                                       {
                                         PartitionId,
                                       },
                                       TaskOptions));

  public Task DeleteSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public IAsyncEnumerable<string> ListSessionsAsync(SessionFilter     sessionFilter,
                                                    CancellationToken cancellationToken = default)
    => new List<string>()
       {
         SessionId,
       }.ToAsyncEnumerable();

  public Task<IEnumerable<SessionData>> ListSessionsAsync(ListSessionsRequest request,
                                                          CancellationToken   cancellationToken = default)
    => Task.FromResult<IEnumerable<SessionData>>(new List<SessionData>
                                                 {
                                                   new(SessionId,
                                                       SessionStatus.Running,
                                                       DateTime.Today.ToUniversalTime(),
                                                       null,
                                                       new List<string>
                                                       {
                                                         PartitionId,
                                                       },
                                                       TaskOptions),
                                                 });
}
