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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

internal class SimpleResultWatcher : IResultWatcher
{
  public const string ResultId           = "MyResultId";
  public const string OwnerPodId         = "MyOwnerPodId";
  public const string PreviousOwnerPodId = "MyPreviousOwnerPodId";

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task<IAsyncEnumerable<NewResult>> GetNewResults(string            sessionId,
                                                         CancellationToken cancellationToken = default)
    => Task.FromResult(new[]
                       {
                         new NewResult(sessionId,
                                       ResultId,
                                       OwnerPodId,
                                       ResultStatus.Created),
                       }.ToAsyncEnumerable());

  /// <inheritdoc />
  public Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(string            sessionId,
                                                                         CancellationToken cancellationToken = default)
    => Task.FromResult(new[]
                       {
                         new ResultOwnerUpdate(sessionId,
                                               ResultId,
                                               PreviousOwnerPodId,
                                               OwnerPodId),
                       }.ToAsyncEnumerable());

  /// <inheritdoc />
  public Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(string            sessionId,
                                                                           CancellationToken cancellationToken = default)
    => Task.FromResult(new[]
                       {
                         new ResultStatusUpdate(sessionId,
                                                ResultId,
                                                ResultStatus.Completed),
                       }.ToAsyncEnumerable());
}
