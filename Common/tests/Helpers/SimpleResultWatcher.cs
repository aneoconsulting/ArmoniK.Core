// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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

using System.Collections.Generic;
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

  /// <inheritdoc/>
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  /// <inheritdoc/>
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc/>
  public Task<IAsyncEnumerator<NewResult>> GetNewResults(string            sessionId,
                                                         CancellationToken cancellationToken = default)
    => Task.FromResult<IAsyncEnumerator<NewResult>>(new SimpleWatcherEnumerator<NewResult>(new[]
                                                                                           {
                                                                                             new NewResult(sessionId,
                                                                                                           ResultId,
                                                                                                           OwnerPodId,
                                                                                                           ResultStatus.Created),
                                                                                           }));

  /// <inheritdoc/>
  public Task<IAsyncEnumerator<ResultOwnerUpdate>> GetResultOwnerUpdates(string            sessionId,
                                                                         CancellationToken cancellationToken = default)
    => Task.FromResult<IAsyncEnumerator<ResultOwnerUpdate>>(new SimpleWatcherEnumerator<ResultOwnerUpdate>(new[]
                                                                                                           {
                                                                                                             new ResultOwnerUpdate(sessionId,
                                                                                                                                   ResultId,
                                                                                                                                   PreviousOwnerPodId,
                                                                                                                                   OwnerPodId),
                                                                                                           }));

  /// <inheritdoc/>
  public Task<IAsyncEnumerator<ResultStatusUpdate>> GetResultStatusUpdates(string            sessionId,
                                                                           CancellationToken cancellationToken = default)
    => Task.FromResult<IAsyncEnumerator<ResultStatusUpdate>>(new SimpleWatcherEnumerator<ResultStatusUpdate>(new[]
                                                                                                             {
                                                                                                               new ResultStatusUpdate(sessionId,
                                                                                                                                      ResultId,
                                                                                                                                      ResultStatus.Completed),
                                                                                                             }));
}
