// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="IResultWatcher" />, fed by the change events of <see cref="ResultTable" />.
/// </summary>
/// <remarks>
///   Events are produced in the order the writes were applied. A status or owner update is produced only when the field
///   actually changes. Filters are evaluated on the new value of the result.
/// </remarks>
public class ResultWatcher : IResultWatcher
{
  private readonly ILogger<ResultWatcher> logger_;
  private readonly ResultTable            resultTable_;

  private bool isInitialized_;

  /// <summary>
  ///   Creates a watcher of the given result table
  /// </summary>
  /// <param name="resultTable">Result table to watch</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public ResultWatcher(ResultTable            resultTable,
                       ILogger<ResultWatcher> logger)
  {
    resultTable_ = resultTable;
    logger_      = logger;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task<IAsyncEnumerable<NewResult>> GetNewResults(Expression<Func<Result, bool>> filter,
                                                         CancellationToken              cancellationToken = default)
  {
    var compiled = filter.Compile();
    return Task.FromResult(Subscribe((previous,
                                      current) => previous is null && compiled(current)
                                                    ? new NewResult(current.SessionId,
                                                                    current.ResultId,
                                                                    current.OwnerTaskId,
                                                                    current.Status)
                                                    : null,
                                     cancellationToken));
  }

  /// <inheritdoc />
  /// <remarks>
  ///   The previous owner is left empty, as the MongoDB and PostgreSQL watchers do, to keep the same events across adapters.
  /// </remarks>
  public Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(Expression<Func<Result, bool>> filter,
                                                                         CancellationToken              cancellationToken = default)
  {
    var compiled = filter.Compile();
    return Task.FromResult(Subscribe((previous,
                                      current) => previous is not null && previous.OwnerTaskId != current.OwnerTaskId && compiled(current)
                                                    ? new ResultOwnerUpdate(current.SessionId,
                                                                            current.ResultId,
                                                                            "",
                                                                            current.OwnerTaskId)
                                                    : null,
                                     cancellationToken));
  }

  /// <inheritdoc />
  public Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(Expression<Func<Result, bool>> filter,
                                                                           CancellationToken              cancellationToken = default)
  {
    var compiled = filter.Compile();
    return Task.FromResult(Subscribe((previous,
                                      current) => previous is not null && previous.Status != current.Status && compiled(current)
                                                    ? new ResultStatusUpdate(current.SessionId,
                                                                             current.ResultId,
                                                                             current.Status)
                                                    : null,
                                     cancellationToken));
  }

  private IAsyncEnumerable<TEvent> Subscribe<TEvent>(Func<Result?, Result, TEvent?> convert,
                                                     CancellationToken              cancellationToken)
    where TEvent : class
    => EventSubscription.Subscribe(handler => resultTable_.ResultChanged += handler,
                                   handler => resultTable_.ResultChanged -= handler,
                                   convert,
                                   logger_,
                                   cancellationToken);
}
