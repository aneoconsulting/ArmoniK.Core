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
using System.Diagnostics;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Implementation of <see cref="IResultWatcher" /> for MongoDB
/// </summary>
public class ResultWatcher : IResultWatcher
{
  private readonly ActivitySource                                          activitySource_;
  private readonly ILogger<ResultWatcher>                                  logger_;
  private readonly MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private          bool                                                    isInitialized_;

  /// <summary>
  ///   Initializes <see cref="ResultWatcher" /> from the given parameters
  /// </summary>
  /// <param name="sessionProvider">MongoDB session provider</param>
  /// <param name="resultCollectionProvider">Result collection provider</param>
  /// <param name="activitySource">Activity source</param>
  /// <param name="logger">Logger used to produce logs</param>
  public ResultWatcher(SessionProvider                                         sessionProvider,
                       MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider,
                       ActivitySource                                          activitySource,
                       ILogger<ResultWatcher>                                  logger)
  {
    sessionProvider_          = sessionProvider;
    resultCollectionProvider_ = resultCollectionProvider;
    activitySource_           = activitySource;
    logger_                   = logger;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      await resultCollectionProvider_.Init(cancellationToken)
                                     .ConfigureAwait(false);
      resultCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }


  /// <inheritdoc />
  public async Task<IAsyncEnumerable<NewResult>> GetNewResults(Expression<Func<Result, bool>> filter,
                                                               CancellationToken              cancellationToken = default)
  {
    using var activity      = activitySource_.StartActivity($"{nameof(GetNewResults)}");
    var       sessionHandle = sessionProvider_.Get();

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<Result>>().Match(input => input.OperationType == ChangeStreamOperationType.Insert)
                                                                              .Match(filter.Convert());

    var changeStreamCursor = await resultCollectionProvider_.Get()
                                                            .WatchAsync(sessionHandle,
                                                                        pipeline,
                                                                        cancellationToken: cancellationToken,
                                                                        options: new ChangeStreamOptions
                                                                                 {
                                                                                   FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                                                                                 })
                                                            .ConfigureAwait(false);

    return new WatchEnumerable<NewResult, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                        resultUpdate => new NewResult(resultUpdate.FullDocument.SessionId,
                                                                                                      resultUpdate.FullDocument.ResultId,
                                                                                                      resultUpdate.FullDocument.OwnerTaskId,
                                                                                                      resultUpdate.FullDocument.Status));
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(Expression<Func<Result, bool>> filter,
                                                                               CancellationToken              cancellationToken = default)
  {
    using var activity      = activitySource_.StartActivity($"{nameof(GetResultOwnerUpdates)}");
    var       sessionHandle = sessionProvider_.Get();

    var changeStreamCursor = await ChangeStreamUpdate.GetUpdates(resultCollectionProvider_.Get(),
                                                                 sessionHandle,
                                                                 filter,
                                                                 new[]
                                                                 {
                                                                   nameof(Result.OwnerTaskId),
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);

    return new WatchEnumerable<ResultOwnerUpdate, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                                doc => new ResultOwnerUpdate(doc.FullDocument.SessionId,
                                                                                                             doc.FullDocument.ResultId,
                                                                                                             "",
                                                                                                             doc.FullDocument.OwnerTaskId));
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(Expression<Func<Result, bool>> filter,
                                                                                 CancellationToken              cancellationToken = default)
  {
    using var activity      = activitySource_.StartActivity($"{nameof(GetResultStatusUpdates)}");
    var       sessionHandle = sessionProvider_.Get();

    var changeStreamCursor = await ChangeStreamUpdate.GetUpdates(resultCollectionProvider_.Get(),
                                                                 sessionHandle,
                                                                 filter,
                                                                 new[]
                                                                 {
                                                                   nameof(Result.Status),
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);

    return new WatchEnumerable<ResultStatusUpdate, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                                 doc => new ResultStatusUpdate(doc.FullDocument.SessionId,
                                                                                                               doc.FullDocument.ResultId,
                                                                                                               doc.FullDocument.Status));
  }
}
