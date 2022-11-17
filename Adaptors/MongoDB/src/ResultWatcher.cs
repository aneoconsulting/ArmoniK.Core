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
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Graphs;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public class ResultWatcher : IResultWatcher
{
  private readonly ActivitySource                                          activitySource_;
  private readonly ILogger<ResultWatcher>                                  logger_;
  private readonly SessionProvider                                         sessionProvider_;
  private          bool                                                    isInitialized_;
  private readonly MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider_;

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
      resultCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }


  public async Task<IWatchEnumerator<NewResult>> GetNewResults(string            sessionId,
                                                               CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetNewResults)}");
    activity?.SetTag($"{nameof(GetNewResults)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<Result>>().Match(input => input.OperationType          == ChangeStreamOperationType.Insert &&
                                                                                              input.FullDocument.SessionId == sessionId);

    var changeStreamCursor = await resultCollectionProvider_.Get()
                                                            .WatchAsync(sessionHandle,
                                                                        pipeline: pipeline,
                                                                        cancellationToken: cancellationToken,
                                                                        options: new ChangeStreamOptions
                                                                                 {
                                                                                   FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                                                                                 })
                                                            .ConfigureAwait(false);

    return new WatchEnumerator<NewResult, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                        resultUpdate => new NewResult(resultUpdate.FullDocument.SessionId,
                                                                                                      resultUpdate.FullDocument.Name,
                                                                                                      resultUpdate.FullDocument.OwnerTaskId,
                                                                                                      resultUpdate.FullDocument.Status),
                                                                        cancellationToken);
  }

  public async Task<IWatchEnumerator<ResultOwnerUpdate>> GetResultOwnerUpdates(string            sessionId,
                                                                               CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResultOwnerUpdates)}");
    activity?.SetTag($"{nameof(GetResultOwnerUpdates)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();

    var changeStreamCursor = await ChangeStreamUpdate.GetUpdates(resultCollectionProvider_.Get(),
                                                                 sessionHandle,
                                                                 document => document.FullDocument.SessionId == sessionId,
                                                                 new[]
                                                                 {
                                                                   nameof(Result.OwnerTaskId),
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);

    return new WatchEnumerator<ResultOwnerUpdate, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                                doc => new ResultOwnerUpdate(doc.FullDocument.SessionId,
                                                                                                             doc.FullDocument.Name,
                                                                                                             "",
                                                                                                             doc.FullDocument.OwnerTaskId),
                                                                                cancellationToken);
  }

  public async Task<IWatchEnumerator<ResultStatusUpdate>> GetResultStatusUpdates(string            sessionId,
                                                                                 CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResultStatusUpdates)}");
    activity?.SetTag($"{nameof(GetResultStatusUpdates)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();

    var changeStreamCursor = await ChangeStreamUpdate.GetUpdates(resultCollectionProvider_.Get(),
                                                                 sessionHandle,
                                                                 document => document.FullDocument.SessionId == sessionId,
                                                                 new[]
                                                                 {
                                                                   nameof(Result.Status),
                                                                 },
                                                                 cancellationToken)
                                                     .ConfigureAwait(false);

    return new WatchEnumerator<ResultStatusUpdate, ChangeStreamDocument<Result>>(changeStreamCursor,
                                                                                 doc => new ResultStatusUpdate(doc.FullDocument.SessionId,
                                                                                                               doc.FullDocument.Name,
                                                                                                               doc.FullDocument.Status),
                                                                                 cancellationToken);
  }
}