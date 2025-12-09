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
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Implementation of <see cref="ISessionWatcher" /> for MongoDB
/// </summary>
public class SessionWatcher : ISessionWatcher
{
  private readonly ActivitySource activitySource_;
  private readonly ILogger<SessionWatcher> logger_;
  private readonly SessionProvider sessionProvider_;
  private readonly MongoCollectionProvider<SessionData, SessionDataModelMapping> sessionCollectionProvider_;
  private bool isInitialized_;

  /// <summary>
  ///   Initializes <see cref="SessionWatcher" /> from the given parameters
  /// </summary>
  /// <param name="sessionProvider">MongoDB session provider</param>
  /// <param name="sessionCollectionProvider">Task collection provider</param>
  /// <param name="activitySource">Activity source</param>
  /// <param name="logger">Logger used to produce logs</param>
  public SessionWatcher(SessionProvider sessionProvider,
    MongoCollectionProvider<SessionData, SessionDataModelMapping> sessionCollectionProvider,
    ActivitySource activitySource,
    ILogger<SessionWatcher> logger)
  {
    sessionProvider_ = sessionProvider;
    sessionCollectionProvider_ = sessionCollectionProvider;
    activitySource_ = activitySource;
    logger_ = logger;
  }

  /// <inheritdoc />
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    var result = await HealthCheckResultCombiner.Combine(tag,
                                                         $"{nameof(SessionWatcher)} is not initialized",
                                                         sessionProvider_,
                                                         sessionCollectionProvider_)
                                                .ConfigureAwait(false);

    return isInitialized_ && result.Status == HealthStatus.Healthy
             ? HealthCheckResult.Healthy()
             : HealthCheckResult.Unhealthy(result.Description);
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      await sessionCollectionProvider_.Init(cancellationToken)
                                   .ConfigureAwait(false);
      sessionCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<SessionUpdate> GetSessionsChangesAsync(
      [EnumeratorCancellation] CancellationToken token = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetSessionsChangesAsync)}");
    var sessionHandle = sessionProvider_.Get();

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<SessionData>>()
      .Match(i => i.OperationType == ChangeStreamOperationType.Insert
               || i.OperationType == ChangeStreamOperationType.Update
               || i.OperationType == ChangeStreamOperationType.Delete);

    var changeStreamCursor = await sessionCollectionProvider_.Get()
      .WatchAsync(sessionHandle,
                  pipeline,
                  cancellationToken: token,
                  options: new ChangeStreamOptions
                  {
                    FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                  })
      .ConfigureAwait(false);

    while (await changeStreamCursor.MoveNextAsync(token))
    {
      var updates = changeStreamCursor.Current
        .Select(translateSessionUpdate);
      foreach (var update in updates)
        yield return update;
    }
  }

  private static SessionUpdate translateSessionUpdate(
      ChangeStreamDocument<SessionData> document)
  {
    return document.OperationType switch
    {
      ChangeStreamOperationType.Insert =>
          new SessionUpdate(
            document.FullDocument.SessionId,
            document.FullDocument.Status,
            SessionUpdateType.Create),
      ChangeStreamOperationType.Update =>
          new SessionUpdate(
            document.FullDocument.SessionId,
            document.FullDocument.Status,
            SessionUpdateType.Update),
      ChangeStreamOperationType.Delete =>
          new SessionUpdate(
            document.DocumentKey.First().Value.AsString, // hmm.. :(
            SessionStatus.Deleted,
            SessionUpdateType.Delete),
      _ => throw new InvalidOperationException("unsupported operation type")
    };
  }
}
