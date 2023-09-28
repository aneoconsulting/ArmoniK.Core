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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public class SessionTable : ISessionTable
{
  private readonly ActivitySource                                                activitySource_;
  private readonly MongoCollectionProvider<SessionData, SessionDataModelMapping> sessionCollectionProvider_;
  private readonly SessionProvider                                               sessionProvider_;


  private bool isInitialized_;

  public SessionTable(SessionProvider                                               sessionProvider,
                      MongoCollectionProvider<SessionData, SessionDataModelMapping> sessionCollectionProvider,
                      ILogger<SessionTable>                                         logger,
                      ActivitySource                                                activitySource)
  {
    sessionProvider_           = sessionProvider;
    sessionCollectionProvider_ = sessionCollectionProvider;
    Logger                     = logger;
    activitySource_            = activitySource;
  }


  [PublicAPI]
  public async Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                                TaskOptions         defaultOptions,
                                                CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetSessionDataAsync)}");
    var rootSessionId = Guid.NewGuid()
                            .ToString();
    activity?.SetTag($"{nameof(SetSessionDataAsync)}_sessionId",
                     rootSessionId);
    var sessionCollection = sessionCollectionProvider_.Get();

    SessionData data = new(rootSessionId,
                           SessionStatus.Running,
                           partitionIds.AsIList(),
                           defaultOptions);

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken)
                           .ConfigureAwait(false);
    return data.SessionId;
  }

  /// <inheritdoc />
  public async Task<SessionData> GetSessionAsync(string            sessionId,
                                                 CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction(sessionId);
    using var activity = activitySource_.StartActivity($"{nameof(GetSessionAsync)}");
    activity?.SetTag($"{nameof(GetSessionAsync)}_sessionId",
                     sessionId);
    var sessionHandle     = sessionProvider_.Get();
    var sessionCollection = sessionCollectionProvider_.Get();


    try
    {
      return await sessionCollection.Find(session => session.SessionId == sessionId)
                                    .SingleAsync(cancellationToken)
                                    .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found",
                                         e);
    }
  }


  /// <inheritdoc />
  public async Task<bool> IsSessionCancelledAsync(string            sessionId,
                                                  CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction(sessionId);
    using var activity = activitySource_.StartActivity($"{nameof(IsSessionCancelledAsync)}");
    activity?.SetTag($"{nameof(IsSessionCancelledAsync)}_sessionId",
                     sessionId);

    return (await GetSessionAsync(sessionId,
                                  cancellationToken)
              .ConfigureAwait(false)).Status == SessionStatus.Cancelled;
  }

  /// <inheritdoc />
  public async Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetDefaultTaskOptionAsync)}");
    activity?.SetTag($"{nameof(GetDefaultTaskOptionAsync)}_sessionId",
                     sessionId);
    var sessionHandle     = sessionProvider_.Get();
    var sessionCollection = sessionCollectionProvider_.Get();

    try
    {
      return await sessionCollection.Find(sdm => sdm.SessionId == sessionId)
                                    .Project(sdm => sdm.Options)
                                    .SingleAsync(cancellationToken)
                                    .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found",
                                         e);
    }
  }

  /// <inheritdoc />
  public async Task<SessionData> CancelSessionAsync(string            sessionId,
                                                    CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction(sessionId);
    using var activity = activitySource_.StartActivity($"{nameof(CancelSessionAsync)}");
    activity?.SetTag($"{nameof(CancelSessionAsync)}_sessionId",
                     sessionId);

    var sessionCollection = sessionCollectionProvider_.Get();

    var filterDefinition = new FilterDefinitionBuilder<SessionData>().Where(model => model.SessionId == sessionId && model.Status == SessionStatus.Running);

    var resSession = await sessionCollection.FindOneAndUpdateAsync(filterDefinition,
                                                                   Builders<SessionData>.Update.Set(model => model.Status,
                                                                                                    SessionStatus.Cancelled)
                                                                                        .Set(model => model.CancellationDate,
                                                                                             DateTime.UtcNow),
                                                                   new FindOneAndUpdateOptions<SessionData>
                                                                   {
                                                                     ReturnDocument = ReturnDocument.After,
                                                                   },
                                                                   cancellationToken)
                                            .ConfigureAwait(false);

#pragma warning disable IDE0270 // null check can be simplified with a less readable approach
    if (resSession is null)
#pragma warning restore IDE0270
    {
      throw new SessionNotFoundException($"No open session with key '{sessionId}' was found");
    }

    return resSession;
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteSessionAsync)}");
    activity?.SetTag($"{nameof(DeleteSessionAsync)}_sessionId",
                     sessionId);

    var sessionCollection = sessionCollectionProvider_.Get();

    var res = await sessionCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                                      cancellationToken)
                                     .ConfigureAwait(false);

    if (res.DeletedCount == 0)
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found");
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListSessionsAsync(SessionFilter                              sessionFilter,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction();
    using var activity          = activitySource_.StartActivity($"{nameof(ListSessionsAsync)}");
    var       sessionHandle     = sessionProvider_.Get();
    var       sessionCollection = sessionCollectionProvider_.Get();

    await foreach (var sessionId in sessionCollection.AsQueryable(sessionHandle)
                                                     .FilterQuery(sessionFilter)
                                                     .Select(model => model.SessionId)
                                                     .ToAsyncEnumerable()
                                                     .WithCancellation(cancellationToken)
                                                     .ConfigureAwait(false))
    {
      yield return sessionId;
    }
  }

  public async Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                            Expression<Func<SessionData, object?>> orderField,
                                                                                            bool                                   ascOrder,
                                                                                            int                                    page,
                                                                                            int                                    pageSize,
                                                                                            CancellationToken                      cancellationToken = default)
  {
    using var _                 = Logger.LogFunction();
    using var activity          = activitySource_.StartActivity($"{nameof(ListSessionsAsync)}");
    var       sessionHandle     = sessionProvider_.Get();
    var       sessionCollection = sessionCollectionProvider_.Get();

    var findFluent = sessionCollection.Find(sessionHandle,
                                            filter);

    var ordered = ascOrder
                    ? findFluent.SortBy(orderField)
                    : findFluent.SortByDescending(orderField);

    return (await ordered.Skip(page * pageSize)
                         .Limit(pageSize)
                         .ToListAsync(cancellationToken) // todo : do not create list there but pass cancellation token
                         .ConfigureAwait(false), await sessionCollection.Find(sessionHandle,
                                                                              filter)
                                                                        .CountDocumentsAsync(cancellationToken)
                                                                        .ConfigureAwait(false));
  }

  /// <inheritdoc />
  public ILogger Logger { get; }


  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      await sessionCollectionProvider_.Init(cancellationToken)
                                      .ConfigureAwait(false);
      sessionCollectionProvider_.Get();
      sessionProvider_.Get();
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());
}
