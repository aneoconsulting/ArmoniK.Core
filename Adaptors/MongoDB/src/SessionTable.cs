// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.DataStructures;
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
  public IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>> filter,
                                                  Expression<Func<SessionData, T>>    selector,
                                                  CancellationToken                   cancellationToken = default)
  {
    using var activity          = activitySource_.StartActivity($"{nameof(FindSessionsAsync)}");
    var       sessionHandle     = sessionProvider_.Get();
    var       sessionCollection = sessionCollectionProvider_.Get();

    return sessionCollection.Find(sessionHandle,
                                  filter)
                            .Project(selector)
                            .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteSessionAsync)}");
    activity?.SetTag($"{nameof(DeleteSessionAsync)}_sessionId",
                     sessionId);
    var sessionCollection = sessionCollectionProvider_.Get();

    var res = await sessionCollection.DeleteManyAsync(data => data.SessionId == sessionId,
                                                      cancellationToken)
                                     .ConfigureAwait(false);

    if (res.DeletedCount > 0)
    {
      Logger.LogInformation("Deleted {sessionId}",
                            sessionId);
    }
    else
    {
      Logger.LogInformation("Tried to delete {sessionId} but not found",
                            sessionId);
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                            Expression<Func<SessionData, object?>> orderField,
                                                                                            bool                                   ascOrder,
                                                                                            int                                    page,
                                                                                            int                                    pageSize,
                                                                                            CancellationToken                      cancellationToken = default)
  {
    using var _             = Logger.LogFunction();
    using var activity      = activitySource_.StartActivity($"{nameof(ListSessionsAsync)}");
    var       sessionHandle = sessionProvider_.Get();
    var sessionCollection = sessionCollectionProvider_.Get()
                                                      .WithReadPreference(ReadPreference.SecondaryPreferred);

    var sessionList = Task.FromResult(new List<SessionData>());
    if (pageSize > 0)
    {
      var findFluent1 = sessionCollection.Find(sessionHandle,
                                               filter);

      var ordered = ascOrder
                      ? findFluent1.SortBy(orderField)
                      : findFluent1.SortByDescending(orderField);

      sessionList = ordered.Skip(page * pageSize)
                           .Limit(pageSize)
                           .ToListAsync(cancellationToken);
    }

    // Find needs to be duplicated, otherwise, the count is computed on a single page, and not the whole collection
    var sessionCount = sessionCollection.CountDocumentsAsync(sessionHandle,
                                                             filter,
                                                             cancellationToken: cancellationToken);

    return (await sessionList.ConfigureAwait(false), await sessionCount.ConfigureAwait(false));
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

  /// <inheritdoc />
  public async Task<SessionData?> UpdateOneSessionAsync(string                                            sessionId,
                                                        Expression<Func<SessionData, bool>>?              filter,
                                                        Core.Common.Storage.UpdateDefinition<SessionData> updates,
                                                        bool                                              before            = false,
                                                        CancellationToken                                 cancellationToken = default)
  {
    using var activity          = activitySource_.StartActivity($"{nameof(UpdateOneSessionAsync)}");
    var       sessionCollection = sessionCollectionProvider_.Get();

    var updateDefinition = new UpdateDefinitionBuilder<SessionData>().Combine();

    foreach (var (selector, newValue) in updates.Setters)
    {
      updateDefinition = updateDefinition.Set(selector,
                                              newValue);
    }

    var where = new FilterDefinitionBuilder<SessionData>().Where(x => x.SessionId == sessionId);

    if (filter is not null)
    {
      where = new FilterDefinitionBuilder<SessionData>().And(where,
                                                             new FilterDefinitionBuilder<SessionData>().Where(filter));
    }

    var sessionData = await sessionCollection.FindOneAndUpdateAsync(where,
                                                                    updateDefinition,
                                                                    new FindOneAndUpdateOptions<SessionData>
                                                                    {
                                                                      ReturnDocument = before
                                                                                         ? ReturnDocument.Before
                                                                                         : ReturnDocument.After,
                                                                    },
                                                                    cancellationToken)
                                             .ConfigureAwait(false);

    Logger.LogInformation("Update {session} with {@updates}",
                          sessionId,
                          updates);

    return sessionData;
  }
}
