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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

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
  public async Task SetSessionDataAsync(string                          rootSessionId,
                                        IEnumerable<string>             partitionIds,
                                        Core.Common.Storage.TaskOptions defaultOptions,
                                        CancellationToken               cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetSessionDataAsync)}");
    activity?.SetTag($"{nameof(SetSessionDataAsync)}_sessionId",
                     rootSessionId);
    var sessionCollection = sessionCollectionProvider_.Get();

    SessionData data = new(rootSessionId,
                           SessionStatus.Running,
                           partitionIds.ToIList(),
                           defaultOptions);

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken)
                           .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<SessionData> GetSessionAsync(string            sessionId,
                                                 CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(sessionId);
    using var activity = activitySource_.StartActivity($"{nameof(GetSessionAsync)}");
    activity?.SetTag($"{nameof(GetSessionAsync)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();
    var sessionCollection = sessionCollectionProvider_.Get();


    try
    {
      return await sessionCollection.AsQueryable(sessionHandle)
                                    .Where(sdm => sdm.SessionId == sessionId)
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

    return await this.GetSessionAsync(sessionId,
                                      cancellationToken)
                     .Status == SessionStatus.Canceled;
  }

  /// <inheritdoc />
  public async Task<Core.Common.Storage.TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                                               CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetDefaultTaskOptionAsync)}");
    activity?.SetTag($"{nameof(GetDefaultTaskOptionAsync)}_sessionId",
                     sessionId);
    var sessionHandle     = sessionProvider_.Get();
    var sessionCollection = sessionCollectionProvider_.Get();

    try
    {
      return await sessionCollection.AsQueryable(sessionHandle)
                                    .Where(sdm => sdm.SessionId == sessionId)
                                    .Select(sdm => sdm.Options)
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
  public async Task CancelSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction(sessionId);
    using var activity = activitySource_.StartActivity($"{nameof(CancelSessionAsync)}");
    activity?.SetTag($"{nameof(CancelSessionAsync)}_sessionId",
                     sessionId);

    var sessionCollection = sessionCollectionProvider_.Get();


    var resSession = sessionCollection.UpdateOneAsync(model => model.SessionId == sessionId && model.Status == SessionStatus.Running,
                                                      Builders<SessionData>.Update.Set(model => model.Status,
                                                                                       SessionStatus.Canceled)
                                                                           .Set(model => model.CancellationDate,
                                                                                DateTime.UtcNow),
                                                      cancellationToken: cancellationToken);

    if ((await resSession.ConfigureAwait(false)).MatchedCount < 1)
    {
      throw new SessionNotFoundException($"No open session with key '{sessionId}' was found");
    }
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

  /// <inheritdoc />
  public ILogger Logger { get; }


  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      sessionCollectionProvider_.Get();
      sessionProvider_.Get();
    }

    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);
}
