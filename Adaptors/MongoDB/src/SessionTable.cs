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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

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
  public async Task CreateSessionDataAsync(string            rootSessionId,
                                           string            parentTaskId,
                                           string            dispatchId,
                                           TaskOptions       defaultOptions,
                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreateSessionDataAsync)}");
    activity?.SetTag($"{nameof(CreateSessionDataAsync)}_sessionId",
                     rootSessionId);
    activity?.SetTag($"{nameof(CreateSessionDataAsync)}_parentTaskId",
                     parentTaskId);
    activity?.SetTag($"{nameof(CreateSessionDataAsync)}_dispatchId",
                     dispatchId);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    SessionData data = new(IsCancelled: false,
                           Options: defaultOptions,
                           SessionId: rootSessionId,
                           DispatchId: dispatchId,
                           AncestorsDispatchId: Array.Empty<string>() // TODO : look how to fill this field
                          );

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken)
                           .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<SessionData> GetSessionAsync(string            dispatchId,
                                                 CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetSessionAsync)}");
    activity?.SetTag($"{nameof(GetSessionAsync)}_dispatchId",
                     dispatchId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    var queryableSessionCollection = sessionCollection.AsQueryable(sessionHandle)
                                                      .Where(model => model.DispatchId == dispatchId);

    if (!queryableSessionCollection.Any())
    {
      throw new ArmoniKException($"Key '{dispatchId}' not found");
    }

    return await queryableSessionCollection.FirstAsync(cancellationToken)
                                           .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<bool> IsSessionCancelledAsync(string            sessionId,
                                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(IsSessionCancelledAsync)}");
    return await IsDispatchCancelledAsync(sessionId,
                                          sessionId,
                                          cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<bool> IsDispatchCancelledAsync(string            rootSessionId,
                                                   string            dispatchId,
                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(IsDispatchCancelledAsync)}");
    activity?.SetTag($"{nameof(IsDispatchCancelledAsync)}_sessionId",
                     rootSessionId);
    activity?.SetTag($"{nameof(IsDispatchCancelledAsync)}_dispatchId",
                     dispatchId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);


    var queryableSessionCollection = sessionCollection.AsQueryable(sessionHandle)
                                                      .Where(model => model.DispatchId == dispatchId);

    if (!queryableSessionCollection.Any())
    {
      throw new ArmoniKException($"Key '{dispatchId}' not found");
    }

    return await queryableSessionCollection.Select(model => model.IsCancelled)
                                           .FirstAsync(cancellationToken)
                                           .ConfigureAwait(false);
  }


  /// <inheritdoc />
  public async Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                           CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetDefaultTaskOptionAsync)}");
    activity?.SetTag($"{nameof(GetDefaultTaskOptionAsync)}_sessionId",
                     sessionId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(sdm => sdm.DispatchId == sessionId)
                                  .Select(sdm => sdm.Options)
                                  .FirstAsync(cancellationToken)
                                  .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CancelSessionAsync)}");
    await CancelDispatchAsync(sessionId,
                              sessionId,
                              cancellationToken)
      .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string            rootSessionId,
                                        string            dispatchId,
                                        CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction(dispatchId);
    using var activity = activitySource_.StartActivity($"{nameof(CancelDispatchAsync)}");
    activity?.SetTag($"{nameof(CancelDispatchAsync)}_sessionId",
                     rootSessionId);
    activity?.SetTag($"{nameof(CancelDispatchAsync)}_dispatchId",
                     dispatchId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);


    var resSession = sessionCollection.UpdateOneAsync(model => model.DispatchId == dispatchId,
                                                      Builders<SessionData>.Update.Set(model => model.IsCancelled,
                                                                                       true),
                                                      cancellationToken: cancellationToken);

    if ((await resSession.ConfigureAwait(false)).MatchedCount < 1)
    {
      throw new ArmoniKException("No open session found. Was the session closed?");
    }
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteSessionAsync)}");
    activity?.SetTag($"{nameof(DeleteSessionAsync)}_sessionId",
                     sessionId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    var res = await sessionCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                                      cancellationToken)
                                     .ConfigureAwait(false);

    if (res.DeletedCount == 0)
    {
      throw new ArmoniKException($"Key '{sessionId}' not found");
    }
  }

  /// <inheritdoc />
  public async Task DeleteDispatchAsync(string            rootSessionId,
                                        string            dispatchId,
                                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteDispatchAsync)}");
    activity?.SetTag($"{nameof(DeleteDispatchAsync)}_sessionId",
                     rootSessionId);
    activity?.SetTag($"{nameof(DeleteDispatchAsync)}_dispatchId",
                     dispatchId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    var res = await sessionCollection.DeleteManyAsync(model => model.AncestorsDispatchId.Contains(dispatchId),
                                                      cancellationToken)
                                     .ConfigureAwait(false);
    // TODO: Enable pertinent check depending if what has to be erased are the ancestors or the dispatch itself
    //if (res.DeletedCount == 0)
    //  throw new ArmoniKException($"Key '{dispatchId}' not found");
  }


  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListSessionsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction();
    using var activity = activitySource_.StartActivity($"{nameof(ListSessionsAsync)}");
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    await foreach (var session in sessionCollection.AsQueryable(sessionHandle)
                                                   .Select(model => model.SessionId)
                                                   .Distinct()
                                                   .ToAsyncEnumerable()
                                                   .WithCancellation(cancellationToken)
                                                   .ConfigureAwait(false))
    {
      yield return session;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListDispatchesAsync(string                                     rootSessionId,
                                                            [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListDispatchesAsync)}");
    activity?.SetTag($"{nameof(ListDispatchesAsync)}_sessionId",
                     rootSessionId);
    var sessionHandle = await sessionProvider_.GetAsync()
                                              .ConfigureAwait(false);
    var sessionCollection = await sessionCollectionProvider_.GetAsync()
                                                            .ConfigureAwait(false);

    await foreach (var session in sessionCollection.AsQueryable(sessionHandle)
                                                   .Where(model => model.SessionId == rootSessionId)
                                                   .Select(model => model.DispatchId)
                                                   .Distinct()
                                                   .ToAsyncEnumerable()
                                                   .WithCancellation(cancellationToken)
                                                   .ConfigureAwait(false))
    {
      yield return session;
    }
  }

  /// <inheritdoc />
  public ILogger Logger { get; }


  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionCollectionProvider_.GetAsync()
                                      .ConfigureAwait(false);
      await sessionProvider_.GetAsync()
                            .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);
}
