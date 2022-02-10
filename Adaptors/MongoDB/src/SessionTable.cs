// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

public class SessionTable : ISessionTable
{
  private readonly SessionProvider                           sessionProvider_;
  private readonly MongoCollectionProvider<SessionDataModel> sessionCollectionProvider_;

  public SessionTable(SessionProvider sessionProvider, MongoCollectionProvider<SessionDataModel> sessionCollectionProvider, ILogger<SessionTable> logger)
  {
    sessionProvider_           = sessionProvider;
    sessionCollectionProvider_ = sessionCollectionProvider;
    Logger                     = logger;
  }


  [PublicAPI]
  public async Task CreateSessionDataAsync(string            rootSessionId,
                                           string            parentTaskId,
                                           string            dispatchId,
                                           TaskOptions       defaultOptions,
                                           CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    SessionDataModel data = new()
                            {
                              IsCancelled = false,
                              Options     = defaultOptions,
                              SessionId   = rootSessionId,
                              DispatchId  = dispatchId,
                            };

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task<SessionData> GetSessionAsync(string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction(dispatchId);
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(model => model.DispatchId == dispatchId)
                                  .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<bool> IsSessionCancelledAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(sessionId);
    return await IsDispatchCancelledAsync(sessionId,
                                          sessionId,
                                          cancellationToken);
  }

  /// <inheritdoc />
  public async Task<bool> IsDispatchCancelledAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction(dispatchId);
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(model => model.DispatchId == dispatchId)
                                  .Select(model => model.IsCancelled)
                                  .FirstAsync(cancellationToken);

  }


  /// <inheritdoc />
  public async Task<TaskOptions> GetDefaultTaskOptionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction(sessionId);
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(sdm => sdm.DispatchId == sessionId)
                                  .Select(sdm => sdm.Options)
                                  .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(sessionId);
    await CancelDispatchAsync(sessionId,
                              sessionId,
                              cancellationToken);
  }

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(dispatchId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync();


    var resSession = sessionCollection.UpdateOneAsync(model => model.DispatchId == dispatchId,
                                                      Builders<SessionDataModel>.Update
                                                                                .Set(model => model.IsCancelled,
                                                                                     true),
                                                      cancellationToken: cancellationToken);

    if ((await resSession).MatchedCount < 1)
      throw new InvalidOperationException("No open session found. Was the session closed?");
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(sessionId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync();

    await sessionCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                            cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeleteDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(dispatchId);

    var sessionCollection = await sessionCollectionProvider_.GetAsync();

    await sessionCollection.DeleteManyAsync(model => model.AncestorsDispatchId.Contains(dispatchId),
                                            cancellationToken);
  }


  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListSessionsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction();
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    await foreach (var session in sessionCollection.AsQueryable(sessionHandle)
                                                   .Select(model => model.SessionId)
                                                   .Distinct()
                                                   .ToAsyncEnumerable()
                                                   .WithCancellation(cancellationToken))
      yield return session;
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListDispatchesAsync(string rootSessionId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                 = Logger.LogFunction();
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    await foreach (var session in sessionCollection.AsQueryable(sessionHandle)
                                                   .Where(model => model.SessionId == rootSessionId)
                                                   .Select(model => model.DispatchId)
                                                   .Distinct()
                                                   .ToAsyncEnumerable()
                                                   .WithCancellation(cancellationToken))
      yield return session;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }



  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionCollectionProvider_.GetAsync();
      await sessionProvider_.GetAsync();
    }

    isInitialized_ = true;
  }


  private bool isInitialized_ = false;

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);

}
