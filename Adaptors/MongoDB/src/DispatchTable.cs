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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

public class DispatchTable : IDispatchTable
{
  public ILogger  Logger                { get; }

  /// <inheritdoc />
  public TimeSpan DispatchRefreshPeriod { get; set; }

  private readonly SessionProvider                                                             sessionProvider_;
  private readonly MongoCollectionProvider<DispatchHandler, DispatchDataModelMapping> dispatchCollectionProvider_;

  [UsedImplicitly]
  public DispatchTable(SessionProvider        sessionProvider, MongoCollectionProvider<DispatchHandler, DispatchDataModelMapping> dispatchCollectionProvider,
                       Options.TableStorage   options,
                       ILogger<DispatchTable> logger)
  {
    sessionProvider_            = sessionProvider;
    dispatchCollectionProvider_ = dispatchCollectionProvider;
    DispatchTimeToLiveDuration  = options.DispatchTimeToLive;
    Logger                      = logger;
  }

  /// <inheritdoc />
  public TimeSpan DispatchTimeToLiveDuration { get; }

  /// <inheritdoc />
  public async Task<bool> TryAcquireDispatchAsync(string                      sessionId,
                                                  string                      taskId,
                                                  string                      dispatchId,
                                                  IDictionary<string, string> metadata,
                                                  CancellationToken           cancellationToken = default)
  {
    using var _ = Logger.LogFunction(taskId,
                                     properties: (nameof(dispatchId), dispatchId));

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchHandler>.Update
                                                    .SetOnInsert(model => model.TimeToLive,
                                                                 DateTime.UtcNow + DispatchTimeToLiveDuration)
                                                    .SetOnInsert(model => model.Id,
                                                                 dispatchId)
                                                    .SetOnInsert(model => model.Attempt,
                                                                 1)
                                                    .SetOnInsert(model => model.CreationDate,
                                                                 DateTime.UtcNow)
                                                    .SetOnInsert(model => model.TaskId,
                                                                 taskId)
                                                    .SetOnInsert(model => model.SessionId,
                                                                 sessionId);

    var res = await dispatchCollection.FindOneAndUpdateAsync<DispatchHandler>(model => model.TaskId == taskId && model.TimeToLive > DateTime.UtcNow,
                                                                              updateDefinition,
                                                                              new FindOneAndUpdateOptions<DispatchHandler>
                                                                              {
                                                                                IsUpsert       = true,
                                                                                ReturnDocument = ReturnDocument.After,
                                                                              },
                                                                              cancellationToken);

    if (dispatchId == res.Id)
    {
      Logger.LogInformation("Dispatch {dispatchId} acquired for task {id}",
                            dispatchId,
                            taskId);

      var oldDispatchUpdates = Builders<DispatchHandler>.Update
                                                        .Set(model => model.TimeToLive,
                                                             DateTime.MinValue)
                                                        .AddToSet(model => model.Statuses,
                                                                  new(TaskStatus.Failed,
                                                                      DateTime.UtcNow,
                                                                      "Dispatch Ttl expired"));

      var olds = await dispatchCollection.UpdateManyAsync(model => model.TaskId == taskId && model.Id != dispatchId,
                                                          oldDispatchUpdates,
                                                          cancellationToken: cancellationToken);

      if (olds.ModifiedCount > 0)
        await dispatchCollection.FindOneAndUpdateAsync(model => model.Id == dispatchId,
                                                       Builders<DispatchHandler>.Update
                                                                                .Set(m => m.Attempt,
                                                                                     olds.ModifiedCount + 1),
                                                       cancellationToken: cancellationToken);
      return true;
    }

    Logger.LogInformation("Could not acquire lease for task {id}",
                          taskId);
    return false;
  }


  /// <inheritdoc />
  public async Task<Dispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(dispatchId);

    var sessionHandle      = await sessionProvider_.GetAsync();
    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    return await dispatchCollection.AsQueryable(sessionHandle)
                                   .Where(model => model.Id == dispatchId)
                                   .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task AddStatusToDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    using var _                  = Logger.LogFunction(id);
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchHandler>.Update
                                                    .AddToSet(model => model.Statuses,
                                                              new(status,
                                                                  DateTime.UtcNow,
                                                                  string.Empty));

    var res = await dispatchCollection.FindOneAndUpdateAsync<DispatchHandler>(model => model.Id == id && model.TimeToLive > DateTime.UtcNow,
                                                                              updateDefinition,
                                                                              new FindOneAndUpdateOptions<DispatchHandler>
                                                                              {
                                                                                IsUpsert       = false,
                                                                                ReturnDocument = ReturnDocument.After,
                                                                              },
                                                                              cancellationToken);

    if (res == null)
      throw new KeyNotFoundException();
  }


  /// <inheritdoc />
  public async Task ExtendDispatchTtl(string id, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(id);

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var res = await dispatchCollection.FindOneAndUpdateAsync(model => model.Id == id,
                                                             Builders<DispatchHandler>.Update
                                                                                      .Set(model => model.TimeToLive,
                                                                                           DateTime.UtcNow + DispatchTimeToLiveDuration),
                                                             cancellationToken: cancellationToken);
    if (res == null)
      throw new KeyNotFoundException();
  }

  /// <inheritdoc />
  public async Task DeleteDispatchFromTaskIdAsync(string id, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(id);

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    await dispatchCollection.DeleteManyAsync(model => model.TaskId == id,
                                             cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeleteDispatch(string id, CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(id);

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    await dispatchCollection.DeleteManyAsync(model => model.Id == id,
                                             cancellationToken);
  }


  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListDispatchAsync(string taskId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                  = Logger.LogFunction(taskId);
    var       sessionHandle      = await sessionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    await foreach (var dispatch in dispatchCollection.AsQueryable(sessionHandle)
                                                     .Where(model => model.TaskId == taskId)
                                                     .Select(model => model.Id)
                                                     .ToAsyncEnumerable()
                                                     .WithCancellation(cancellationToken))
      yield return dispatch;
  }
}
