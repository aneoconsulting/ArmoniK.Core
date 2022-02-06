// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;


using KeyNotFoundException = System.Collections.Generic.KeyNotFoundException;
using TaskFilter = ArmoniK.Api.gRPC.V1.TaskFilter;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

[PublicAPI]
public class TableStorage : ITableStorage
{
  private readonly ILogger<TableStorage>                      logger_;
  private readonly MongoCollectionProvider<SessionDataModel>  sessionCollectionProvider_;
  private readonly SessionProvider                            sessionProvider_;
  private readonly MongoCollectionProvider<TaskDataModel>     taskCollectionProvider_;
  private readonly MongoCollectionProvider<DispatchDataModel> dispatchCollectionProvider_;
  private readonly MongoCollectionProvider<ResultDataModel>   resultCollectionProvider_;

  public TableStorage(
    MongoCollectionProvider<SessionDataModel>  sessionCollectionProvider,
    MongoCollectionProvider<TaskDataModel>     taskCollectionProvider,
    MongoCollectionProvider<DispatchDataModel> dispatchCollectionProvider,
    MongoCollectionProvider<ResultDataModel>   resultCollectionProvider,
    SessionProvider                            sessionProvider,
    Options.TableStorage                       options,
    ILogger<TableStorage>                      logger
  )
  {
    if (options.PollingDelay == TimeSpan.Zero)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.TableStorage.PollingDelay)} is not defined.");
    if (options.DispatchTimeToLive == TimeSpan.Zero)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.TableStorage.DispatchTimeToLive)} is not defined.");
    if (options.DispatchAcquisitionPeriod == TimeSpan.Zero)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.TableStorage.DispatchAcquisitionPeriod)} is not defined.");

    sessionCollectionProvider_  = sessionCollectionProvider;
    taskCollectionProvider_     = taskCollectionProvider;
    dispatchCollectionProvider_ = dispatchCollectionProvider;
    resultCollectionProvider_   = resultCollectionProvider;
    sessionProvider_            = sessionProvider;
    PollingDelay                = options.PollingDelay;
    DispatchTimeToLiveDuration  = options.DispatchTimeToLive;
    DispatchRefreshPeriod       = options.DispatchAcquisitionPeriod;
    logger_                     = logger;
  }

  /// <inheritdoc />
  public       TimeSpan           DispatchRefreshPeriod { get; }

  /// <inheritdoc />
  public TimeSpan PollingDelay { get; }

  /// <inheritdoc />
  public TimeSpan DispatchTimeToLiveDuration { get; }
  
  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(sessionId);
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();
    var       taskCollection    = await taskCollectionProvider_.GetAsync();


    var resTask = sessionCollection.UpdateOneAsync(model => model.SessionId == sessionId,
                                                   Builders<SessionDataModel>.Update
                                                                             .Set(model => model.IsCancelled,
                                                                                  true),
                                                   cancellationToken: cancellationToken);

    await taskCollection.UpdateManyAsync(model => model.SessionId == sessionId,
                                         Builders<TaskDataModel>.Update
                                                                .Set(model => model.Status,
                                                                     TaskStatus.Canceling),
                                         cancellationToken: cancellationToken);

    if ((await resTask).MatchedCount < 1)
      throw new InvalidOperationException("No open session found. Was the session closed?");
  }

  /// <inheritdoc />
  public async Task CancelDispatchAsync(string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(dispatchId);
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();
    var       taskCollection    = await taskCollectionProvider_.GetAsync();


    var resSession = sessionCollection.UpdateOneAsync(model => model.DispatchId == dispatchId,
                                                   Builders<SessionDataModel>.Update
                                                                             .Set(model => model.IsCancelled,
                                                                                  true),
                                                   cancellationToken: cancellationToken);

    await taskCollection.UpdateManyAsync(model => model.DispatchId == dispatchId,
                                         Builders<TaskDataModel>.Update
                                                                .Set(model => model.Status,
                                                                     TaskStatus.Canceling),
                                         cancellationToken: cancellationToken);

    if ((await resSession).MatchedCount < 1)
      throw new InvalidOperationException("No open session found. Was the session closed?");
  }

  /// <inheritdoc />
  public async Task<bool> IsSessionCancelledAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction();
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(model => model.SessionId == sessionId)
                                  .Select(model => model.IsCancelled)
                                  .FirstAsync(cancellationToken);

  }

  /// <inheritdoc />
  public async Task<bool> IsDispatchCancelledAsync(string sessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction();
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(model => model.DispatchId == dispatchId)
                                  .Select(model => model.IsCancelled)
                                  .FirstAsync(cancellationToken);

  }

  /// <inheritdoc />
  public async Task<bool> IsTaskCancelledAsync(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(model => model.TaskId == taskId)
                               .Select(model => model.Status == TaskStatus.Canceled || model.Status == TaskStatus.Canceling)
                               .FirstAsync(cancellationToken);

  }


  /// <inheritdoc />
  public async Task CreateSessionAsync(string id, TaskOptions defaultOptions, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(id);
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    SessionDataModel data = new ()
                            {
                              IsCancelled  = false,
                              Options      = defaultOptions,
                              SessionId    = id,
                              DispatchId = id,
                            };

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken);
  }


  /// <inheritdoc />
  public async Task CreateDispatchedSessionAsync(string rootSessionId, string parentTaskId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction();
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    SessionDataModel data = new()
                            {
                              IsCancelled = false,
                              Options = await sessionCollection.AsQueryable(sessionHandle)
                                                               .Where(model => model.SessionId == rootSessionId && model.DispatchId == rootSessionId)
                                                               .Select(model => model.Options)
                                                               .FirstAsync(cancellationToken),
                              SessionId    = rootSessionId,
                              DispatchId   = dispatchId,
                            };

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                                  TaskStatus        status,
                                                  CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       taskCollection = await taskCollectionProvider_.GetAsync();
    

    var updateDefinition = new UpdateDefinitionBuilder<TaskDataModel>().Set(tdm => tdm.Status,
                                                                            status);

    var res = taskCollection.UpdateManyAsync(filter.ToFilterExpression(),
                                             updateDefinition,
                                             cancellationToken: cancellationToken);


    return (int)(await res).MatchedCount;
  }

  /// <inheritdoc />
  public async Task<IEnumerable<(TaskStatus Status, int Count)>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();
    
    

    var res = await taskCollection.AsQueryable(sessionHandle)
                                      .FilterQuery(filter)
                                      .GroupBy(model => model.Status)
                                      .Select(models => new
                                                        {
                                                          Status = models.Key,
                                                          Count  = models.Count(),
                                                        })
                                      .ToListAsync(cancellationToken);

    return res.Select(tuple => (tuple.Status, tuple.Count));
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(id);
    var       taskCollection     = await taskCollectionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    await Task.WhenAll(taskCollection.DeleteOneAsync(tdm => tdm.TaskId == id,
                                                     cancellationToken),
                       dispatchCollection.DeleteManyAsync(model => model.TaskId == id,
                                                          cancellationToken));

  }

  /// <inheritdoc />
  public async Task<bool> TryAcquireDispatchAsync(string                      sessionId,
                                                  string                      taskId,
                                                  string                      dispatchId,
                                                  IDictionary<string, string> metadata,
                                                 CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(taskId, properties:(nameof(dispatchId),dispatchId));

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchDataModel>.Update
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

    var res = await dispatchCollection.FindOneAndUpdateAsync<DispatchDataModel>(model => model.TaskId == taskId && model.TimeToLive > DateTime.UtcNow,
                                                                          updateDefinition,
                                                                          new FindOneAndUpdateOptions<DispatchDataModel>
                                                                          {
                                                                            IsUpsert       = true,
                                                                            ReturnDocument = ReturnDocument.After,
                                                                          },
                                                                          cancellationToken);

    if (dispatchId == res.Id)
    {
      logger_.LogInformation("Dispatch {dispatchId} acquired for task {id}",
                             dispatchId,
                             taskId);

      var oldDispatchUpdates = Builders<DispatchDataModel>.Update
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
                                                       Builders<DispatchDataModel>.Update
                                                                                  .Set(m => m.Attempt,
                                                                                       olds.ModifiedCount + 1),
                                                       cancellationToken: cancellationToken);
      return true;
    }

    logger_.LogInformation("Could not acquire lease for task {id}",
                           taskId);
    return false;
  }

  /// <inheritdoc />
  public async Task DeleteDispatch(string id, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(id);
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();
    await dispatchCollection.DeleteOneAsync(model => model.Id == id,
                                            cancellationToken);
  }

  /// <inheritdoc />
  public async Task AddStatusToDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(id);
    var       taskCollection     = await taskCollectionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchDataModel>.Update
                                                      .AddToSet(model => model.Statuses,
                                                                new(status,
                                                                    DateTime.UtcNow,
                                                                    string.Empty));

    var res = await dispatchCollection.FindOneAndUpdateAsync<DispatchDataModel>(model => model.Id == id && model.TimeToLive > DateTime.UtcNow,
                                                                                updateDefinition,
                                                                                new FindOneAndUpdateOptions<DispatchDataModel>
                                                                                {
                                                                                  IsUpsert       = false,
                                                                                  ReturnDocument = ReturnDocument.After,
                                                                                },
                                                                                cancellationToken);

    if (res == null)
      throw new KeyNotFoundException();

    await taskCollection.FindOneAndUpdateAsync(model => model.TaskId == res.TaskId,
                                               Builders<TaskDataModel>.Update
                                                                      .Set(model => model.Status,
                                                                           status),
                                               cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task ExtendDispatchTtl(string id, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(id);
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var res = await dispatchCollection.FindOneAndUpdateAsync(model => model.Id == id,
                                                   Builders<DispatchDataModel>.Update
                                                                              .Set(model => model.TimeToLive,
                                                                                   DateTime.UtcNow + DispatchTimeToLiveDuration),
                                                   cancellationToken: cancellationToken);
    if (res == null)
      throw new KeyNotFoundException();
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListDispatchAsync(string taskId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(taskId);
    var       sessionHandle      = await sessionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    await foreach (var dispatch in dispatchCollection.AsQueryable(sessionHandle)
                                                     .Where(model => model.TaskId == taskId)
                                                     .Select(model => model.Id)
                                                     .ToAsyncEnumerable()
                                                     .WithCancellation(cancellationToken))
      yield return dispatch;
  }

  /// <inheritdoc />
  public async Task<IDispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(dispatchId);
    var       sessionHandle      = await sessionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    return await dispatchCollection.AsQueryable(sessionHandle)
                                   .Where(model => model.Id == dispatchId)
                                   .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListResultsAsync(string sessionId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(sessionId);
    var       sessionHandle    = await sessionProvider_.GetAsync();
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    await foreach (var result in resultCollection.AsQueryable(sessionHandle)
                                                 .Where(model => model.SessionId == sessionId)
                                                 .Select(model => model.Id)
                                                 .ToAsyncEnumerable()
                                                 .WithCancellation(cancellationToken))
      yield return result;
  }

  /// <inheritdoc />
  public async Task<IResult> GetResult(string sessionId, string key, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       sessionHandle    = await sessionProvider_.GetAsync();
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    return await resultCollection.AsQueryable(sessionHandle)
                                 .Where(model => model.Id == key)
                                 .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<bool> AreResultsAvailableAsync(string sessionId, IEnumerable<string> keys, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(sessionId);
    var       sessionHandle    = await sessionProvider_.GetAsync();
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    return !await resultCollection.AsQueryable(sessionHandle)
                                  .AnyAsync(model => !model.IsResultAvailable,
                                            cancellationToken);
  }

  /// <inheritdoc />
  public async Task SetResult(string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    var res =await resultCollection.UpdateOneAsync(Builders<ResultDataModel>.Filter
                                                                   .Where(model => model.Key == key && model.OwnerTaskId == ownerTaskId),
                                          Builders<ResultDataModel>.Update
                                                                   .Set(model => model.IsResultAvailable,
                                                                        true)
                                                                   .Set(model => model.Data,
                                                                        smallPayload),
                                          cancellationToken: cancellationToken);
    if (res.ModifiedCount == 0)
      throw new KeyNotFoundException();
  }

  /// <inheritdoc />
  public async Task DeleteResult(string session, string key, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    await resultCollection.DeleteOneAsync(model => model.Key == key && model.SessionId == session,
                                          cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeleteResults(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction();
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    await resultCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                          cancellationToken);
  }

  /// <inheritdoc />
  public async Task<string> GetDispatchId(string taskId, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                               .Where(model => model.TaskId == taskId)
                               .Select(model => model.DispatchId)
                               .FirstAsync(cancellationToken);

  }

  /// <inheritdoc />
  public async Task ChangeTaskDispatch(string oldDispatchId, string targetDispatchId, CancellationToken cancellationToken)
  {
    using var _              = logger_.LogFunction();

    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.UpdateManyAsync(model => model.DispatchId == oldDispatchId,
                                         Builders<TaskDataModel>.Update
                                                                .Set(model => model.DispatchId,
                                                                     targetDispatchId),
                                         cancellationToken: cancellationToken);
  }

  /// <inheritdoc />
  public async Task ChangeResultDispatch(string oldDispatchId, string targetDispatchId, CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction();

    var resultCollection = await resultCollectionProvider_.GetAsync();



    await resultCollection.UpdateManyAsync(model => model.OriginDispatchId == oldDispatchId,
                                           Builders<ResultDataModel>.Update
                                                                  .Set(model => model.OriginDispatchId,
                                                                       targetDispatchId),
                                           cancellationToken: cancellationToken);


  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListSessionsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction();
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
  public async Task<TaskOptions> GetDefaultTaskOptionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(sessionId);
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(sdm => sdm.DispatchId == sessionId)
                                  .Select(sdm => sdm.Options)
                                  .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task InitializeTaskCreationAsync(string                            session,
                                           string                                 parentTaskId,
                                           string                                 dispatchId,
                                           TaskOptions                            options,
                                           IEnumerable<ITableStorage.TaskRequest> requests,
                                           CancellationToken                      cancellationToken = default)
  {
    using var _                 = logger_.LogFunction($"{session}.{parentTaskId}.{dispatchId}");
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       taskCollection    = await taskCollectionProvider_.GetAsync();
    var       resultCollection  = await resultCollectionProvider_.GetAsync();


    async Task LoadOptions()
    {
      options = await GetDefaultTaskOptionAsync(session,
                                                cancellationToken);
    }

    IList<string> ancestors = null;

    async Task LoadAncestorDispatchIds()
    {
      ancestors = await taskCollection.AsQueryable(sessionHandle)
                                      .Where(model => model.TaskId == parentTaskId)
                                      .Select(model => model.AncestorDispatchIds)
                                      .FirstAsync(cancellationToken);

      ancestors.Add(dispatchId);
    }


    var preload = new List<Task>();
    if (options is null)
    {

      preload.Add(LoadOptions());
    }

    preload.Add(LoadAncestorDispatchIds());

    await Task.WhenAll(preload);


    var taskDataModels = requests.Select(request =>
                                         {
                                           var tdm = new TaskDataModel
                                                     {
                                                       HasPayload          = request.PayloadChunk is not null,
                                                       Options             = options,
                                                       SessionId           = session,
                                                       Status              = TaskStatus.Creating,
                                                       Payload             = request.PayloadChunk?.ToArray(),
                                                       ParentTaskId        = parentTaskId,
                                                       DataDependencies    = request.DataDependencies.ToList(),
                                                       TaskId              = request.Id,
                                                       DispatchId          = dispatchId,
                                                       AncestorDispatchIds = ancestors,
                                                       ExpectedOutput      = request.ExpectedOutputKeys.ToList(),
                                                     };

                                           logger_.LogDebug("Stored {size} bytes for task",
                                                            tdm.ToBson().Length);

                                           var resultWriter = request.ExpectedOutputKeys
                                                                     .Select(key => new InsertOneModel<ResultDataModel>(new ResultDataModel()
                                                                                                                                           {
                                                                                                                                             IsResultAvailable = false,
                                                                                                                                             OwnerTaskId = request.Id, 
                                                                                                                                             Key = key,
                                                                                                                                             SessionId  = session,
                                                                                                                                           }));
                                           return (TaskDataModel: tdm, WriterModel:resultWriter);
                                         })
                                 .ToList();

    await taskCollection.InsertManyAsync(taskDataModels.Select(tuple => tuple.TaskDataModel),
                                         cancellationToken: cancellationToken);

    await resultCollection.BulkWriteAsync(taskDataModels.SelectMany(tuple => tuple.WriterModel),
                                          new BulkWriteOptions
                                          {
                                            IsOrdered = false,
                                          },
                                          cancellationToken);

  }

  /// <inheritdoc />
  public async Task<ITaskData> ReadTaskAsync(string id, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction(id);
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    return await taskCollection.AsQueryable(sessionHandle)
                                  .Where(tdm => tdm.TaskId == id)
                                  .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  async IAsyncEnumerable<string> ITableStorage.ListTasksAsync(TaskFilter filter, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await foreach (var taskId in taskCollection.AsQueryable(sessionHandle)
                                               .FilterQuery(filter)
                                               .Select(model => model.TaskId)
                                               .AsAsyncEnumerable().WithCancellation(cancellationToken))
      yield return taskId;
  }

  /// <inheritdoc />
  public async Task UpdateTaskStatusAsync(string            id,
                                          TaskStatus        status,
                                          CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction(id);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    var updateDefinition = new UpdateDefinitionBuilder<TaskDataModel>().Set(tdm => tdm.Status,
                                                                            status);
    logger_.LogDebug("update task {task} to status {status}",
                     id,
                     status);
    var res = await taskCollection.UpdateManyAsync(x => x.TaskId == id &&
                                                        x.Status != TaskStatus.Completed &&
                                                        x.Status != TaskStatus.Canceled,
                                                   updateDefinition,
                                                   cancellationToken: cancellationToken);

    switch (res.MatchedCount)
    {
      case 0:
        throw new ArmoniKException($"Task not found - {id}");
      case > 1:
        throw new ArmoniKException("Multiple tasks modified");
    }
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default) 
    => throw new NotImplementedException();

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var session        = sessionProvider_.GetAsync();
      var taskCollection = taskCollectionProvider_.GetAsync();
      await sessionCollectionProvider_.GetAsync();
      await session;
      await taskCollection;
      isInitialized_ = true;
    }
  }


  private bool isInitialized_;
  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);
}