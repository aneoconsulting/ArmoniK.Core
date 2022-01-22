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
using System.Linq.Expressions;
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
using SessionId = ArmoniK.Api.gRPC.V1.SessionId;
using TaskFilter = ArmoniK.Api.gRPC.V1.TaskFilter;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB;

[PublicAPI]
public class TableStorage : ITableStorage
{
  private readonly ILogger<TableStorage>                                      logger_;
  private readonly MongoCollectionProvider<SessionDataModel>                  sessionCollectionProvider_;
  private readonly SessionProvider                                            sessionProvider_;
  private readonly MongoCollectionProvider<TaskDataModel>                     taskCollectionProvider_;
  private readonly MongoCollectionProvider<DispatchDataModel> dispatchCollectionProvider_;
  private readonly MongoCollectionProvider<ResultDataModel>                   resultCollectionProvider_;

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
    if (options.DispatchAcquisitionDuration == TimeSpan.Zero)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.TableStorage.DispatchAcquisitionDuration)} is not defined.");
    if (options.DispatchAcquisitionPeriod == TimeSpan.Zero)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.TableStorage.DispatchAcquisitionPeriod)} is not defined.");

    sessionCollectionProvider_  = sessionCollectionProvider;
    taskCollectionProvider_     = taskCollectionProvider;
    dispatchCollectionProvider_ = dispatchCollectionProvider;
    resultCollectionProvider_   = resultCollectionProvider;
    sessionProvider_            = sessionProvider;
    PollingDelay                = options.PollingDelay;
    DispatchAcquisitionDuration = options.DispatchAcquisitionDuration;
    DispatchAcquisitionPeriod   = options.DispatchAcquisitionPeriod;
    logger_                     = logger;
  }

  public TimeSpan DispatchAcquisitionPeriod { get; set; }

  public TimeSpan DispatchAcquisitionDuration { get; set; }

  public TimeSpan PollingDelay       { get; }

  /// <inheritdoc />
  public TimeSpan DispatchTimeToLive { get; set; }


  /// <inheritdoc />
  public async Task<CreateSessionReply> CreateSessionAsync(CreateSessionRequest sessionRequest, CancellationToken cancellationToken = default)
  {
        using var _                               = logger_.LogFunction();
    var       sessionHandle                   = await sessionProvider_.GetAsync();
    var       sessionCollection               = await sessionCollectionProvider_.GetAsync();

    SessionDataModel data;
    if (sessionRequest.SessionTypeCase == CreateSessionRequest.SessionTypeOneofCase.Root)
    {

      data = new ()
             {
               IsCancelled  = false,
               Options      = sessionRequest.Root.DefaultTaskOption,
               SessionId    = sessionRequest.Root.Id,
               ParentTaskId = sessionRequest.Root.Id,
             };
    }
    else
    {
      List<string> ancestors = new();

      var t = await sessionCollection.AsQueryable(sessionHandle)
                                     .Where(x => x.SessionId == sessionRequest.SubSession.RootId &&
                                                 x.ParentTaskId == sessionRequest.SubSession.ParentTaskId)
                                     .FirstAsync(cancellationToken);
      ancestors.AddRange(t.Ancestors);

      ancestors.Add(sessionRequest.SubSession.ParentTaskId);

      data = new ()
             {
               IsCancelled  = false,
               Options      = t.Options,
               Ancestors    = ancestors,
               ParentTaskId = sessionRequest.SubSession.ParentTaskId,
               SessionId    = sessionRequest.SubSession.RootId,
             };
    }

    await sessionCollection.InsertOneAsync(data,
                                           cancellationToken: cancellationToken);

    return new()
           {
             Ok = new(),
           };
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(sessionId.ToString());
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    var filterDefinition = Builders<SessionDataModel>.Filter
                                                     .Where(sdm => sessionId.Session == sdm.SessionId ||
                                                                   (sessionId.ParentTaskId == sessionId.Session ||
                                                                    sdm.Ancestors.Any(id => id == sessionId.ParentTaskId)));

    var updateDefinition = Builders<SessionDataModel>.Update
                                                     .Set(model => model.IsCancelled,
                                                          true);

    var res = await sessionCollection.UpdateOneAsync(filterDefinition,
                                                     updateDefinition,
                                                     cancellationToken: cancellationToken);
    if (res.MatchedCount < 1)
      throw new InvalidOperationException("No open session found. Was the session closed?");
  }
  
  /// <inheritdoc />
  public async Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                                  TaskStatus        status,
                                                  CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();


    var rootTaskFilter = new TaskFilter();
    if (filter.IdsCase == TaskFilter.IdsOneofCase.Known)
    {
      rootTaskFilter.Known = filter.Known;
    }
    else
    {
      rootTaskFilter.Unknown = filter.Unknown;
    }

    var rootTaskListTask = taskCollection.AsQueryable(sessionHandle)
                                         .FilterQuery(rootTaskFilter)
                                         .Select(model => model.TaskId)
                                         .ToListAsync(cancellationToken);

    var updateDefinition = new UpdateDefinitionBuilder<TaskDataModel>().Set(tdm => tdm.Status,
                                                                            status);

    if (filter.StatusesCase == TaskFilter.StatusesOneofCase.Included)
    {
      if (filter.Included.IncludedStatuses.Contains(TaskStatus.Completed) ||
          filter.Included.IncludedStatuses.Contains(TaskStatus.Canceled) ||
          filter.Included.IncludedStatuses.Contains(TaskStatus.Failed))
      {
        throw new ArgumentException("Completed, Canceled and Failed are definitive statuses and they cannot be overwritten",
                                    nameof(filter));
      }
    }
    else
    {
      if (!filter.Excluded.IncludedStatuses.Contains(TaskStatus.Completed))
        filter.Excluded.IncludedStatuses.Add(TaskStatus.Completed);
      if (!filter.Excluded.IncludedStatuses.Contains(TaskStatus.Canceled))
        filter.Excluded.IncludedStatuses.Add(TaskStatus.Canceled);
      if (!filter.Excluded.IncludedStatuses.Contains(TaskStatus.Failed))
        filter.Excluded.IncludedStatuses.Add(TaskStatus.Failed);
    }


    var rootResultTask = taskCollection.UpdateManyAsync(filter.ToFilterExpression(),
                                                      updateDefinition,
                                                      cancellationToken: cancellationToken);


    var filterExpression = BuildChildrenFilterExpression(await rootTaskListTask);

    var childrenTaskFilter = new TaskFilter(filter)
                             {
                               Unknown = new(),
                             };
    var childrenUpdateFilter = Builders<TaskDataModel>.Filter.And(filterExpression,
                                       childrenTaskFilter.ToFilterExpression());

    var childrenResultTask = taskCollection.UpdateManyAsync(childrenUpdateFilter,
                                                            updateDefinition,
                                                            cancellationToken: cancellationToken);



    return (int)((await rootResultTask).MatchedCount + (await childrenResultTask).MatchedCount);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<(TaskStatus Status, int Count)>> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction();
    var       sessionHandle  = await sessionProvider_.GetAsync();
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    var rootTaskFilter = new TaskFilter();
    if (filter.IdsCase == TaskFilter.IdsOneofCase.Known)
    {
      rootTaskFilter.Known = filter.Known;
    }
    else
    {
      rootTaskFilter.Unknown = filter.Unknown;
    }

    var rootTaskListTask = taskCollection.AsQueryable(sessionHandle)
                                         .FilterQuery(rootTaskFilter)
                                         .Select(model => model.TaskId)
                                         .ToListAsync(cancellationToken);

    var rootCountTask = taskCollection.AsQueryable(sessionHandle)
                                      .FilterQuery(filter)
                                      .Select(model => new
                                                       {
                                                         model.Status,
                                                         Id = model.TaskId,
                                                       })
                                      .GroupBy(model => model.Status)
                                      .Select(models => new
                                                        {
                                                          Status = models.Key,
                                                          Count  = models.Count(),
                                                        })
                                      .ToListAsync(cancellationToken);

    var rootTaskList = await rootTaskListTask;
    logger_.LogTrace("root tasks: {taskList}",
                     string.Join(", ",
                                 rootTaskList));

    var filterExpression = BuildChildrenFilterExpression(rootTaskList);

    var childrenTaskFilter = new TaskFilter(filter)
                             {
                               Unknown = new(),
                             };

    var childrenCountTask = taskCollection.AsQueryable(sessionHandle)
                                          .Where(filterExpression)
                                          .FilterQuery(childrenTaskFilter)
                                          .GroupBy(model => model.Status)
                                          .Select(models => new
                                                            {
                                                              Status = models.Key,
                                                              Count  = models.Count(),
                                                            })
                                          .ToListAsync(cancellationToken);

    var rootCount     = await rootCountTask;
    var childrenCount = await childrenCountTask;

    logger_.LogDebug("RootCount:{rootCount}",
                     rootCount);
    logger_.LogDebug("ChildrenCount:{childrenCount}",
                     childrenCount);

    rootCount.AddRange(childrenCount);

    var output = rootCount.GroupBy(arg => arg.Status)
                          .Select(grouping => (Status: grouping.Key, Count: grouping.Sum(arg => arg.Count)));

    logger_.LogDebug("Output:{output}",
                     output);

    return output;
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string id, CancellationToken cancellationToken = default)
  {
    using var _              = logger_.LogFunction(id);
    var       taskCollection = await taskCollectionProvider_.GetAsync();

    await taskCollection.DeleteOneAsync(tdm => tdm.TaskId == id,
                                        cancellationToken);
  }

  /// <inheritdoc />
  public async Task<bool> TryAcquireDispatchAsync(string            dispatchId,
                                                  string            taskId,
                                                  DateTime          ttl,
                                                  string            podId             = "",
                                                  string            nodeId            = "",
                                                  CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(taskId, properties:(nameof(dispatchId),dispatchId));

    var dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchDataModel>.Update
                                                      .SetOnInsert(model => model.TimeToLive,
                                                                   DateTime.UtcNow + DispatchAcquisitionDuration)
                                                      .SetOnInsert(model => model.Id,
                                                                   dispatchId)
                                                      .SetOnInsert(model => model.Attempt,
                                                                   1)
                                                      .SetOnInsert(model => model.CreationDate,
                                                                   DateTime.UtcNow)
                                                      .SetOnInsert(model => model.TaskId,
                                                                   taskId);

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
  public async Task UpdateDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default)
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
  public async Task ExtendDispatchTtl(string id, DateTime newTtl, CancellationToken cancellationToken = default)
  {
    using var _                  = logger_.LogFunction(id);
    var       taskCollection     = await taskCollectionProvider_.GetAsync();
    var       dispatchCollection = await dispatchCollectionProvider_.GetAsync();

    var updateDefinition = Builders<DispatchDataModel>.Update
                                                      .Set(model => model.TimeToLive,
                                                           DateTime.UtcNow + DispatchAcquisitionDuration);

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
  public async Task SetResult(string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       resultCollection = await resultCollectionProvider_.GetAsync();

    var res =await resultCollection.UpdateOneAsync(Builders<ResultDataModel>.Filter
                                                                   .Where(model => model.Key == key && model.Owner == ownerTaskId),
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







  public async Task<bool> IsSessionCancelledAsync(SessionId sessionId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(sessionId.ToString());
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();


    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(x => x.IsCancelled && x.SessionId == sessionId.Session)
                                  .AnyAsync(cancellationToken);
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
  public async Task<TaskOptions> GetDefaultTaskOption(string sessionId, string parentTaskId, CancellationToken cancellationToken = default)
  {
    using var _                 = logger_.LogFunction(sessionId);
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();

    return await sessionCollection.AsQueryable(sessionHandle)
                                  .Where(sdm => sdm.ParentTaskId == parentTaskId)
                                  .Select(sdm => sdm.Options)
                                  .FirstAsync(cancellationToken);
  }

  /// <inheritdoc />
  public async Task InitializeTaskCreation(string                                                session,
                                           string                                                parentTaskId,
                                           TaskOptions                                           options,
                                           IEnumerable<CreateSmallTaskRequest.Types.TaskRequest> requests,
                                           CancellationToken                                     cancellationToken = default)
  {
    using var _                 = logger_.LogFunction($"{session}.{parentTaskId}");
    var       sessionHandle     = await sessionProvider_.GetAsync();
    var       taskCollection    = await taskCollectionProvider_.GetAsync();
    var       sessionCollection = await sessionCollectionProvider_.GetAsync();
    var       resultCollection  = await resultCollectionProvider_.GetAsync();


    var parents = await sessionCollection.AsQueryable(sessionHandle)
                                         .Where(model => model.ParentTaskId == parentTaskId)
                                         .Select(model => model.Ancestors)
                                         .FirstAsync(cancellationToken);

    options ??= await sessionCollection.AsQueryable(sessionHandle)
                                       .Where(model => model.ParentTaskId == parentTaskId)
                                       .Select(model => model.Options)
                                       .FirstAsync(cancellationToken);

    var taskDataModels = requests.Select(request =>
                                         {
                                           var tdm = new TaskDataModel
                                                     {
                                                       HasPayload       = request.Payload is not null,
                                                       Options          = options,
                                                       SessionId        = session,
                                                       Status           = TaskStatus.Creating,
                                                       Ancestors        = parents,
                                                       Payload          = request.Payload?.ToByteArray(),
                                                       ParentTaskId     = parentTaskId,
                                                       DataDependencies = request.DataDependencies,
                                                       TaskId           = request.Id,
                                                     };

                                           logger_.LogDebug("Stored {size} bytes for task",
                                                            tdm.ToBson().Length);
                                           var resultUpdateDefinition = Builders<ResultDataModel>.Update
                                                                                                 .Set(model => model.Owner,
                                                                                                      request.Id);

                                             var resultFilter = Builders<ResultDataModel>.Filter
                                                                                         .Where(model => model.SessionId == session &&
                                                                                                         request.ExpectedOutputKeys.Contains(model.Key));

                                           var resultWriter = request.ExpectedOutputKeys.Count switch
                                                          {
                                                            1 => new UpdateOneModel<ResultDataModel>(resultFilter,
                                                                                                     resultUpdateDefinition),
                                                            > 1 => new UpdateManyModel<ResultDataModel>(resultFilter,
                                                                                                        resultUpdateDefinition),
                                                            _ => null as WriteModel<ResultDataModel>,
                                                          };

                                           return (TaskDataModel: tdm, WriterModel:resultWriter);
                                         })
                                 .ToList();

    await taskCollection.InsertManyAsync(taskDataModels.Select(tuple => tuple.TaskDataModel),
                                         cancellationToken: cancellationToken);

    await resultCollection.BulkWriteAsync(taskDataModels.Select(tuple => tuple.WriterModel),
                                          new()
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
  IAsyncEnumerable<string> ITableStorage.ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken) => throw new NotImplementedException();

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
  public Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(sessionId.ToString());
    throw new NotImplementedException();
  }
  

  public static Expression<Func<TaskDataModel, bool>> BuildChildrenFilterExpression(IList<string> rootTaskList)
  {
    if (rootTaskList is null || !rootTaskList.Any())
      return model => false;
    if (rootTaskList.Count == 1)
      return model => rootTaskList[0] == model.ParentTaskId || model.Ancestors.Contains(rootTaskList[0]);

    return model => rootTaskList.Contains(model.ParentTaskId) ||
                    // ReSharper disable once ConvertClosureToMethodGroup for better handling by MongoDriver visitor
                    model.Ancestors.Any(parentSubSession => rootTaskList.Contains(parentSubSession));
  }

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


  private bool isInitialized_ = false;
  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);
}