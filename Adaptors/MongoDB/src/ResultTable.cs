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
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

public class ResultTable : IResultTable
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;

  private bool isInitialized_;

  public ResultTable(SessionProvider                                         sessionProvider,
                     MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider,
                     ActivitySource                                          activitySource,
                     ILogger<ResultTable>                                    logger)
  {
    sessionProvider_          = sessionProvider;
    resultCollectionProvider_ = resultCollectionProvider;
    activitySource_           = activitySource;
    Logger                    = logger;
  }

  /// <inheritdoc />
  public async Task Create(IEnumerable<Result> results,
                           CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(Create)}");

    var resultCollection = resultCollectionProvider_.Get();

    try
    {
      if (results.Any())
      {
        await resultCollection.BulkWriteAsync(results.Select(result => new InsertOneModel<Result>(result)),
                                              new BulkWriteOptions
                                              {
                                                IsOrdered = false,
                                              },
                                              cancellationToken)
                              .ConfigureAwait(false);
      }
    }
    catch (Exception e)
    {
      throw new ArmoniKException("Key already exists",
                                 e);
    }
  }

  /// <inheritdoc />
  public async Task AddTaskDependency(string              sessionId,
                                      ICollection<string> resultIds,
                                      ICollection<string> taskIds,
                                      CancellationToken   cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AddTaskDependency)}");
    activity?.SetTag($"{nameof(AddTaskDependency)}_sessionId",
                     sessionId);

    var resultCollection = resultCollectionProvider_.Get();

    var result = await resultCollection.UpdateManyAsync(Builders<Result>.Filter.Where(model => resultIds.Contains(model.ResultId)),
                                                        Builders<Result>.Update.AddToSetEach(model => model.DependentTasks,
                                                                                             taskIds),
                                                        cancellationToken: cancellationToken)
                                       .ConfigureAwait(false);

    if (result.ModifiedCount != resultIds.Count)
    {
      throw new ResultNotFoundException("One of the input result was not found");
    }
  }

  /// <inheritdoc />
  public async Task<IEnumerable<string>> GetDependents(string            sessionId,
                                                       string            resultId,
                                                       CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetDependents)}");
    activity?.SetTag($"{nameof(GetDependents)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(GetDependents)}_resultId",
                     resultId);
    var resultCollection = resultCollectionProvider_.Get();

    try
    {
      return await resultCollection.AsQueryable()
                                   .Where(result => result.ResultId == resultId)
                                   .Select(result => result.DependentTasks)
                                   .SingleAsync(cancellationToken)
                                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ResultNotFoundException($"Key '{resultId}' not found");
    }
  }

  async Task<Result> IResultTable.GetResult(string            sessionId,
                                            string            resultId,
                                            CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(IResultTable.GetResult)}");
    activity?.SetTag($"{nameof(IResultTable.GetResult)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();
    try
    {
      return await resultCollection.AsQueryable(sessionHandle)
                                   .Where(model => model.ResultId == resultId)
                                   .SingleAsync(cancellationToken)
                                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ResultNotFoundException($"Key '{resultId}' not found");
    }
  }


  public IAsyncEnumerable<Result> GetResults(string              sessionId,
                                             IEnumerable<string> resultIds,
                                             CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResults)}");
    activity?.SetTag($"{nameof(GetResults)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return resultCollection.AsQueryable(sessionHandle)
                           .Where(model => resultIds.Contains(model.ResultId))
                           .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<ResultStatusCount>> AreResultsAvailableAsync(string              sessionId,
                                                                             IEnumerable<string> resultIds,
                                                                             CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AreResultsAvailableAsync)}");
    activity?.SetTag($"{nameof(AreResultsAvailableAsync)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return await resultCollection.AsQueryable(sessionHandle)
                                 .Where(model => resultIds.Contains(model.ResultId))
                                 .GroupBy(model => model.Status)
                                 .Select(models => new ResultStatusCount(models.Key,
                                                                         models.Count()))
                                 .ToListAsync(cancellationToken)
                                 .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                                    Expression<Func<Result, object?>> orderField,
                                                                                    bool                              ascOrder,
                                                                                    int                               page,
                                                                                    int                               pageSize,
                                                                                    CancellationToken                 cancellationToken = default)
  {
    using var _                = Logger.LogFunction();
    using var activity         = activitySource_.StartActivity($"{nameof(ListResultsAsync)}");
    var       sessionHandle    = sessionProvider_.Get();
    var       resultCollection = resultCollectionProvider_.Get();


    var queryable = resultCollection.AsQueryable(sessionHandle)
                                    .Where(filter);

    var ordered = ascOrder
                    ? queryable.OrderBy(orderField)
                    : queryable.OrderByDescending(orderField);

    return (await ordered.Skip(page * pageSize)
                         .Take(pageSize)
                         .ToListAsync(cancellationToken) // todo : do not create list there but pass cancellation token
                         .ConfigureAwait(false), await ordered.CountAsync(cancellationToken)
                                                              .ConfigureAwait(false));
  }

  /// <inheritdoc />
  public async Task SetResult(string            sessionId,
                              string            ownerTaskId,
                              string            resultId,
                              byte[]            smallPayload,
                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");
    activity?.SetTag($"{nameof(SetResult)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(SetResult)}_ownerTaskId",
                     ownerTaskId);
    activity?.SetTag($"{nameof(SetResult)}_key",
                     resultId);
    var resultCollection = resultCollectionProvider_.Get();

    Logger.LogInformation("Update result {resultId} to completed",
                          key);

    var res = await resultCollection.UpdateOneAsync(Builders<Result>.Filter.Where(model => model.Id == Result.GenerateId(sessionId,
                                                                                                                         key) && model.OwnerTaskId == ownerTaskId),
                                                    Builders<Result>.Update.Set(model => model.Status,
                                                                                ResultStatus.Completed)
                                                                    .Set(model => model.Data,
                                                                         smallPayload),
                                                    cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);
    if (res.ModifiedCount == 0)
    {
      throw new ResultNotFoundException($"Key '{resultId}' not found");
    }
  }

  /// <inheritdoc />
  public async Task SetResult(string            sessionId,
                              string            ownerTaskId,
                              string            resultId,
                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");
    activity?.SetTag($"{nameof(SetResult)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(SetResult)}_ownerTaskId",
                     ownerTaskId);
    activity?.SetTag($"{nameof(SetResult)}_key",
                     resultId);

    var resultCollection = resultCollectionProvider_.Get();

    Logger.LogInformation("Update result {resultId} to completed",
                          key);

    var res = await resultCollection.UpdateOneAsync(Builders<Result>.Filter.Where(model => model.Id == Result.GenerateId(sessionId,
                                                                                                                         key) && model.OwnerTaskId == ownerTaskId),
                                                    Builders<Result>.Update.Set(model => model.Status,
                                                                                ResultStatus.Completed),
                                                    cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);
    if (res.MatchedCount == 0)
    {
      throw new ResultNotFoundException($"Key '{resultId}' not found for '{ownerTaskId}'");
    }
  }

  /// <inheritdoc />
  public async Task<IEnumerable<GetResultStatusReply.Types.IdStatus>> GetResultStatus(IEnumerable<string> keys,
                                                                                      string              sessionId,
                                                                                      CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResultStatus)}");

    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return await resultCollection.AsQueryable(sessionHandle)
                                 .Where(model => keys.Contains(model.ResultId))
                                 .Select(model => new GetResultStatusReply.Types.IdStatus
                                                  {
                                                    ResultId = model.ResultId,
                                                    Status   = model.Status,
                                                  })
                                 .ToListAsync(cancellationToken)
                                 .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task SetTaskOwnership(string                                        sessionId,
                                     ICollection<(string resultId, string taskId)> requests,
                                     CancellationToken                             cancellationToken = default)
  {
    using var activity         = activitySource_.StartActivity($"{nameof(SetTaskOwnership)}");
    var       resultCollection = resultCollectionProvider_.Get();

    var res = await resultCollection.BulkWriteAsync(requests.Select(r => new UpdateOneModel<Result>(Builders<Result>.Filter.Eq(model => model.ResultId,
                                                                                                                               r.resultId),
                                                                                                    Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                                                                r.taskId))),
                                                    cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);

    if (res.ModifiedCount != requests.Count())
    {
      throw new ResultNotFoundException("One of the requested result was not found");
    }
  }

  /// <inheritdoc />
  public async Task AbortTaskResults(string            sessionId,
                                     string            ownerTaskId,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AbortTaskResults)}");

    var resultCollection = resultCollectionProvider_.Get();

    var res = await resultCollection.UpdateManyAsync(Builders<Result>.Filter.Where(model => model.SessionId == sessionId && model.OwnerTaskId == ownerTaskId),
                                                     Builders<Result>.Update.Set(model => model.Status,
                                                                                 ResultStatus.Aborted),
                                                     cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task ChangeResultOwnership(string                                                 sessionId,
                                          string                                                 oldTaskId,
                                          IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                          CancellationToken                                      cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ChangeResultOwnership)}");
    activity?.SetTag($"{nameof(ChangeResultOwnership)}_sessionId",
                     sessionId);

    var resultCollection = resultCollectionProvider_.Get();

    await resultCollection.BulkWriteAsync(requests.Select(r =>
                                                          {
                                                            return new UpdateManyModel<Result>(Builders<Result>.Filter.And(Builders<Result>.Filter.In(model
                                                                                                                                                        => model
                                                                                                                                                          .ResultId,
                                                                                                                                                      r.Keys),
                                                                                                                           Builders<Result>.Filter
                                                                                                                                           .Eq(model
                                                                                                                                                 => model.OwnerTaskId,
                                                                                                                                               oldTaskId),
                                                                                                                           Builders<Result>.Filter
                                                                                                                                           .Eq(model => model.SessionId,
                                                                                                                                               sessionId)),
                                                                                               Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                                                           r.NewTaskId));
                                                          }),
                                          cancellationToken: cancellationToken)
                          .ConfigureAwait(false);
  }


  /// <inheritdoc />
  public async Task DeleteResult(string            session,
                                 string            key,
                                 CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteResult)}");
    activity?.SetTag($"{nameof(DeleteResult)}_sessionId",
                     session);
    activity?.SetTag($"{nameof(DeleteResult)}_key",
                     key);
    var resultCollection = resultCollectionProvider_.Get();

    var result = await resultCollection.DeleteOneAsync(model => model.ResultId == key,
                                                       cancellationToken)
                                       .ConfigureAwait(false);

    if (result.DeletedCount == 0)
    {
      throw new ResultNotFoundException($"Result '{key}' not found.");
    }

    if (result.DeletedCount > 1)
    {
      throw new ArmoniKException("Multiple tasks deleted");
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListResultsAsync(string                                     sessionId,
                                                         [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ListResultsAsync)}");
    activity?.SetTag($"{nameof(ListResultsAsync)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    await foreach (var result in resultCollection.AsQueryable(sessionHandle)
                                                 .Where(model => model.SessionId == sessionId)
                                                 .Select(model => model.ResultId)
                                                 .ToAsyncEnumerable()
                                                 .WithCancellation(cancellationToken)
                                                 .ConfigureAwait(false))
    {
      yield return result;
    }
  }

  /// <inheritdoc />
  public async Task DeleteResults(string            sessionId,
                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeleteResults)}");
    activity?.SetTag($"{nameof(DeleteResults)}_sessionId",
                     sessionId);
    var resultCollection = resultCollectionProvider_.Get();

    await resultCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                           cancellationToken)
                          .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      await resultCollectionProvider_.Init(cancellationToken)
                                     .ConfigureAwait(false);
      resultCollectionProvider_.Get();
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());
}
