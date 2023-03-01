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
using ArmoniK.Core.Common;
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

  async Task<Result> IResultTable.GetResult(string            sessionId,
                                            string            key,
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
                                   .Where(model => model.Id == Result.GenerateId(sessionId,
                                                                                 key))
                                   .SingleAsync(cancellationToken)
                                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }


  public IAsyncEnumerable<Result> GetResults(string              sessionId,
                                             IEnumerable<string> keys,
                                             CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResults)}");
    activity?.SetTag($"{nameof(GetResults)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return resultCollection.AsQueryable(sessionHandle)
                           .Where(model => keys.Contains(model.Name) && model.SessionId == sessionId)
                           .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IList<ResultStatusCount>> AreResultsAvailableAsync(string              sessionId,
                                                                       IEnumerable<string> keys,
                                                                       CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AreResultsAvailableAsync)}");
    activity?.SetTag($"{nameof(AreResultsAvailableAsync)}_sessionId",
                     sessionId);
    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return await resultCollection.AsQueryable(sessionHandle)
                                 .Where(model => model.SessionId == sessionId && keys.Contains(model.Name))
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
                              string            key,
                              byte[]            smallPayload,
                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");
    activity?.SetTag($"{nameof(SetResult)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(SetResult)}_ownerTaskId",
                     ownerTaskId);
    activity?.SetTag($"{nameof(SetResult)}_key",
                     key);
    var resultCollection = resultCollectionProvider_.Get();

    var res = await resultCollection
                    .UpdateOneAsync(Builders<Result>.Filter.Where(model => model.Name == key && model.OwnerTaskId == ownerTaskId && model.SessionId == sessionId),
                                    Builders<Result>.Update.Set(model => model.Status,
                                                                ResultStatus.Completed)
                                                    .Set(model => model.Data,
                                                         smallPayload),
                                    cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
    if (res.ModifiedCount == 0)
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }

  /// <inheritdoc />
  public async Task SetResult(string            sessionId,
                              string            ownerTaskId,
                              string            key,
                              CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SetResult)}");
    activity?.SetTag($"{nameof(SetResult)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(SetResult)}_ownerTaskId",
                     ownerTaskId);
    activity?.SetTag($"{nameof(SetResult)}_key",
                     key);

    var resultCollection = resultCollectionProvider_.Get();

    var res = await resultCollection.UpdateOneAsync(Builders<Result>.Filter.Where(model => model.Name == key && model.OwnerTaskId == ownerTaskId),
                                                    Builders<Result>.Update.Set(model => model.Status,
                                                                                ResultStatus.Completed),
                                                    cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);
    if (res.MatchedCount == 0)
    {
      throw new ResultNotFoundException($"Key '{key}' not found for '{ownerTaskId}'");
    }
  }

  /// <inheritdoc />
  public async Task<IEnumerable<GetResultStatusReply.Types.IdStatus>> GetResultStatus(IEnumerable<string> ids,
                                                                                      string              sessionId,
                                                                                      CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResultStatus)}");

    var sessionHandle    = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();


    return await resultCollection.AsQueryable(sessionHandle)
                                 .Where(model => ids.Contains(model.Name) && model.SessionId == sessionId)
                                 .Select(model => new GetResultStatusReply.Types.IdStatus
                                                  {
                                                    ResultId = model.Name,
                                                    Status   = model.Status,
                                                  })
                                 .ToListAsync(cancellationToken)
                                 .ConfigureAwait(false);
  }

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

    await resultCollection.BulkWriteAsync(requests.Select(r => new UpdateManyModel<Result>(Builders<Result>.Filter.And(Builders<Result>.Filter.In(model => model.Name,
                                                                                                                                                  r.Keys),
                                                                                                                       Builders<Result>.Filter
                                                                                                                                       .Eq(model => model.OwnerTaskId,
                                                                                                                                           oldTaskId),
                                                                                                                       Builders<Result>.Filter
                                                                                                                                       .Eq(model => model.SessionId,
                                                                                                                                           sessionId)),
                                                                                           Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                                                       r.NewTaskId))),
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

    var result = await resultCollection.DeleteOneAsync(model => model.Name == key && model.SessionId == session,
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
                                                 .Select(model => model.Id)
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
