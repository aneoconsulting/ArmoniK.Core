// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <inheritdoc cref="ArmoniK.Core.Common.Storage.IResultTable" />
public class ResultTable : BaseTable<Result, ResultDataModelMapping>, IResultTable
{
  /// <inheritdoc />
  public ResultTable(SessionProvider                                         sessionProvider,
                     MongoCollectionProvider<Result, ResultDataModelMapping> resultCollectionProvider,
                     ActivitySource                                          activitySource,
                     ILogger<ResultTable>                                    logger)
    : base(sessionProvider,
           resultCollectionProvider,
           activitySource,
           logger)
  {
  }

  /// <inheritdoc />
  private ResultTable(ResultTable resultTable,
                      bool        readOnly)
    : base(resultTable,
           readOnly)
  {
  }

  /// <inheritdoc />
  public IResultTable Secondary
    => new ResultTable(this,
                       true);

  /// <inheritdoc />
  public async Task Create(ICollection<Result> results,
                           CancellationToken   cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

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

        Logger.LogDebug("Created {results}",
                        results);
      }
    }
    catch (Exception e)
    {
      throw new ArmoniKException("Key already exists",
                                 e);
    }
  }

  /// <inheritdoc />
  public async Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                        CancellationToken                        cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    if (!dependencies.Any())
    {
      return;
    }

    Logger.LogDebug("Add task dependencies: {@Dependencies}",
                    dependencies);

    var dependencyRequests = dependencies.Select(dependency => new UpdateManyModel<Result>(Builders<Result>.Filter.Where(result => dependency.Key == result.ResultId),
                                                                                           Builders<Result>.Update.AddToSetEach(result => result.DependentTasks,
                                                                                                                                dependency.Value)));

    var writeResult = await resultCollection.BulkWriteAsync(dependencyRequests,
                                                            new BulkWriteOptions
                                                            {
                                                              IsOrdered = false,
                                                            },
                                                            cancellationToken)
                                            .ConfigureAwait(false);

    if (writeResult.MatchedCount != dependencies.Count)
    {
      throw new ResultNotFoundException($"One of the input result was not found: expected: {dependencies.Count}, found: {writeResult.MatchedCount}");
    }
  }

  async Task<Result> IResultTable.GetResult(string            resultId,
                                            CancellationToken cancellationToken)
  {
    using var activity         = StartActivity();
    var       sessionHandle    = GetSession();
    var       resultCollection = GetReadCollection();
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

  /// <inheritdoc />
  public async Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                                    Expression<Func<Result, object?>> orderField,
                                                                                    bool                              ascOrder,
                                                                                    int                               page,
                                                                                    int                               pageSize,
                                                                                    CancellationToken                 cancellationToken = default)
  {
    using var _                = Logger.LogFunction();
    using var activity         = StartActivity();
    var       sessionHandle    = GetSession();
    var       resultCollection = GetReadCollection();


    var resultList = Task.FromResult(new List<Result>());
    if (pageSize > 0)
    {
      var findFluent1 = resultCollection.Find(sessionHandle,
                                              filter);

      var ordered = ascOrder
                      ? findFluent1.SortBy(orderField)
                      : findFluent1.SortByDescending(orderField);

      resultList = ordered.Skip(page * pageSize)
                          .Limit(pageSize)
                          .ToListAsync(cancellationToken);
    }

    // Find needs to be duplicated, otherwise, the count is computed on a single page, and not the whole collection
    var resultCount = resultCollection.CountDocumentsAsync(sessionHandle,
                                                           filter,
                                                           cancellationToken: cancellationToken);

    return (await resultList.ConfigureAwait(false), (int)await resultCount.ConfigureAwait(false));
  }

  /// <inheritdoc />
  public async Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                                     CancellationToken                             cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    if (!requests.Any())
    {
      return;
    }

    var res = await resultCollection.BulkWriteAsync(requests.Select(r => new UpdateOneModel<Result>(Builders<Result>.Filter.Eq(model => model.ResultId,
                                                                                                                               r.resultId),
                                                                                                    Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                                                                r.taskId))),
                                                    cancellationToken: cancellationToken)
                                    .ConfigureAwait(false);

    if (res.ModifiedCount != requests.Count)
    {
      throw new ResultNotFoundException("One of the requested result was not found");
    }
  }

  /// <inheritdoc />
  public async Task ChangeResultOwnership(string                                                 oldTaskId,
                                          IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                          CancellationToken                                      cancellationToken)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    await resultCollection.BulkWriteAsync(requests.Select(r =>
                                                          {
                                                            return new UpdateManyModel<Result>(Builders<Result>.Filter.And(Builders<Result>.Filter.In(model
                                                                                                                                                        => model
                                                                                                                                                          .ResultId,
                                                                                                                                                      r.Keys),
                                                                                                                           Builders<Result>.Filter
                                                                                                                                           .Eq(model
                                                                                                                                                 => model.OwnerTaskId,
                                                                                                                                               oldTaskId)),
                                                                                               Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                                                           r.NewTaskId));
                                                          }),
                                          cancellationToken: cancellationToken)
                          .ConfigureAwait(false);
  }


  /// <inheritdoc />
  public async Task DeleteResult(string            key,
                                 CancellationToken cancellationToken = default)
  {
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeleteResult)}_key",
                     key);
    var resultCollection = GetCollection();

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
  public async Task DeleteResults(ICollection<string> results,
                                  CancellationToken   cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    await resultCollection.DeleteManyAsync(model => results.Contains(model.ResultId),
                                           cancellationToken)
                          .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                           Expression<Func<Result, T>>    convertor,
                                           CancellationToken              cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       sessionHandle    = GetSession();
    var       resultCollection = GetReadCollection();

    return resultCollection.Find(sessionHandle,
                                 filter)
                           .Project(convertor)
                           .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeleteResults(string            sessionId,
                                  CancellationToken cancellationToken = default)
  {
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeleteResults)}_sessionId",
                     sessionId);
    var resultCollection = GetCollection();

    await resultCollection.DeleteManyAsync(model => model.SessionId == sessionId,
                                           cancellationToken)
                          .ConfigureAwait(false);

    Logger.LogInformation("Deleted results from {sessionId}",
                          sessionId);
  }

  /// <inheritdoc />
  public async Task<Result> UpdateOneResult(string                                       resultId,
                                            Core.Common.Storage.UpdateDefinition<Result> updates,
                                            CancellationToken                            cancellationToken = default)
  {
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeleteResult)}_resultId",
                     resultId);
    var resultCollection = GetCollection();

    var updateDefinition = new UpdateDefinitionBuilder<Result>().Combine();

    foreach (var (selector, newValue) in updates.Setters)
    {
      updateDefinition = updateDefinition.Set(selector,
                                              newValue);
    }

    var filter = new FilterDefinitionBuilder<Result>().Where(x => x.ResultId == resultId);

    var result = await resultCollection.FindOneAndUpdateAsync(filter,
                                                              updateDefinition,
                                                              new FindOneAndUpdateOptions<Result>
                                                              {
                                                                ReturnDocument = ReturnDocument.Before,
                                                              },
                                                              cancellationToken)
                                       .ConfigureAwait(false);

    return result ?? throw new ResultNotFoundException($"Result not found {resultId}");
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyResults(Expression<Func<Result, bool>>               filter,
                                            Core.Common.Storage.UpdateDefinition<Result> updates,
                                            CancellationToken                            cancellationToken = default)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    var updateDefinition = new UpdateDefinitionBuilder<Result>().Combine();

    foreach (var (selector, newValue) in updates.Setters)
    {
      updateDefinition = updateDefinition.Set(selector,
                                              newValue);
    }

    var result = await resultCollection.UpdateManyAsync(filter,
                                                        updateDefinition,
                                                        cancellationToken: cancellationToken)
                                       .ConfigureAwait(false);

    return result.MatchedCount;
  }

  /// <inheritdoc />
  async Task<long> IResultTable.BulkUpdateResults(IEnumerable<(Expression<Func<Result, bool>> filter, Core.Common.Storage.UpdateDefinition<Result> updates)> bulkUpdates,
                                                  CancellationToken cancellationToken)
  {
    using var activity         = StartActivity();
    var       resultCollection = GetCollection();

    var requests = bulkUpdates.Select(item =>
                                      {
                                        var updateDefinition = new UpdateDefinitionBuilder<Result>().Combine();

                                        foreach (var (selector, newValue) in item.updates.Setters)
                                        {
                                          updateDefinition = updateDefinition.Set(selector,
                                                                                  newValue);
                                        }

                                        return new UpdateManyModel<Result>(Builders<Result>.Filter.Where(item.filter),
                                                                           updateDefinition);
                                      })
                              .AsICollection();

    var updateResult = await resultCollection.BulkWriteAsync(requests,
                                                             new BulkWriteOptions
                                                             {
                                                               IsOrdered = false,
                                                             },
                                                             cancellationToken)
                                             .ConfigureAwait(false);

    Logger.LogDebug("Bulk update results matching {@updates}",
                    bulkUpdates);

    return updateResult.MatchedCount;
  }

  /// <inheritdoc />
  ILogger IResultTable.Logger
    => base.Logger;
}
