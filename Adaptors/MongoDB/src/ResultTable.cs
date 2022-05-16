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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

using Result = ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Result;

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
  public async Task Create(IEnumerable<Core.Common.Storage.Result> results,
                           CancellationToken                       cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(Create)}");

    var resultCollection = resultCollectionProvider_.Get();

    try
    {
      var writeResult = await resultCollection.BulkWriteAsync(results.Select(result => new InsertOneModel<Result>(result.ToResultDataModel())),
                                                              new BulkWriteOptions
                                                              {
                                                                IsOrdered = false,
                                                              },
                                                              cancellationToken)
                                              .ConfigureAwait(false);
    }
    catch
    {
      throw new ArmoniKException("Key already exists");
    }
  }

  /// <inheritdoc />
  public async Task<Core.Common.Storage.Result> GetResult(string            sessionId,
                                                          string            key,
                                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(GetResult)}");
    activity?.SetTag($"{nameof(GetResult)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(GetResult)}_key",
                     key);
    var sessionHandle = sessionProvider_.Get();
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

  /// <inheritdoc />
  public async Task<bool> AreResultsAvailableAsync(string              sessionId,
                                                   IEnumerable<string> keys,
                                                   CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AreResultsAvailableAsync)}");
    activity?.SetTag($"{nameof(AreResultsAvailableAsync)}_sessionId",
                     sessionId);
    var sessionHandle = sessionProvider_.Get();
    var resultCollection = resultCollectionProvider_.Get();

    return !await resultCollection.AsQueryable(sessionHandle)
                                  .AnyAsync(model => model.Status != ResultStatus.Completed && model.SessionId == sessionId && keys.Contains(model.Name),
                                            cancellationToken)
                                  .ConfigureAwait(false);
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
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }

  /// <inheritdoc />
  public async Task ChangeResultOwnership(string              sessionId,
                                          IEnumerable<string> keys,
                                          string              oldTaskId,
                                          string              newTaskId,
                                          CancellationToken   cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ChangeResultOwnership)}");
    activity?.SetTag($"{nameof(ChangeResultOwnership)}_sessionId",
                     sessionId);
    activity?.SetTag($"{nameof(ChangeResultOwnership)}_oldTaskId",
                     oldTaskId);
    activity?.SetTag($"{nameof(ChangeResultOwnership)}_newTaskId",
                     newTaskId);
    activity?.SetTag($"{nameof(ChangeResultOwnership)}_keys",
                     keys);
    if (keys.Any())
    {
      var resultCollection = resultCollectionProvider_.Get();

      var result = await resultCollection.UpdateManyAsync(model => model.OwnerTaskId == oldTaskId && keys.Contains(model.Name) && model.SessionId == sessionId,
                                                          Builders<Result>.Update.Set(model => model.OwnerTaskId,
                                                                                      newTaskId),
                                                          cancellationToken: cancellationToken)
                                         .ConfigureAwait(false);
      if (result.ModifiedCount != keys.Count())
      {
        throw new ArmoniKException("The number of modified values should correspond to the number of keys provided");
      }
    }
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
    var sessionHandle = sessionProvider_.Get();
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
  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      sessionProvider_.Get();
      resultCollectionProvider_.Get();
      isInitialized_ = true;
    }
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);
}
