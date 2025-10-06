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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

/// <summary>
///   Provides a MongoDB collection for data operations, implementing initialization and async initialization.
/// </summary>
/// <typeparam name="TData">The type of data stored in the MongoDB collection.</typeparam>
/// <typeparam name="TModelMapping">The type of the model mapping that defines the MongoDB collection mapping.</typeparam>
/// <remarks>
///   This class is responsible for providing access to a MongoDB collection and ensuring it is properly initialized.
/// </remarks>
[PublicAPI]
public class MongoCollectionProvider<TData, TModelMapping> : IInitializable, IAsyncInitialization<IMongoCollection<TData>>
  where TModelMapping : IMongoDataModelMapping<TData>, new()
{
  private bool                     isInitialized_;
  private IMongoCollection<TData>? mongoCollection_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="MongoCollectionProvider{TData, TModelMapping}" /> class.
  /// </summary>
  /// <param name="options">The MongoDB options used for configuration, including data retention settings.</param>
  /// <param name="sessionProvider">The session provider used to manage MongoDB sessions.</param>
  /// <param name="mongoDatabase">The MongoDB database instance to which the collection belongs.</param>
  /// <param name="logger">The logger instance used for logging purposes.</param>
  /// <param name="cancellationToken">
  ///   A cancellation token to observe while waiting for the initialization to complete
  ///   (optional).
  /// </param>
  /// <exception cref="ArgumentOutOfRangeException">
  ///   Thrown when the data retention option is not defined (i.e., set to zero).
  /// </exception>
  public MongoCollectionProvider(Options.MongoDB                  options,
                                 SessionProvider                  sessionProvider,
                                 IMongoDatabase                   mongoDatabase,
                                 ILogger<IMongoCollection<TData>> logger,
                                 CancellationToken                cancellationToken = default)
  {
    if (options.DataRetention == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.MongoDB.DataRetention)} is not defined.");
    }

    Initialization = InitializeAsync(options,
                                     sessionProvider,
                                     mongoDatabase,
                                     logger,
                                     cancellationToken);
  }

  /// <inheritdoc />
  public Task<IMongoCollection<TData>> Initialization { get; private set; }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult
                                                                                   .Unhealthy($"Mongo Collection<{typeof(TData)}> not initialized yet.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_ && mongoCollection_ is not null
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy($"Mongo Collection<{typeof(TData)}> not initialized yet.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      mongoCollection_ = await Initialization.ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  private static async Task<IMongoCollection<TData>> InitializeAsync(Options.MongoDB                  options,
                                                                     SessionProvider                  sessionProvider,
                                                                     IMongoDatabase                   mongoDatabase,
                                                                     ILogger<IMongoCollection<TData>> logger,
                                                                     CancellationToken                cancellationToken = default)
  {
    var        model         = new TModelMapping();
    Exception? lastException = null;

    for (var collectionRetry = 1; collectionRetry < options.MaxRetries; collectionRetry++)
    {
      lastException = null;
      try
      {
        await mongoDatabase.CreateCollectionAsync(model.CollectionName,
                                                  null,
                                                  cancellationToken)
                           .ConfigureAwait(false);
        break;
      }
      catch (MongoCommandException ex) when (ex.CodeName == "NamespaceExists")
      {
        logger.LogDebug(ex,
                        "Use already existing instance of Collection {CollectionName}",
                        model.CollectionName);
        break;
      }
      catch (Exception ex)
      {
        lastException = ex;
        logger.LogDebug(ex,
                        "Retrying to create Collection {CollectionName}",
                        model.CollectionName);
        await Task.Delay(1000 * collectionRetry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    if (lastException is not null)
    {
      throw new TimeoutException($"Create {model.CollectionName}: Max retries reached",
                                 lastException);
    }

    var output = mongoDatabase.GetCollection<TData>(model.CollectionName);
    await sessionProvider.Init(cancellationToken)
                         .ConfigureAwait(false);
    var session = sessionProvider.Get();

    for (var indexRetry = 1; indexRetry < options.MaxRetries; indexRetry++)
    {
      lastException = null;
      try
      {
        await model.InitializeIndexesAsync(session,
                                           output,
                                           options)
                   .ConfigureAwait(false);
        break;
      }
      catch (MongoCommandException ex) when (ex.CodeName == "IndexOptionsConflict")
      {
        logger.LogWarning(ex,
                          "Index options conflict for {CollectionName} collection",
                          model.CollectionName);
        break;
      }
      catch (Exception ex)
      {
        lastException = ex;
        logger.LogDebug(ex,
                        "Retrying to Initialize indexes for {CollectionName} collection",
                        model.CollectionName);
        await Task.Delay(1000 * indexRetry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    if (options.Sharding)
    {
      for (var indexRetry = 1; indexRetry < options.MaxRetries; indexRetry++)
      {
        lastException = null;
        try
        {
          await model.ShardCollectionAsync(session,
                                           options)
                     .ConfigureAwait(false);
          break;
        }
        catch (Exception ex)
        {
          lastException = ex;
          logger.LogDebug(ex,
                          "Retrying to shard {CollectionName} collection",
                          model.CollectionName);
          await Task.Delay(1000 * indexRetry,
                           cancellationToken)
                    .ConfigureAwait(false);
        }
      }
    }

    if (lastException is not null)
    {
      throw new TimeoutException($"Init Index or shard for {model.CollectionName}: Max retries reached",
                                 lastException);
    }

    return output;
  }

  /// <summary>
  ///   Retrieves the MongoDB collection for data operations.
  /// </summary>
  /// <returns>The MongoDB collection of type <see cref="IMongoCollection{TData}" />.</returns>
  /// <exception cref="InvalidOperationException">
  ///   Thrown when the MongoDB collection has not been initialized.
  ///   Ensure that the <see cref="InitializeAsync" /> method has been called before accessing the collection.
  /// </exception>
  public IMongoCollection<TData> Get()
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException("Mongo Collection has not been initialized; call Init method first");
    }

    return mongoCollection_!;
  }
}
