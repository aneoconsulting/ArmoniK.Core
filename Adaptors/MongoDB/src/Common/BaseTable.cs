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

using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

/// <summary>
///   Base class for tables
/// </summary>
/// <typeparam name="TData">Model stored in the table</typeparam>
/// <typeparam name="TModelMapping">Mapping between the model and the document</typeparam>
public abstract class BaseTable<TData, TModelMapping> : IInitializable
  where TModelMapping : IMongoDataModelMapping<TData>, new()
{
  private readonly ActivitySource                                activitySource_;
  private readonly MongoCollectionProvider<TData, TModelMapping> collectionProvider_;
  private readonly bool                                          isReadOnly_;
  private readonly SessionProvider                               sessionProvider_;
  private          bool                                          isInitialized_;

  /// <summary>
  ///   Construct a new table
  /// </summary>
  /// <param name="sessionProvider">Provider for <see cref="IClientSessionHandle" /></param>
  /// <param name="collectionProvider">Provider for <see cref="IMongoCollection{TDocument}" /></param>
  /// <param name="activitySource">Source for the activities when querying the table</param>
  /// <param name="logger">Logger used to produce logs when querying the table</param>
  protected BaseTable(SessionProvider                               sessionProvider,
                      MongoCollectionProvider<TData, TModelMapping> collectionProvider,
                      ActivitySource                                activitySource,
                      ILogger                                       logger)
  {
    sessionProvider_    = sessionProvider;
    collectionProvider_ = collectionProvider;
    activitySource_     = activitySource;
    Logger              = logger;
    isReadOnly_         = false;
    isInitialized_      = false;
  }

  /// <summary>
  ///   Construct a copy of the table adjusting the read preference
  /// </summary>
  /// <param name="baseTable">Source table</param>
  /// <param name="readOnly">Whether the new table is read only</param>
  protected BaseTable(BaseTable<TData, TModelMapping> baseTable,
                      bool                            readOnly)
  {
    sessionProvider_    = baseTable.sessionProvider_;
    collectionProvider_ = baseTable.collectionProvider_;
    activitySource_     = baseTable.activitySource_;
    Logger              = baseTable.Logger;
    isReadOnly_         = readOnly;
    isInitialized_      = true;
  }

  /// <summary>
  ///   Logger used to produce logs when querying the table
  /// </summary>
  protected ILogger Logger { get; }

  private static string TableName
    => $"BaseTable<{typeof(TData).Name}>";


  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();
      await collectionProvider_.Init(cancellationToken)
                               .ConfigureAwait(false);
      collectionProvider_.Get();
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    var result = await HealthCheckResultCombiner.Combine(tag,
                                                         $"{TableName} is not initialized",
                                                         sessionProvider_,
                                                         collectionProvider_)
                                                .ConfigureAwait(false);

    return isInitialized_ && result.Status == HealthStatus.Healthy
             ? HealthCheckResult.Healthy()
             : HealthCheckResult.Unhealthy(result.Description);
  }

  /// <summary>
  ///   Get a Read/Write <see cref="IMongoCollection{TDocument}" /> from the collection provider
  /// </summary>
  /// <returns>Collection</returns>
  /// <exception cref="ConstraintException">Table is read-only</exception>
  protected IMongoCollection<TData> GetCollection()
    => isReadOnly_
         ? throw new ConstraintException($"{TableName} is read-only, but read-write access has been requested")
         : collectionProvider_.Get();

  /// <summary>
  ///   Get a Read-only <see cref="IMongoCollection{TDocument}" /> from the collection provider
  /// </summary>
  /// <remarks>
  ///   If table is marked read-only, read preference is set to secondary-preferred
  /// </remarks>
  /// <returns>Collection</returns>
  protected IMongoCollection<TData> GetReadCollection()
    => isReadOnly_
         ? collectionProvider_.Get()
                              .WithReadPreference(ReadPreference.SecondaryPreferred)
         : collectionProvider_.Get();

  /// <inheritdoc cref="ActivitySource.StartActivity(string, ActivityKind)" />
  protected Activity? StartActivity([CallerMemberName] string name = "")
    => activitySource_.StartActivity(name);

  /// <summary>
  ///   Get a <see cref="IClientSessionHandle" />
  /// </summary>
  /// <returns>session handle</returns>
  protected IClientSessionHandle GetSession()
    => sessionProvider_.Get();
}
