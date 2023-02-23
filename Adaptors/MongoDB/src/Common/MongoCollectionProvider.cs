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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

[PublicAPI]
public class MongoCollectionProvider<TData, TModelMapping> : IInitializable, IAsyncInitialization<IMongoCollection<TData>>
  where TModelMapping : IMongoDataModelMapping<TData>, new()
{
  private          IMongoCollection<TData>? mongoCollection_;
  private          bool                     isInitialized_;
  public MongoCollectionProvider(Options.MongoDB   options,
                                 SessionProvider   sessionProvider,
                                 IMongoDatabase    mongoDatabase,
                                 CancellationToken cancellationToken = default)
  {
    if (options.DataRetention == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.MongoDB.DataRetention)} is not defined.");
    }

    Initialization = InitializeAsync(options,
                                     sessionProvider,
                                     mongoDatabase,
                                     cancellationToken);
  }

  public Task<IMongoCollection<TData>> Initialization { get; private set; }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    switch (tag)
    {
      case HealthCheckTag.Startup:
      case HealthCheckTag.Readiness:
        return Task.FromResult(isInitialized_
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("MongoCollection not initialized yet."));
      case HealthCheckTag.Liveness:
        return Task.FromResult(isInitialized_ && mongoCollection_ is null
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("MongoCollection not initialized yet."));
      default:
        throw new ArgumentOutOfRangeException(nameof(tag),
                                              tag,
                                              null);
    }
  }

  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      mongoCollection_ = await Initialization.ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  private static async Task<IMongoCollection<TData>> InitializeAsync(Options.MongoDB   options,
                                                                     SessionProvider   sessionProvider,
                                                                     IMongoDatabase    mongoDatabase,
                                                                     CancellationToken cancellationToken = default)
  {
    var model = new TModelMapping();
    try
    {
      await mongoDatabase.CreateCollectionAsync(model.CollectionName,
                                                new CreateCollectionOptions<TData>
                                                {
                                                  ExpireAfter = options.DataRetention,
                                                },
                                                cancellationToken)
                         .ConfigureAwait(false);
    }
    catch (MongoCommandException)
    {
    }

    var output = mongoDatabase.GetCollection<TData>(model.CollectionName);
    await sessionProvider.Init(cancellationToken)
                         .ConfigureAwait(false);
    var session = sessionProvider.Get();
    try
    {
      await model.InitializeIndexesAsync(session,
                                         output)
                 .ConfigureAwait(false);
    }
    catch (MongoCommandException)
    {
    }

    return output;
  }

  public IMongoCollection<TData> Get()
  {
    if (!isInitialized_)
    {
      throw new ArmoniKException($"Mongo Collection has not been initialized; call Init method first");
    }
    else
    {
      return mongoCollection_!;
    }
  }
}
