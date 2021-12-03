// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Google.Protobuf.WellKnownTypes;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  [PublicAPI]
  public class LeaseProvider : ILeaseProvider
  {
    private readonly MongoCollectionProvider<LeaseDataModel> leaseCollectionProvider_;
    private readonly SessionProvider                         sessionProvider_;
    private readonly ILogger<LeaseProvider>                  logger_;

    public LeaseProvider(IOptions<Options.LeaseProvider>         options,
                         MongoCollectionProvider<LeaseDataModel> leaseCollectionProvider,
                         SessionProvider                         sessionProvider,
                         ILogger<LeaseProvider>                  logger)
    {
      AcquisitionPeriod        = options.Value.AcquisitionPeriod;
      AcquisitionDuration      = options.Value.AcquisitionDuration;
      leaseCollectionProvider_ = leaseCollectionProvider;
      sessionProvider_         = sessionProvider;
      logger_                  = logger;
    }

    /// <inheritdoc />
    public TimeSpan AcquisitionPeriod { get; }

    /// <inheritdoc />
    public TimeSpan AcquisitionDuration { get; }

    /// <inheritdoc />
    public async Task<Lease> TryAcquireLease(TaskId id, CancellationToken cancellationToken = default)
    {
      var _ = logger_.LogFunction();
      logger_.LogDebug("Trying to acquire lease for task {id}", id);
      var key             = id.ToPrintableId();
      var leaseId         = Guid.NewGuid().ToString();
      var leaseCollection = await leaseCollectionProvider_.GetAsync();

      var updateDefinitionBuilder = new UpdateDefinitionBuilder<LeaseDataModel>();
      var updateDefinition = updateDefinitionBuilder.SetOnInsert(ldm => ldm.ExpiresAt, DateTime.UtcNow + AcquisitionDuration)
                                                    .SetOnInsert(ldm => ldm.Lock, leaseId)
                                                    .SetOnInsert(ldm => ldm.Key, key);

      var res = await leaseCollection.FindOneAndUpdateAsync<LeaseDataModel>(await sessionProvider_.GetAsync(),
                                                                            ldm => ldm.Key == key,
                                                                            updateDefinition,
                                                                            new FindOneAndUpdateOptions<LeaseDataModel>
                                                                            {
                                                                              IsUpsert       = true,
                                                                              ReturnDocument = ReturnDocument.After,
                                                                            },
                                                                            cancellationToken);
      if (leaseId == res.Lock)
      {
        logger_.LogInformation("Lease {leaseId} acquired for task {id}", leaseId, id);
        return new Lease { ExpirationDate = Timestamp.FromDateTime(res.ExpiresAt), Id = id, LeaseId = leaseId };
      }

      logger_.LogWarning("Could not acquire lease for task {id}", id);
      return new Lease { Id = id, LeaseId = string.Empty, ExpirationDate = new Timestamp() };
    }

    /// <inheritdoc />
    public async Task<Lease> TryRenewLease(TaskId id, string leaseId, CancellationToken cancellationToken = default)
    {
      var _ = logger_.LogFunction();
      logger_.LogDebug("Trying to renew lease {leaseId} for task {id}", leaseId, id);
      var key             = id.ToPrintableId();
      var leaseCollection = await leaseCollectionProvider_.GetAsync();

      var updateDefinition = new UpdateDefinitionBuilder<LeaseDataModel>().Set(ldm => ldm.ExpiresAt,
                                                                               DateTime.UtcNow + AcquisitionDuration);

      var res = await leaseCollection.FindOneAndUpdateAsync<LeaseDataModel>(await sessionProvider_.GetAsync(),
                                                                            ldm => ldm.Key == key && ldm.Lock == leaseId,
                                                                            updateDefinition,
                                                                            new FindOneAndUpdateOptions<LeaseDataModel>
                                                                            {
                                                                              ReturnDocument = ReturnDocument.After,
                                                                              MaxTime = TimeSpan.FromSeconds(1),
                                                                            },
                                                                            cancellationToken);
      logger_.LogDebug("Obtained LeaseResult {res}", res);
      if (leaseId == res.Lock)
      {
        logger_.LogInformation("Lease {leaseId} renewed for task {id}", leaseId, id);
        return new Lease { Id = id, LeaseId = leaseId, ExpirationDate = Timestamp.FromDateTime(res.ExpiresAt) };
      }

      logger_.LogInformation("Could not renew lease {leaseId} for task {id}", leaseId, id);
      return new Lease { Id = id, LeaseId = string.Empty, ExpirationDate = new Timestamp() };
    }

    /// <inheritdoc />
    public async Task ReleaseLease(TaskId id, string leaseId, CancellationToken cancellationToken = default)
    {
      var _ = logger_.LogFunction();
      logger_.LogDebug("Trying to release lease {leaseId} for task {id}", leaseId, id);
      var key             = id.ToPrintableId();
      var leaseCollection = await leaseCollectionProvider_.GetAsync();

      var res = await leaseCollection.FindOneAndDeleteAsync(await sessionProvider_.GetAsync(),
                                                            ldm => ldm.Key == key && ldm.Lock == leaseId,
                                                            cancellationToken: cancellationToken);
      if (res is null)
        logger_.LogWarning("Could not release lease {leaseId} for task {id}", leaseId, id);
    }
  }
}