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

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  [PublicAPI]
  public class LeaseProvider : ILeaseProvider
  {
    private readonly IMongoCollection<LeaseDataModel> leaseCollection_;
    private readonly IClientSessionHandle             sessionHandle_;

    public LeaseProvider(TimeSpan acquisitionPeriod, 
                         TimeSpan acquisitionDuration, 
                         IMongoCollection<LeaseDataModel> leaseCollection, 
                         IClientSessionHandle sessionHandle)
    {
      AcquisitionPeriod = acquisitionPeriod;
      AcquisitionDuration = acquisitionDuration;
      leaseCollection_ = leaseCollection;
      sessionHandle_ = sessionHandle;
    }

    /// <inheritdoc />
    public TimeSpan AcquisitionPeriod { get; }

    /// <inheritdoc />
    public TimeSpan AcquisitionDuration { get; }

    /// <inheritdoc />
    public async Task<Lease> TryAcquireLease(TaskId id, CancellationToken cancellationToken = default)
    {
      var key     = id.ToPrintableId();
      var leaseId = Guid.NewGuid().ToString();

      var updateDefinition = new UpdateDefinitionBuilder<LeaseDataModel>().SetOnInsert(ldm=> ldm.ExpiresAt, DateTime.UtcNow + AcquisitionDuration)
                                                                          .SetOnInsert(ldm => ldm.Lock, leaseId)
                                                                          .SetOnInsert(ldm => ldm.Key, key);

      var res = await leaseCollection_.FindOneAndUpdateAsync<LeaseDataModel>(sessionHandle_,
                                                                             ldm => ldm.Key == key,
                                                                             updateDefinition,
                                                                             new FindOneAndUpdateOptions<LeaseDataModel>
                                                                             {
                                                                               IsUpsert       = true,
                                                                               ReturnDocument = ReturnDocument.After,
                                                                             }, 
                                                                             cancellationToken);
      return leaseId == res.Lock ? 
               new Lease { ExpirationDate = Timestamp.FromDateTime(res.ExpiresAt), Id = id, LeaseId = leaseId } : 
               new Lease { Id             = id, LeaseId = string.Empty, ExpirationDate = new Timestamp() };
    }

    /// <inheritdoc />
    public async Task<Lease> TryRenewLease(TaskId id, string leaseId, CancellationToken cancellationToken = default)
    {
      var key = id.ToPrintableId();

      var updateDefinition = new UpdateDefinitionBuilder<LeaseDataModel>().Set(ldm => ldm.ExpiresAt,
                                                                               DateTime.UtcNow + AcquisitionDuration);

      var res = await leaseCollection_.FindOneAndUpdateAsync<LeaseDataModel>(sessionHandle_,
                                                                             ldm => ldm.Key == key &&
                                                                                    ldm.Lock == leaseId,
                                                                             updateDefinition,
                                                                             new FindOneAndUpdateOptions<LeaseDataModel>
                                                                             {
                                                                               ReturnDocument = ReturnDocument.After,
                                                                             },
                                                                             cancellationToken);

      return leaseId == res.Lock
               ? new Lease { Id = id, LeaseId = leaseId,      ExpirationDate = Timestamp.FromDateTime(res.ExpiresAt) }
               : new Lease { Id = id, LeaseId = string.Empty, ExpirationDate = new Timestamp() };
    }

    /// <inheritdoc />
    public async Task ReleaseLease(TaskId id, string leaseId, CancellationToken cancellationToken = default)
    {
      var key = id.ToPrintableId();
      
      await leaseCollection_.FindOneAndDeleteAsync(sessionHandle_,
                                                   ldm => ldm.Key == key &&
                                                          ldm.Lock == leaseId,
                                                   cancellationToken: cancellationToken);

    }
  }
}
