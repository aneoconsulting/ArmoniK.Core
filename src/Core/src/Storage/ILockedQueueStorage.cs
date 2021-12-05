// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface ILockedQueueStorage : IQueueStorageBase
  {
    TimeSpan LockRefreshPeriodicity { get; }

    TimeSpan LockRefreshExtension { get; }

    bool AreMessagesUnique { get; }

    Task<bool> RenewLeaseAsync(string id, CancellationToken cancellationToken = default);
  }
}