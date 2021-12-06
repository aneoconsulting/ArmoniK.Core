// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public class DistributedCacheObjectStorage : IObjectStorage
  {
    private readonly IDistributedCache                      distributedCache_;
    private readonly ILogger<DistributedCacheObjectStorage> logger_;

    public DistributedCacheObjectStorage(IDistributedCache distributedCache, ILogger<DistributedCacheObjectStorage> logger)
    {
      logger_           = logger;
      distributedCache_ = distributedCache;
    }

    /// <inheritdoc />
    public Task AddOrUpdateAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      return distributedCache_.SetAsync(key, value, cancellationToken);
    }

    /// <inheritdoc />
    public Task<byte[]> TryGetValuesAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      return distributedCache_.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      try
      {
        await distributedCache_.RemoveAsync(key, cancellationToken);
        return true;
      }
      catch
      {
        return false;
      }
    }
  }
}
