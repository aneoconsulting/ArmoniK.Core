// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
      return distributedCache_.SetAsync(key,
                                        value,
                                        cancellationToken);
    }

    /// <inheritdoc />
    public Task<byte[]> TryGetValuesAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      return distributedCache_.GetAsync(key,
                                        cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      try
      {
        await distributedCache_.RemoveAsync(key,
                                            cancellationToken);
        return true;
      }
      catch
      {
        return false;
      }
    }
  }
}
