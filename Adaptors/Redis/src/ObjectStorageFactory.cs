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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorageFactory : IObjectStorageFactory
{
  private readonly ILoggerFactory         loggerFactory_;
  private readonly IConnectionMultiplexer redis_;
  private          bool                   isInitialized_;

  public ObjectStorageFactory(IConnectionMultiplexer redis,
                              ILoggerFactory         loggerFactory)
  {
    redis_         = redis;
    loggerFactory_ = loggerFactory;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await redis_.GetDatabase()
                  .PingAsync()
                  .ConfigureAwait(false);

      foreach (var endPoint in redis_.GetEndPoints())
      {
        await redis_.GetServer(endPoint)
                    .PingAsync()
                    .ConfigureAwait(false);
      }
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  public IObjectStorage CreateObjectStorage(string objectStorageName)
  {
    if (!isInitialized_)
    {
      using var cts = new CancellationTokenSource(20000);
      Init(cts.Token)
        .Wait(cts.Token);
    }

    return new ObjectStorage(redis_.GetDatabase(),
                             objectStorageName,
                             loggerFactory_.CreateLogger<ObjectStorage>());
  }
}
