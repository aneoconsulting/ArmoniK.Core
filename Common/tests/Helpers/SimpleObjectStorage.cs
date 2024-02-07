// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleObjectStorage : IObjectStorage
{
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<long> AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
    => Task.FromResult((long)0);

  public IAsyncEnumerable<byte[]> GetValuesAsync(string            key,
                                                 CancellationToken cancellationToken = default)
    => new List<byte[]>
       {
         Encoding.UTF8.GetBytes(key),
       }.ToAsyncEnumerable();

  public Task TryDeleteAsync(IEnumerable<string> keys,
                             CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => new List<string>
       {
         "key1",
         "key2",
       }.ToAsyncEnumerable();
}
