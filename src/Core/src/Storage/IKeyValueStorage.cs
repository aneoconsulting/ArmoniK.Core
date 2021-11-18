// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Storage
{
  public interface IKeyValueStorage<TKey, TValue>
  {
    Task<IAsyncEnumerable<(TKey, TValue)>> GetOrAddAsync(IEnumerable<(TKey, TValue)> values, CancellationToken cancellationToken = default);

    Task AddOrUpdateAsync(IEnumerable<(TKey, TValue)> values, CancellationToken cancellationToken = default);

    Task<IAsyncEnumerable<(TKey, TValue)>> TryGetValuesAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default);

    Task TryDeleteAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default);
  }
}
