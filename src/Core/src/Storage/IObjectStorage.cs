// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Storage
{
  public interface IObjectStorage
  {
    Task<IAsyncEnumerable<(string, byte[])>> GetOrAddAsync(IEnumerable<(string, byte[])> values,
                                                           CancellationToken             cancellationToken = default);

    Task AddOrUpdateAsync(IEnumerable<(string, byte[])> values, CancellationToken cancellationToken = default);

    Task<IAsyncEnumerable<(string, byte[])>>
      TryGetValuesAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default);

    Task TryDeleteAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default);
  }
}
