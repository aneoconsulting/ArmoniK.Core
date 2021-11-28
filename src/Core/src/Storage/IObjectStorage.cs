// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Storage
{
  public interface IObjectStorage
  {
    Task AddOrUpdateAsync(string key, byte[] value, CancellationToken cancellationToken = default);

    Task<byte[]> TryGetValuesAsync(string key, CancellationToken cancellationToken = default);

    Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default);
  }
}
