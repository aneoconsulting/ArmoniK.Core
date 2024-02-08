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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Object Storage interface
///   It is used to store data in ArmoniK
/// </summary>
public interface IObjectStorage : IInitializable
{
  /// <summary>
  ///   Add the given data in the storage at the given key
  ///   Update data if it already exists
  /// </summary>
  /// <param name="key">Key representing the object</param>
  /// <param name="valueChunks">Chunks of data that will be stored in the Object Storage</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The size of the object that has been uploaded.
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">the key is not found</exception>
  Task<long> AddOrUpdateAsync(string                                 key,
                              IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                              CancellationToken                      cancellationToken = default);

  /// <summary>
  ///   Get object in the Object Storage
  /// </summary>
  /// <param name="key">Key representing the object to be retrieved</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Byte arrays representing the object chunked
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">the key is not found</exception>
  IAsyncEnumerable<byte[]> GetValuesAsync(string            key,
                                          CancellationToken cancellationToken = default);

  /// <summary>
  ///   Delete data in the object storage
  /// </summary>
  /// <param name="keys">Keys representing the objects to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">the key is not found</exception>
  Task TryDeleteAsync(IEnumerable<string> keys,
                      CancellationToken   cancellationToken = default);

  /// <summary>
  ///   List data in the Object Storage
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The keys representing data found in the Object Storage
  /// </returns>
  IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default);
}
