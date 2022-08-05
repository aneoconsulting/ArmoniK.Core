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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Factory to create Object Storage
/// </summary>
public interface IObjectStorageFactory : IInitializable
{
  /// <summary>
  ///   Create an Object Storage with the given name
  /// </summary>
  /// <param name="objectStorageName">Name of the object storage to create</param>
  /// <returns>
  ///   An object implementing the <see cref="IObjectStorage" /> interface
  /// </returns>
  IObjectStorage CreateObjectStorage(string objectStorageName);
}

/// <summary>
///   Factory extension to create specific Object Storage
/// </summary>
public static class ObjectStorageFactoryExt
{
  /// <summary>
  ///   Creation of Object Storage to store payloads
  /// </summary>
  /// <param name="factory">Factory for creating Object Storage</param>
  /// <param name="session">Session Id of the tasks that will use this Object Storage</param>
  /// <returns>
  ///   The created Object Storage
  /// </returns>
  public static IObjectStorage CreatePayloadStorage(this IObjectStorageFactory factory,
                                                    string                     session)
    => factory.CreateObjectStorage($"payloads/{session}");

  /// <summary>
  ///   Creation of Object Storage to store results
  /// </summary>
  /// <param name="factory">Factory for creating Object Storage</param>
  /// <param name="session">Session Id of the tasks that will use this Object Storage</param>
  /// <returns>
  ///   The created Object Storage
  /// </returns>
  public static IObjectStorage CreateResultStorage(this IObjectStorageFactory factory,
                                                   string                     session)
    => factory.CreateObjectStorage($"results/{session}");

  /// <summary>
  ///   Creation of Object Storage to store resources
  /// </summary>
  /// <param name="factory">Factory for creating Object Storage</param>
  /// <returns>
  ///   The created Object Storage
  /// </returns>
  public static IObjectStorage CreateResourcesStorage(this IObjectStorageFactory factory)
    => factory.CreateObjectStorage("resources/");
}

/// <summary>
///   Object Storage interface
///   It is used to store data in ArmoniK
/// </summary>
public interface IObjectStorage
{
  /// <summary>
  ///   Add the given data in the storage at the given key
  ///   Update data if it already exists
  /// </summary>
  /// <param name="key">Key representing the object</param>
  /// <param name="valueChunks">Chunks of data that will be stored in the Object Storage</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="ArmoniKException">there is 0 chunk</exception>
  Task AddOrUpdateAsync(string                   key,
                        IAsyncEnumerable<byte[]> valueChunks,
                        CancellationToken        cancellationToken = default);

  /// <summary>
  ///   Add the given data in the storage at the given key
  ///   Update data if it already exists
  /// </summary>
  /// <param name="key">Key representing the object</param>
  /// <param name="valueChunks">Chunks of data that will be stored in the Object Storage</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">the key is not found</exception>
  Task AddOrUpdateAsync(string                                 key,
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
  /// <param name="key">Key representing the object to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A bool representing the success of the deletion
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">the key is not found</exception>
  Task<bool> TryDeleteAsync(string            key,
                            CancellationToken cancellationToken = default);

  /// <summary>
  ///   List data in the Object Storage
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The keys representing data found in the Object Storage
  /// </returns>
  IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default);
}
