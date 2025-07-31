// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System.Threading.Tasks;

using ArmoniK.Core.Common.Injection.Options.Database;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

/// <summary>
///   Defines the contract for mapping a data model to a MongoDB collection.
/// </summary>
/// <typeparam name="T">The type of the data model.</typeparam>
public interface IMongoDataModelMapping<T>
{
  /// <summary>
  ///   Gets the name of the MongoDB collection associated with the data model.
  /// </summary>
  string CollectionName { get; }

  /// <summary>
  ///   Setup indexes for the collection
  ///   Can be called multiple times
  /// </summary>
  /// <param name="sessionHandle">MongoDB Client session</param>
  /// <param name="collection">MongoDDB Collection in which to insert data</param>
  /// <param name="options">Options for MongoDB</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task InitializeIndexesAsync(IClientSessionHandle sessionHandle,
                              IMongoCollection<T>  collection,
                              Options.MongoDB      options);

  /// <summary>
  ///   Setup sharding for the collection
  ///   Can be called multiple times
  /// </summary>
  /// <param name="sessionHandle">MongoDB Client session</param>
  /// <param name="options">Options for MongoDB</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                            Options.MongoDB      options);

  /// <summary>
  ///   Insert data into the collection after its creation.
  ///   Can be called multiple times
  /// </summary>
  /// <param name="sessionHandle">MongoDB Client session</param>
  /// <param name="collection">MongoDDB Collection in which to insert data</param>
  /// <param name="initDatabase">Data to insert</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task InitializeCollectionAsync(IClientSessionHandle sessionHandle,
                                 IMongoCollection<T>  collection,
                                 InitDatabase         initDatabase)
    => Task.CompletedTask;
}
