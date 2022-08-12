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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Interface to manage partitions and their life cycle
///   in the data base
/// </summary>
public interface IPartitionTable : IInitializable
{
  /// <summary>
  ///   Inserts a collection of partitions in the data base
  /// </summary>
  /// <param name="partitions">Collection of partitions to be inserted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                             CancellationToken          cancellationToken = default);

  /// <summary>
  ///   Retrieves a partition from the data base
  /// </summary>
  /// <param name="partitionId">Id of the partition to read</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task metadata of the retrieved partition
  /// </returns>
  /// <exception cref="PartitionNotFoundException">required partition is not found</exception>
  Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Gets partitions that are pod allocated to it (PodMax > 0)
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of partitions respecting the request
  /// </returns>
  IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   Remove a partition from the data base given its id
  /// </summary>
  /// <param name="partitionId">Id of the partition to be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="PartitionNotFoundException">required partition is not found</exception>
  Task DeletePartitionAsync(string            partitionId,
                            CancellationToken cancellationToken = default);

  /// <summary>
  ///   Check the availability of the given partitions in the data base
  /// </summary>
  /// <param name="partitionIds">Collection of partition ids</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A bool representing whether all the partitions are available
  /// </returns>
  Task<bool> ArePartitionExistingAsync(IEnumerable<string> partitionIds,
                                       CancellationToken   cancellationToken = default);
}
