// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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

using ArmoniK.Core.Common.Exceptions;

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Helper to manage sessions
/// </summary>
public static class SessionLifeCycleHelper
{
  /// <summary>
  ///   Create a session with default options for the tasks
  /// </summary>
  /// <param name="sessionTable">Interface to manage session states</param>
  /// <param name="partitionTable">Interface to manage partition states</param>
  /// <param name="partitionIds">Partitions the tasks from the created session are allowed to access</param>
  /// <param name="defaultTaskOptions">Default options for the tasks in the session</param>
  /// <param name="defaultPartition">Default partition for the session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The ID of the created session
  /// </returns>
  /// <exception cref="PartitionNotFoundException">when the partition is not found</exception>
  public static async Task<string> CreateSession(ISessionTable     sessionTable,
                                                 IPartitionTable   partitionTable,
                                                 IList<string>     partitionIds,
                                                 TaskOptions       defaultTaskOptions,
                                                 string            defaultPartition,
                                                 CancellationToken cancellationToken)
  {
    if (!partitionIds.Any())
    {
      partitionIds.Add(defaultPartition);
    }

    if (partitionIds.Count == 1 && string.IsNullOrEmpty(partitionIds.Single()))
    {
      partitionIds.Clear();
      partitionIds.Add(defaultPartition);
    }

    if (!await partitionTable.ArePartitionsExistingAsync(partitionIds,
                                                         cancellationToken)
                             .ConfigureAwait(false))
    {
      throw new PartitionNotFoundException("One of the partitions does not exist");
    }

    if (string.IsNullOrEmpty(defaultTaskOptions.PartitionId))
    {
      defaultTaskOptions = defaultTaskOptions with
                           {
                             PartitionId = defaultPartition,
                           };
    }

    if (!await partitionTable.ArePartitionsExistingAsync(new[]
                                                         {
                                                           defaultTaskOptions.PartitionId,
                                                         },
                                                         cancellationToken)
                             .ConfigureAwait(false))
    {
      throw new PartitionNotFoundException("The partition in the task options does not exist");
    }

    var sessionId = await sessionTable.SetSessionDataAsync(partitionIds,
                                                           defaultTaskOptions,
                                                           cancellationToken)
                                      .ConfigureAwait(false);
    return sessionId;
  }
}
