// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System.Collections.Generic;
using System.Linq;

using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Convert <see cref="InitServices" /> into objects we can insert into the database
///   Also used to init database collections
/// </summary>
public class InitDatabase
{
  /// <summary>
  ///   Instantiate <see cref="InitDatabase" /> from the configurations received from the Dependency Injection
  /// </summary>
  /// <param name="initServices">Data structure containing the raw data</param>
  public InitDatabase(InitServices initServices)
  {
    Init = initServices.InitDatabase;

    Partitions = initServices.Partitioning.Partitions.Select(Partition.FromJson)
                             .Select(partition => new PartitionData(partition.PartitionId,
                                                                    partition.ParentPartitionIds,
                                                                    partition.PodReserved,
                                                                    partition.PodMax,
                                                                    partition.PreemptionPercentage,
                                                                    partition.Priority,
                                                                    new PodConfiguration(partition.PodConfiguration)))
                             .ToList();
  }

  /// <summary>
  ///   Whether to init the database
  /// </summary>
  public bool Init { get; init; }

  /// <summary>
  ///   Collection of data to insert in the database for Partitions during ArmoniK initialization
  /// </summary>
  public IReadOnlyCollection<PartitionData> Partitions { get; init; }
}
