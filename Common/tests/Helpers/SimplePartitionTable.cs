// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimplePartitionTable : IPartitionTable
{
  public readonly PartitionData PartitionData = new("PartitionId",
                                                    new List<string>(),
                                                    1,
                                                    2,
                                                    10,
                                                    2,
                                                    new PodConfiguration(new Dictionary<string, string>()));

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(new HealthCheckResult(HealthStatus.Healthy));

  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                    CancellationToken          cancellationToken = default)
    => Task.CompletedTask;

  public Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                CancellationToken cancellationToken = default)
    => Task.FromResult(PartitionData);

  public IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(CancellationToken cancellationToken = default)
    => new List<PartitionData>
       {
         PartitionData,
       }.ToAsyncEnumerable();

  public Task DeletePartitionAsync(string            partitionId,
                                   CancellationToken cancellationToken = default)
    => Task.CompletedTask;

  public Task<bool> ArePartitionsExistingAsync(IEnumerable<string> partitionIds,
                                               CancellationToken   cancellationToken = default)
    => Task.FromResult(true);

  public Task<(IEnumerable<PartitionData> partitions, int totalCount)> ListPartitionsAsync(Expression<Func<PartitionData, bool>>    filter,
                                                                                           Expression<Func<PartitionData, object?>> orderField,
                                                                                           bool                                     ascOrder,
                                                                                           int                                      page,
                                                                                           int                                      pageSize,
                                                                                           CancellationToken                        cancellationToken = default)
    => Task.FromResult((new List<PartitionData>
                        {
                          PartitionData,
                        }.AsEnumerable(), 1));
}
