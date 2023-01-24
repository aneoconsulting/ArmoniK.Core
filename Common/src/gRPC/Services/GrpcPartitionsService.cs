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

using System.Linq;
using System.Threading.Tasks;

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcPartitionsService : Partitions.PartitionsBase
{
  private readonly ILogger<GrpcPartitionsService> logger_;
  private readonly IPartitionTable                partitionTable_;

  public GrpcPartitionsService(IPartitionTable                partitionTable,
                               ILogger<GrpcPartitionsService> logger)
  {
    partitionTable_ = partitionTable;
    logger_         = logger;
  }

  [RequiresPermission(typeof(GrpcPartitionsService),
                      nameof(GetPartition))]
  public override async Task<GetPartitionResponse> GetPartition(GetPartitionRequest request,
                                                                ServerCallContext   context)
  {
    var partition = await partitionTable_.ReadPartitionAsync(request.Id,
                                                             context.CancellationToken)
                                         .ConfigureAwait(false);
    return new GetPartitionResponse
           {
             Partition = new PartitionRaw
                         {
                           Id       = partition.PartitionId,
                           Priority = partition.Priority,
                           PodConfiguration =
                           {
                             partition.PodConfiguration?.Configuration,
                           },
                           PodMax               = partition.PodMax,
                           PodReserved          = partition.PodReserved,
                           PreemptionPercentage = partition.PreemptionPercentage,
                           ParentPartitionIds =
                           {
                             partition.ParentPartitionIds,
                           },
                         },
           };
  }


  [RequiresPermission(typeof(GrpcPartitionsService),
                      nameof(ListPartitions))]
  public override async Task<ListPartitionsResponse> ListPartitions(ListPartitionsRequest request,
                                                                    ServerCallContext     context)
  {
    var partitions = await partitionTable_.ListPartitionsAsync(request.Filter.ToPartitionFilter(),
                                                               request.Sort.ToPartitionField(),
                                                               request.Sort.Direction == ListPartitionsRequest.Types.OrderDirection.Asc,
                                                               request.Page,
                                                               request.PageSize,
                                                               context.CancellationToken)
                                          .ConfigureAwait(false);
    return new ListPartitionsResponse
           {
             Partitions =
             {
               partitions.partitions.Select(p => new PartitionRaw
                                                 {
                                                   Id       = p.PartitionId,
                                                   Priority = p.Priority,
                                                   PodConfiguration =
                                                   {
                                                     p.PodConfiguration?.Configuration,
                                                   },
                                                   PodMax      = p.PodMax,
                                                   PodReserved = p.PodReserved,
                                                   ParentPartitionIds =
                                                   {
                                                     p.ParentPartitionIds,
                                                   },
                                                   PreemptionPercentage = p.PreemptionPercentage,
                                                 }),
             },
             Page     = request.Page,
             PageSize = request.PageSize,
             Total    = partitions.totalCount,
           };
  }
}
