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

using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcResultsService : Results.ResultsBase
{
  private readonly ILogger<GrpcResultsService> logger_;
  private readonly IResultTable                resultTable_;

  public GrpcResultsService(IResultTable                resultTable,
                            ILogger<GrpcResultsService> logger)
  {
    logger_      = logger;
    resultTable_ = resultTable;
  }

  public override async Task<GetOwnerTaskIdResponse> GetOwnerTaskId(GetOwnerTaskIdRequest request,
                                                                    ServerCallContext     context)
  {
    using var _ = logger_.LogFunction();

    return new GetOwnerTaskIdResponse
           {
             SessionId = request.SessionId,
             ResultTask =
             {
               await resultTable_.GetResults(request.SessionId,
                                             request.ResultId,
                                             context.CancellationToken)
                                 .Select(result => new GetOwnerTaskIdResponse.Types.MapResultTask
                                                   {
                                                     TaskId   = result.OwnerTaskId,
                                                     ResultId = result.Name,
                                                   })
                                 .ToListAsync(context.CancellationToken)
                                 .ConfigureAwait(false),
             },
           };
  }

  public override async Task<ListResultsResponse> ListResults(ListResultsRequest request,
                                                              ServerCallContext  context)
    => new()
       {
         PageSize = request.PageSize,
         Page     = request.Page,
         Results =
         {
           (await resultTable_.ListResultsAsync(request,
                                                context.CancellationToken)
                              .ConfigureAwait(false)).Select(result => new ResultRaw(result)),
         },
       };
}
