// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Versions;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcVersionsService : Versions.VersionsBase
{
  public static readonly string CoreVersion = typeof(GrpcVersionsService).Assembly.GetName()
                                                                         .Version!.ToString();

  public static readonly string ApiVersion = typeof(Versions.VersionsBase).Assembly.GetName()
                                                                          .Version!.ToString();

  [RequiresPermission(typeof(GrpcVersionsService),
                      nameof(ListVersions))]
  public override Task<ListVersionsResponse> ListVersions(ListVersionsRequest request,
                                                          ServerCallContext   context)
    => Task.FromResult(new ListVersionsResponse
                       {
                         Api  = ApiVersion,
                         Core = CoreVersion,
                       });
}
