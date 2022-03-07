// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Storage;

public static class SessionTableExtensions
{
  public static async Task CreateSessionAsync(this ISessionTable sessionTable, string id, Api.gRPC.V1.TaskOptions defaultOptions, CancellationToken cancellationToken = default)
  {
    using var _ = sessionTable.Logger.LogFunction(id);

    await sessionTable.CreateSessionDataAsync(id,
                                              id,
                                              id,
                                              defaultOptions,
                                              cancellationToken);
  }


  public static async Task CreateDispatchedSessionAsync(this ISessionTable sessionTable, string rootSessionId, string parentTaskId, string dispatchId, CancellationToken cancellationToken = default)
  {
    using var _ = sessionTable.Logger.LogFunction(dispatchId);

    var taskOptions = await sessionTable.GetDefaultTaskOptionAsync(rootSessionId,
                                                                   cancellationToken);

    await sessionTable.CreateSessionDataAsync(rootSessionId,
                                              parentTaskId,
                                              dispatchId,
                                              taskOptions,
                                              cancellationToken);
  }
}
