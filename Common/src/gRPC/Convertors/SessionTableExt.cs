// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Threading;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class SessionTableExt
{
  /// <summary>
  ///   List all sessions matching a given filter
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionFilter">Session filter describing the sessions to be listed </param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of sessions that matched the filter
  /// </returns>
  public static IAsyncEnumerable<string> ListSessionsAsync(this ISessionTable sessionTable,
                                                           SessionFilter      sessionFilter,
                                                           CancellationToken  cancellationToken = default)
    => sessionTable.FindSessionsAsync(sessionFilter.ToFilterExpression(),
                                      data => data.SessionId,
                                      cancellationToken);
}
