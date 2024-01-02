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

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

public static class SessionTableExtensions
{
  /// <summary>
  ///   Get SessionData from sessionId
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to get</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Data of the session
  /// </returns>
  public static async Task<SessionData> GetSessionAsync(this ISessionTable sessionTable,
                                                        string             sessionId,
                                                        CancellationToken  cancellationToken = default)
  {
    try
    {
      return await sessionTable.FindSessionsAsync(data => data.SessionId == sessionId,
                                                  data => data,
                                                  cancellationToken)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new SessionNotFoundException($"Session {sessionId} not found.",
                                         e);
    }
  }

  /// <summary>
  ///   Query a session status to check if it is canceled
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to check</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Boolean representing the cancellation status of the session
  /// </returns>
  public static async Task<bool> IsSessionCancelledAsync(this ISessionTable sessionTable,
                                                         string             sessionId,
                                                         CancellationToken  cancellationToken = default)
  {
    try
    {
      return await sessionTable.FindSessionsAsync(data => data.SessionId == sessionId,
                                                  data => data.Status    == SessionStatus.Cancelled,
                                                  cancellationToken)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new SessionNotFoundException($"Session {sessionId} not found.",
                                         e);
    }
  }

  /// <summary>
  ///   Get default task metadata for a session given its id
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the target session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Default task metadata of this session
  /// </returns>
  public static async Task<TaskOptions> GetDefaultTaskOptionAsync(this ISessionTable sessionTable,
                                                                  string             sessionId,
                                                                  CancellationToken  cancellationToken = default)
  {
    try
    {
      return await sessionTable.FindSessionsAsync(data => data.SessionId == sessionId,
                                                  data => data.Options,
                                                  cancellationToken)
                               .SingleAsync(cancellationToken)
                               .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new SessionNotFoundException($"Session {sessionId} not found.",
                                         e);
    }
  }
}
