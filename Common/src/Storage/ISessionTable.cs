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

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
/// Interface to manage the life cycle of a session
/// </summary>
public interface ISessionTable : IInitializable
{
  /// <summary>
  /// Logger for class ISessionTable
  /// </summary>
  ILogger Logger { get; }

  /// <summary>
  /// Create a new session
  /// </summary>
  /// <param name="rootSessionId">Id for the session to be created</param>
  /// <param name="parentTaskId"> Id of the task spawing this session </param>
  /// <param name="defaultOptions"> Default options for the tasks to be created in this session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task CreateSessionDataAsync(string            rootSessionId,
                              string            parentTaskId,
                              TaskOptions       defaultOptions,
                              CancellationToken cancellationToken = default);

  /// <summary>
  ///  Query a session status to check if it is canceled
  /// </summary>
  /// <param name="sessionId">Id of the session to check</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Boolean representing the cancelation status of the session
  /// </returns>
  Task<bool> IsSessionCancelledAsync(string            sessionId,
                                     CancellationToken cancellationToken = default);

  /// <summary>
  /// Get default task metadata for a session given its id
  /// </summary>
  /// <param name="sessionId">Id of the target session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///  Default task metadata of this session
  /// </returns>
  Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                              CancellationToken cancellationToken = default);

  /// <summary>
  ///  Cancel a session
  /// </summary>
  /// <param name="sessionId">Id of the session to cancel</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task CancelSessionAsync(string            sessionId,
                          CancellationToken cancellationToken = default);

  /// <summary>
  ///  Delete a session
  /// </summary>
  /// <param name="sessionId">Id of the session to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteSessionAsync(string            sessionId,
                          CancellationToken cancellationToken = default);

  /// <summary>
  /// List all sessions matching a given filter
  /// </summary>
  /// <param name="request">Session filter describing the sessions to be listed </param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// List of sessions that matched the filter
  /// </returns>
  IAsyncEnumerable<string> ListSessionsAsync(SessionFilter     request,
                                             CancellationToken cancellationToken = default);
}
