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

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Interface to manage the life cycle of a session
/// </summary>
public interface ISessionTable : IInitializable
{
  /// <summary>
  ///   Logger for class ISessionTable
  /// </summary>
  ILogger Logger { get; }

  /// <summary>
  ///   Set metadata for a new session
  /// </summary>
  /// <param name="partitionIds">List of partitions allowed to be used during the session</param>
  /// <param name="defaultOptions">Default metadata for the tasks to be created in this session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Id of the created session
  /// </returns>
  Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                   TaskOptions         defaultOptions,
                                   CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Get SessionData from sessionId
  /// </summary>
  /// <param name="sessionId">Id of the session to get</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Data of the session
  /// </returns>
  Task<SessionData> GetSessionAsync(string            sessionId,
                                    CancellationToken cancellationToken = default);

  /// <summary>
  ///   Query a session status to check if it is canceled
  /// </summary>
  /// <param name="sessionId">Id of the session to check</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Boolean representing the cancelation status of the session
  /// </returns>
  Task<bool> IsSessionCancelledAsync(string            sessionId,
                                     CancellationToken cancellationToken = default);

  /// <summary>
  ///   Get default task metadata for a session given its id
  /// </summary>
  /// <param name="sessionId">Id of the target session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Default task metadata of this session
  /// </returns>
  Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                              CancellationToken cancellationToken = default);

  /// <summary>
  ///   Cancel a session
  /// </summary>
  /// <param name="sessionId">Id of the session to cancel</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the cancelled session
  /// </returns>
  Task<SessionData> CancelSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default);

  /// <summary>
  ///   Delete a session
  /// </summary>
  /// <param name="sessionId">Id of the session to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteSessionAsync(string            sessionId,
                          CancellationToken cancellationToken = default);

  /// <summary>
  ///   List all sessions matching a given filter
  /// </summary>
  /// <param name="sessionFilter">Session filter describing the sessions to be listed </param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of sessions that matched the filter
  /// </returns>
  IAsyncEnumerable<string> ListSessionsAsync(SessionFilter     sessionFilter,
                                             CancellationToken cancellationToken = default);

  /// <summary>
  ///   List all sessions matching the given request
  /// </summary>
  /// <param name="filter">Filter to select sessions</param>
  /// <param name="orderField">Select the field that will be used to order the sessions</param>
  /// <param name="ascOrder">Is the order ascending</param>
  /// <param name="page">The page of results to retrieve</param>
  /// <param name="pageSize">The number of results pages</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of sessions metadata that matched the filter and total number of results without paging
  /// </returns>
  Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                               Expression<Func<SessionData, object?>> orderField,
                                                                               bool                                   ascOrder,
                                                                               int                                    page,
                                                                               int                                    pageSize,
                                                                               CancellationToken                      cancellationToken = default);
}
