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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

  /// <summary>
  ///   Cancel a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to cancel</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the cancelled session
  /// </returns>
  public static async Task<SessionData> CancelSessionAsync(this ISessionTable sessionTable,
                                                           string             sessionId,
                                                           CancellationToken  cancellationToken = default)
    => await sessionTable.UpdateOneSessionAsync(sessionId,
                                                data => data.Status == SessionStatus.Running || data.Status == SessionStatus.Paused ||
                                                        data.Status != SessionStatus.Cancelled,
                                                new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>
                                                {
                                                  (model => model.WorkerSubmission, false),
                                                  (model => model.ClientSubmission, false),
                                                  (model => model.Status, SessionStatus.Cancelled),
                                                  (model => model.CancellationDate, DateTime.UtcNow),
                                                },
                                                false,
                                                cancellationToken)
                         .ConfigureAwait(false) ?? throw new SessionNotFoundException($"No open session with {sessionId} found.");

  /// <summary>
  ///   Pause a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to pause</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the paused session
  /// </returns>
  public static async Task<SessionData> PauseSessionAsync(this ISessionTable sessionTable,
                                                          string             sessionId,
                                                          CancellationToken  cancellationToken = default)
    => await sessionTable.UpdateOneSessionAsync(sessionId,
                                                data => data.Status == SessionStatus.Running,
                                                new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>
                                                {
                                                  (model => model.Status, SessionStatus.Paused),
                                                },
                                                false,
                                                cancellationToken)
                         .ConfigureAwait(false) ?? throw new SessionNotFoundException($"No open session with {sessionId} found.");

  /// <summary>
  ///   Resume a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to resume</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the resumed session
  /// </returns>
  public static async Task<SessionData> ResumeSessionAsync(this ISessionTable sessionTable,
                                                           string             sessionId,
                                                           CancellationToken  cancellationToken = default)
    => await sessionTable.UpdateOneSessionAsync(sessionId,
                                                data => data.Status == SessionStatus.Paused,
                                                new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>
                                                {
                                                  (model => model.Status, SessionStatus.Running),
                                                },
                                                false,
                                                cancellationToken)
                         .ConfigureAwait(false) ?? throw new SessionNotFoundException($"No paused session with {sessionId} found.");

  /// <summary>
  ///   Purge a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to purge</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the purged session
  /// </returns>
  public static async Task<SessionData> PurgeSessionAsync(this ISessionTable sessionTable,
                                                          string             sessionId,
                                                          CancellationToken  cancellationToken = default)
    => await sessionTable.UpdateOneSessionAsync(sessionId,
                                                data => data.Status == SessionStatus.Running || data.Status == SessionStatus.Paused,
                                                new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>
                                                {
                                                  (model => model.Status, SessionStatus.Purged),
                                                  (model => model.PurgeDate, DateTime.UtcNow),
                                                },
                                                false,
                                                cancellationToken)
                         .ConfigureAwait(false) ?? throw new SessionNotFoundException($"No open session with {sessionId} found.");

  /// <summary>
  ///   Delete a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to delete</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the deleted session
  /// </returns>
  /// <remarks>
  ///   A session cannot be deleted twice
  /// </remarks>
  public static async Task<SessionData> DeleteSessionAsync(this ISessionTable sessionTable,
                                                           string             sessionId,
                                                           CancellationToken  cancellationToken = default)
    => await sessionTable.UpdateOneSessionAsync(sessionId,
                                                data => data.Status != SessionStatus.Deleted,
                                                new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>
                                                {
                                                  (model => model.Status, SessionStatus.Deleted),
                                                  (model => model.DeletionDate, DateTime.UtcNow),
                                                  (model => model.DeletionTtl, DateTime.UtcNow),
                                                },
                                                false,
                                                cancellationToken)
                         .ConfigureAwait(false) ?? throw new SessionNotFoundException($"No open session with {sessionId} found.");

  /// <summary>
  ///   Stops submission for client and/or worker
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="client">Whether to stop submission for clients</param>
  /// <param name="worker">Whether to stop submission for worker</param>
  /// <param name="sessionId">Id of the session to cancel</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the session
  /// </returns>
  /// <exception cref="SessionNotFoundException">if session is not found</exception>
  public static async Task<SessionData> StopSubmissionAsync(this ISessionTable sessionTable,
                                                            string             sessionId,
                                                            bool               client,
                                                            bool               worker,
                                                            CancellationToken  cancellationToken = default)
  {
    var updates = new List<(Expression<Func<SessionData, object?>> selector, object? newValue)>();

    if (client)
    {
      updates.Add((data => data.ClientSubmission, false));
    }

    if (worker)
    {
      updates.Add((data => data.WorkerSubmission, false));
    }

    if (updates.Count > 0)
    {
      return await sessionTable.UpdateOneSessionAsync(sessionId,
                                                      null,
                                                      updates,
                                                      false,
                                                      cancellationToken)
                               .ConfigureAwait(false) ?? throw new SessionNotFoundException($"Session {sessionId} not found");
    }

    return await sessionTable.GetSessionAsync(sessionId,
                                              cancellationToken)
                             .ConfigureAwait(false);
  }
}
