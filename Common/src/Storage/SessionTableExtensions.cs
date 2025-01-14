// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Diagnostics;
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
  /// <exception cref="SessionNotFoundException">if session was not found</exception>
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
  /// <exception cref="SessionNotFoundException">if session was not found</exception>
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
  /// <exception cref="SessionNotFoundException">if session was not found</exception>
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
  /// <exception cref="SessionNotFoundException">if session was not found or deleted</exception>
  /// <exception cref="InvalidSessionTransitionException">if session is in a status that cannot be cancelled</exception>
  public static async Task<SessionData> CancelSessionAsync(this ISessionTable sessionTable,
                                                           string             sessionId,
                                                           CancellationToken  cancellationToken = default)
  {
    var session = await sessionTable.UpdateOneSessionAsync(sessionId,
                                                           data => data.Status != SessionStatus.Cancelled && data.Status != SessionStatus.Purged &&
                                                                   data.Status != SessionStatus.Deleted,
                                                           new UpdateDefinition<SessionData>().Set(model => model.WorkerSubmission,
                                                                                                   false)
                                                                                              .Set(model => model.ClientSubmission,
                                                                                                   false)
                                                                                              .Set(model => model.Status,
                                                                                                   SessionStatus.Cancelled)
                                                                                              .Set(model => model.CancellationDate,
                                                                                                   DateTime.UtcNow),
                                                           false,
                                                           cancellationToken)
                                    .ConfigureAwait(false);
    if (session is not null)
    {
      return session;
    }

    session = await sessionTable.GetSessionAsync(sessionId,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    switch (session.Status)
    {
      case SessionStatus.Running:
      case SessionStatus.Paused:
        throw new UnreachableException($"Session status should be {SessionStatus.Cancelled} but is {session.Status}");
      case SessionStatus.Purged:
      case SessionStatus.Cancelled:
        throw new InvalidSessionTransitionException($"Cannot cancel a session with status {session.Status}");
      case SessionStatus.Deleted:
        throw new SessionNotFoundException($"Session {sessionId} was found but is deleted.",
                                           true);
      case SessionStatus.Unspecified:
      default:
        throw new InvalidOperationException($"Unknown session status {session.Status}");
    }
  }

  /// <summary>
  ///   Pause a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to pause</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the paused session
  /// </returns>
  /// <exception cref="SessionNotFoundException">if session was not found or deleted</exception>
  /// <exception cref="InvalidSessionTransitionException">if session is in a status that cannot be cancelled</exception>
  public static async Task<SessionData> PauseSessionAsync(this ISessionTable sessionTable,
                                                          string             sessionId,
                                                          CancellationToken  cancellationToken = default)
  {
    var session = await sessionTable.UpdateOneSessionAsync(sessionId,
                                                           data => data.Status == SessionStatus.Running,
                                                           new UpdateDefinition<SessionData>().Set(model => model.Status,
                                                                                                   SessionStatus.Paused),
                                                           false,
                                                           cancellationToken)
                                    .ConfigureAwait(false);

    if (session is not null)
    {
      return session;
    }

    session = await sessionTable.GetSessionAsync(sessionId,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    switch (session.Status)
    {
      case SessionStatus.Running:
        throw new UnreachableException($"Session status should be {SessionStatus.Paused} but is {session.Status}");
      case SessionStatus.Paused:
      case SessionStatus.Purged:
      case SessionStatus.Cancelled:
        throw new InvalidSessionTransitionException($"Cannot pause a session with status {session.Status}");
      case SessionStatus.Deleted:
        throw new SessionNotFoundException($"Session {sessionId} was found but is deleted.",
                                           true);
      case SessionStatus.Unspecified:
      default:
        throw new InvalidOperationException($"Unknown session status {session.Status}");
    }
  }

  /// <summary>
  ///   Resume a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to resume</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the resumed session
  /// </returns>
  /// <exception cref="SessionNotFoundException">if session was not found or deleted</exception>
  /// <exception cref="InvalidSessionTransitionException">if session is in a status that cannot be cancelled</exception>
  public static async Task<SessionData> ResumeSessionAsync(this ISessionTable sessionTable,
                                                           string             sessionId,
                                                           CancellationToken  cancellationToken = default)
  {
    var session = await sessionTable.UpdateOneSessionAsync(sessionId,
                                                           data => data.Status == SessionStatus.Paused || data.Status == SessionStatus.Running,
                                                           new UpdateDefinition<SessionData>().Set(model => model.Status,
                                                                                                   SessionStatus.Running),
                                                           false,
                                                           cancellationToken)
                                    .ConfigureAwait(false);

    if (session is not null)
    {
      return session;
    }

    session = await sessionTable.GetSessionAsync(sessionId,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    switch (session.Status)
    {
      case SessionStatus.Paused:
      case SessionStatus.Running:
        throw new UnreachableException($"Session status should be {SessionStatus.Running} but is {session.Status}");
      case SessionStatus.Purged:
      case SessionStatus.Cancelled:
        throw new InvalidSessionTransitionException($"Cannot resume a session with status {session.Status}");
      case SessionStatus.Deleted:
        throw new SessionNotFoundException($"Session {sessionId} was found but is deleted.",
                                           true);
      case SessionStatus.Unspecified:
      default:
        throw new InvalidOperationException($"Unknown session status {session.Status}");
    }
  }

  /// <summary>
  ///   Close a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to purge</param>
  /// <param name="creationDateTime">Start of the session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the closed session
  /// </returns>
  /// <exception cref="SessionNotFoundException">if session was not found or deleted</exception>
  /// <exception cref="InvalidSessionTransitionException">if session is in a status that cannot be cancelled</exception>
  public static async Task<SessionData> CloseSessionAsync(this ISessionTable sessionTable,
                                                          string             sessionId,
                                                          DateTime?          creationDateTime,
                                                          CancellationToken  cancellationToken = default)
  {
    var now = DateTime.UtcNow;
    var ud = new UpdateDefinition<SessionData>().Set(model => model.Status,
                                                     SessionStatus.Closed)
                                                .Set(model => model.ClosureDate,
                                                     now)
                                                .Set(model => model.WorkerSubmission,
                                                     false)
                                                .Set(model => model.ClientSubmission,
                                                     false);

    if (creationDateTime is not null)
    {
      ud = ud.Set(data => data.Duration,
                  now - creationDateTime);
    }

    var session = await sessionTable.UpdateOneSessionAsync(sessionId,
                                                           data => data.Status == SessionStatus.Running || data.Status == SessionStatus.Paused,
                                                           ud,
                                                           false,
                                                           cancellationToken)
                                    .ConfigureAwait(false);

    if (session is not null)
    {
      return session;
    }

    session = await sessionTable.GetSessionAsync(sessionId,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    switch (session.Status)
    {
      case SessionStatus.Running:
      case SessionStatus.Paused:
      case SessionStatus.Purged:
      case SessionStatus.Cancelled:
        throw new UnreachableException($"Session status should be {SessionStatus.Closed} but is {session.Status}");
      case SessionStatus.Closed:
        throw new InvalidSessionTransitionException($"Cannot close a session with status {session.Status}");
      case SessionStatus.Deleted:
        throw new SessionNotFoundException($"Session {sessionId} was found but is deleted.",
                                           true);
      case SessionStatus.Unspecified:
      default:
        throw new InvalidOperationException($"Unknown session status {session.Status}");
    }
  }

  /// <summary>
  ///   Purge a session
  /// </summary>
  /// <param name="sessionTable">Interface to manage sessions lifecycle</param>
  /// <param name="sessionId">Id of the session to purge</param>
  /// <param name="creationDateTime">Start of the session</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The metadata of the purged session
  /// </returns>
  /// <exception cref="SessionNotFoundException">if session was not found or deleted</exception>
  /// <exception cref="InvalidSessionTransitionException">if session is in a status that cannot be cancelled</exception>
  public static async Task<SessionData> PurgeSessionAsync(this ISessionTable sessionTable,
                                                          string             sessionId,
                                                          DateTime?          creationDateTime,
                                                          CancellationToken  cancellationToken = default)
  {
    var now = DateTime.UtcNow;
    var ud = new UpdateDefinition<SessionData>().Set(model => model.Status,
                                                     SessionStatus.Purged)
                                                .Set(model => model.PurgeDate,
                                                     now)
                                                .Set(model => model.WorkerSubmission,
                                                     false)
                                                .Set(model => model.ClientSubmission,
                                                     false);

    var session = await sessionTable.UpdateOneSessionAsync(sessionId,
                                                           data => data.Status == SessionStatus.Closed || data.Status == SessionStatus.Cancelled,
                                                           ud,
                                                           false,
                                                           cancellationToken)
                                    .ConfigureAwait(false);

    if (session is not null)
    {
      return session;
    }

    session = await sessionTable.GetSessionAsync(sessionId,
                                                 cancellationToken)
                                .ConfigureAwait(false);
    switch (session.Status)
    {
      case SessionStatus.Running:
      case SessionStatus.Closed:
      case SessionStatus.Paused:
      case SessionStatus.Cancelled:
        throw new UnreachableException($"Session status should be {SessionStatus.Purged} but is {session.Status}");
      case SessionStatus.Purged:
        throw new InvalidSessionTransitionException($"Cannot purge a session with status {session.Status}");
      case SessionStatus.Deleted:
        throw new SessionNotFoundException($"Session {sessionId} was found but is deleted.",
                                           true);
      case SessionStatus.Unspecified:
      default:
        throw new InvalidOperationException($"Unknown session status {session.Status}");
    }
  }

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
    var updates = new UpdateDefinition<SessionData>();

    if (client)
    {
      updates.Set(data => data.ClientSubmission,
                  false);
    }

    if (worker)
    {
      updates.Set(data => data.WorkerSubmission,
                  false);
    }

    if (updates.Setters.Count > 0)
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
