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
using System.Collections.Generic;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Represents the data associated with a session in the ArmoniK system.
/// </summary>
/// <param name="SessionId">The unique identifier for the session.</param>
/// <param name="Status">The current status of the session.</param>
/// <param name="ClientSubmission">Indicates if clients can still submit tasks in the session.</param>
/// <param name="WorkerSubmission">Indicates if workers can still submit tasks in the session.</param>
/// <param name="CreationDate">The date and time when the session was created.</param>
/// <param name="CancellationDate">The date and time when the session was canceled, if applicable.</param>
/// <param name="ClosureDate">The date and time when the session was closed, if applicable.</param>
/// <param name="PurgeDate">The date and time when the session was purged, if applicable.</param>
/// <param name="DeletionDate">The date and time when the session was deleted, if applicable.</param>
/// <param name="DeletionTtl">The time-to-live for the session deletion, if applicable.</param>
/// <param name="Duration">The duration of the session, if applicable.</param>
/// <param name="PartitionIds">The list of partition IDs associated with the session.</param>
/// <param name="Options">The task options associated with the session.</param>
public record SessionData(string        SessionId,
                          SessionStatus Status,
                          bool          ClientSubmission,
                          bool          WorkerSubmission,
                          DateTime      CreationDate,
                          DateTime?     CancellationDate,
                          DateTime?     ClosureDate,
                          DateTime?     PurgeDate,
                          DateTime?     DeletionDate,
                          DateTime?     DeletionTtl,
                          TimeSpan?     Duration,
                          IList<string> PartitionIds,
                          TaskOptions   Options)
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionData" /> class with the specified session ID, status, partition
  ///   IDs, and task options.
  /// </summary>
  /// <param name="sessionId">The unique identifier for the session.</param>
  /// <param name="status">The current status of the session.</param>
  /// <param name="partitionIds">The list of partition IDs associated with the session.</param>
  /// <param name="options">The task options associated with the session.</param>
  public SessionData(string        sessionId,
                     SessionStatus status,
                     IList<string> partitionIds,
                     TaskOptions   options)
    : this(sessionId,
           status,
           true,
           true,
           DateTime.UtcNow,
           null,
           null,
           null,
           null,
           null,
           null,
           partitionIds,
           options)
  {
  }


  /// <summary>
  ///   Creates a copy of a <see cref="SessionData" /> and modify it according to given updates
  /// </summary>
  /// <param name="original">The object that will be copied</param>
  /// <param name="updates">A collection of field selector and their new values</param>
  public SessionData(SessionData                   original,
                     UpdateDefinition<SessionData> updates)
    : this(original)
    => updates.ApplyTo(this);
}
