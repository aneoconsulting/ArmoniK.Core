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

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Session status.
/// </summary>
public enum SessionStatus
{
  /// <summary>Session is in an unknown state.</summary>
  Unspecified,

  /// <summary>Session is open and accepting tasks for execution.</summary>
  Running,

  /// <summary>Session is cancelled. No more tasks can be submitted and no more tasks will be executed.</summary>
  Cancelled,

  /// <summary>Session is paused. No more tasks will be executed.</summary>
  Paused,

  /// <summary>Session is purged. Data will be deleted.</summary>
  Purged,

  /// <summary>Session is purged. Metadata for sessions, tasks and results will be deleted.</summary>
  Deleted,
}
