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

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task status.
/// </summary>
public enum TaskStatus
{
  /// <summary>Task is in an unknown state.</summary>
  Unspecified,

  /// <summary>Task is being created in database.</summary>
  Creating,

  /// <summary>Task is submitted to the queue.</summary>
  Submitted,

  /// <summary>Task is dispatched to a worker.</summary>
  Dispatched,

  /// <summary>Task is completed.</summary>
  Completed,

  /// <summary>Task is an error state.</summary>
  Error,

  /// <summary>Task is in timeout state.</summary>
  Timeout,

  /// <summary>Task is being cancelled.</summary>
  Cancelling,

  /// <summary>Task is cancelled.</summary>
  Cancelled,

  /// <summary>Task is being processed.</summary>
  Processing,

  /// <summary>Task is processed.</summary>
  Processed,

  /// <summary>Task is retried.</summary>
  Retried,

  /// <summary>Task is waiting for its dependencies before becoming executable.</summary>
  Pending,

  /// <summary>Task is paused and will not be executed until session is resumed.</summary>
  Paused,
}
