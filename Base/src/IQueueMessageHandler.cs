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
using System.Threading;

namespace ArmoniK.Core.Base;

/// <summary>
///   Interface to handle queue messages lifecycle.
/// </summary>
public interface IQueueMessageHandler : IAsyncDisposable
{
  /// <summary>
  ///   Used to signal that the message ownership has been lost
  /// </summary>
  [Obsolete("ArmoniK now manages loss of link with the queue")]
  CancellationToken CancellationToken { get; set; }

  /// <summary>
  ///   Id of the message
  /// </summary>
  string MessageId { get; }

  /// <summary>
  ///   Task Id contained in the message
  /// </summary>
  string TaskId { get; }

  /// <summary>
  ///   Status of the message. Used when the handler is disposed to notify the queue.
  /// </summary>
  QueueMessageStatus Status { get; set; }

  /// <summary>
  ///   Date of reception of the message
  /// </summary>
  DateTime ReceptionDateTime { get; init; }
}
