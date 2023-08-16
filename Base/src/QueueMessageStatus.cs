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

namespace ArmoniK.Core.Base;

/// <summary>
///   Represents the status of a queue message
/// </summary>
public enum QueueMessageStatus
{
  /// <summary>
  ///   Message is waiting for being processed.
  /// </summary>
  Waiting,

  /// <summary>
  ///   Message processing has failed. The message should be put back at the begin of the queue.
  /// </summary>
  Failed,

  /// <summary>
  ///   The message is being processed.
  /// </summary>
  Running,

  /// <summary>
  ///   Task is not ready to be processed. The message should be put at the end of the queue.
  /// </summary>
  Postponed,

  /// <summary>
  ///   The message has been processed. It can safely be removed from the queue.
  /// </summary>
  Processed,

  /// <summary>
  ///   The message processing has been cancelled. the message can safely be removed from the queue.
  /// </summary>
  Cancelled,

  /// <summary>
  ///   Message has been retried too many times and is considered as poisonous for the queue
  /// </summary>
  Poisonous,
}
