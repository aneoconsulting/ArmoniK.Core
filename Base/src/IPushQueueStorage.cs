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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Base;

/// <summary>
///   Interface to insert messages into the queue
/// </summary>
public interface IPushQueueStorage : IQueueStorage
{
  /// <summary>
  ///   Puts messages into the queue, handles priorities of messages
  /// </summary>
  /// <param name="messages">Collection of messages</param>
  /// <param name="partitionId">Id of the partition</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                string                   partitionId,
                                CancellationToken        cancellationToken = default);
}
