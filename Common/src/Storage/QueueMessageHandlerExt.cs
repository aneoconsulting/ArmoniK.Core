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
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Provides extension methods for <see cref="IQueueMessageHandler" /> instances.
/// </summary>
public static class QueueMessageHandlerExt
{
  /// <summary>
  ///   Disposes the message handler asynchronously, ignoring any errors that might occur during disposal.
  /// </summary>
  /// <param name="message">The message handler to dispose.</param>
  /// <param name="logger">Optional logger to record any errors that occur during disposal.</param>
  /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
  /// <remarks>
  ///   If an exception occurs during disposal, it will be logged but not propagated.
  ///   This helps prevent errors in cleanup from affecting the main execution flow.
  /// </remarks>
  public static async ValueTask DisposeIgnoreErrorAsync(this IQueueMessageHandler message,
                                                        ILogger?                  logger = null)
  {
    try
    {
      await message.DisposeAsync()
                   .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger?.LogError(e,
                       "Error while disposing message handler {MessageHandler}. It might appear duplicated",
                       message.MessageId);
    }
  }
}
