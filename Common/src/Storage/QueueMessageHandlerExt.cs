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

public static class QueueMessageHandlerExt
{
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
