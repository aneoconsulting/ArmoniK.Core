// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using Amazon.SQS.Model;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Adapters.SQS.Extensions;

internal static class MessageDataExtensions
{
  public static SendMessageBatchRequestEntry ToBatchRequestEntry(this MessageData messageData,
                                                                 SQS sqsOptions)
  {
    var sendMessageBatchRequestEntry = new SendMessageBatchRequestEntry
    {
      Id          = Guid.NewGuid().ToString(),
      MessageBody = System.Text.Json.JsonSerializer.Serialize(messageData),
    };

    if (sqsOptions.UseSessionMessageGroupId)
    {
      sendMessageBatchRequestEntry.MessageGroupId = messageData.SessionId;
    }

    return sendMessageBatchRequestEntry;
  }

}
