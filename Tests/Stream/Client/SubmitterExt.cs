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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

public static class SubmitterExt
{
  public static string CreateSessionAndCheckReply(this Submitter.SubmitterClient client,
                                                  string                         partitionId)
  {
    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                        MaxRetries  = 3,
                        Priority    = 1,
                        PartitionId = partitionId,
                      };
    Console.WriteLine("Creating Session");
    var session = client.CreateSession(new CreateSessionRequest
                                       {
                                         DefaultTaskOption = taskOptions,
                                         PartitionIds =
                                         {
                                           partitionId,
                                         },
                                       });
    return session.SessionId;
  }

  public static async Task<IEnumerable<string>> CreateTasksAndCheckReplyAsync(this Submitter.SubmitterClient client,
                                                                              string                         sessionId,
                                                                              TaskOptions?                   taskOptions,
                                                                              IEnumerable<TaskRequest>       taskRequestList)
  {
    var createTaskReply = await client.CreateTasksAsync(sessionId,
                                                        taskOptions,
                                                        taskRequestList)
                                      .ConfigureAwait(false);
    switch (createTaskReply.ResponseCase)
    {
      case CreateTaskReply.ResponseOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.ResponseOneofCase.CreationStatusList:
        return createTaskReply.CreationStatusList.CreationStatuses.Select(status => status.StatusCase switch
                                                                                    {
                                                                                      CreateTaskReply.Types.CreationStatus.StatusOneofCase.None =>
                                                                                        throw new Exception("Issue with Server !"),
                                                                                      CreateTaskReply.Types.CreationStatus.StatusOneofCase.TaskInfo => status.TaskInfo
                                                                                                                                                             .TaskId,
                                                                                      CreateTaskReply.Types.CreationStatus.StatusOneofCase.Error => status.Error,
                                                                                      _                                                          => throw new ArgumentOutOfRangeException(),
                                                                                    });
      case CreateTaskReply.ResponseOneofCase.Error:
        throw new Exception("Error : " + createTaskReply.Error);
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
