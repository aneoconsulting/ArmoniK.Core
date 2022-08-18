// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Threading.Tasks;

using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

public static class SubmitterExt
{
  public static void CreateSessionAndCheckReply(this Submitter.SubmitterClient client,
                                                string                         sessionId,
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
                                         Id                = sessionId,
                                         PartitionIds =
                                         {
                                           partitionId,
                                         },
                                       });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");
  }

  public static async Task CreateTasksAndCheckReplyAsync(this Submitter.SubmitterClient client,
                                                         string                         sessionId,
                                                         TaskOptions                    taskOptions,
                                                         IEnumerable<TaskRequest>       taskRequestList)
  {
    var createTaskReply = await client.CreateTasksAsync(sessionId,
                                                        taskOptions,
                                                        taskRequestList)
                                      .ConfigureAwait(false);
    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
