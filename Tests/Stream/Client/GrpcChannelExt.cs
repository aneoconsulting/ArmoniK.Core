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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Grpc.Core;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

public static class GrpcChannelExt
{
  public static async Task<TestPayload> GetPayloadAsyncAndCheck(this ChannelBase channel,
                                                                string           sessionId,
                                                                string           resultId)
  {
    var client = new Submitter.SubmitterClient(channel);

    var request = new ResultRequest
                  {
                    ResultId = resultId,
                    Session  = sessionId,
                  };
    try
    {
      var bytes = await client.GetResultAsync(request)
                              .ConfigureAwait(false);
      if (bytes.Length == 0)
      {
        throw new Exception("Output data is empty");
      }

      return TestPayload.Deserialize(bytes) ?? throw new InvalidOperationException("Payload cannot be deserialized");
    }
    catch (Exception e)
    {
      var resultClient = new Results.ResultsClient(channel);
      var res = await resultClient.GetOwnerTaskIdAsync(new GetOwnerTaskIdRequest
                                                       {
                                                         ResultId =
                                                         {
                                                           resultId,
                                                         },
                                                         SessionId = sessionId,
                                                       })
                                  .ConfigureAwait(false);

      var taskClient = new Tasks.TasksClient(channel);
      var taskData = await taskClient.GetTaskAsync(new GetTaskRequest
                                                   {
                                                     TaskId = res.ResultTask.Single()
                                                                 .TaskId,
                                                   })
                                     .ConfigureAwait(false);

      if (taskData.Task.Output.Success)
      {
        throw;
      }

      throw new Exception($"Error in task : {taskData.Task.Output.Error}",
                          e);
    }
  }
}
