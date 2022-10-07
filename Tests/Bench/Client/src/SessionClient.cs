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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.Bench.Client;

public class SessionClient
{
  private readonly Submitter.SubmitterClient client_;
  private readonly ILogger<SessionClient>    logger_;
  private readonly string                    sessionId_;

  public SessionClient(Submitter.SubmitterClient client,
                       string                    sessionId,
                       ILogger<SessionClient>    logger)
  {
    client_    = client;
    logger_    = logger;
    sessionId_ = sessionId;
  }

  public byte[] GetResult(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

    var availabilityReply = client_.WaitForAvailability(resultRequest);

    switch (availabilityReply.TypeCase)
    {
      case AvailabilityReply.TypeOneofCase.None:
        throw new Exception("Issue with Server !");
      case AvailabilityReply.TypeOneofCase.Ok:
        break;
      case AvailabilityReply.TypeOneofCase.Error:
        throw new Exception($"Task in Error - {availabilityReply.Error.TaskId} : {availabilityReply.Error.Errors}");
      case AvailabilityReply.TypeOneofCase.NotCompletedTask:
        throw new Exception($"Task not completed - {id}");
      default:
        throw new ArgumentOutOfRangeException(nameof(availabilityReply.TypeCase));
    }

    var response = client_.GetResultAsync(resultRequest);
    return response.Result;
  }

  public Task WaitSubtasksCompletion(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

    var availabilityReply = client_.WaitForAvailability(resultRequest);

    switch (availabilityReply.TypeCase)
    {
      case AvailabilityReply.TypeOneofCase.None:
        throw new Exception("Issue with Server !");
      case AvailabilityReply.TypeOneofCase.Ok:
        break;
      case AvailabilityReply.TypeOneofCase.Error:
        throw new Exception($"Task in Error - {availabilityReply.Error.TaskId} : {availabilityReply.Error.Errors}");
      case AvailabilityReply.TypeOneofCase.NotCompletedTask:
        throw new Exception($"Task not completed - {id}");
      default:
        throw new ArgumentOutOfRangeException(nameof(availabilityReply.TypeCase));
    }

    return Task.CompletedTask;
  }

  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies)
  {
    var taskRequests = new List<TaskRequest>();

    foreach (var (payload, dependencies) in payloadsWithDependencies)
    {
      var taskRequest = new TaskRequest
                        {
                          Payload = ByteString.CopyFrom(payload),
                          DataDependencies =
                          {
                            dependencies,
                          },
                          ExpectedOutputKeys =
                          {
                            Guid.NewGuid() + "%root",
                          },
                        };
      ;
      logger_.LogDebug("Dependencies : {dep}",
                       string.Join(", ",
                                   dependencies.Select(item => item.ToString())));
      taskRequests.Add(taskRequest);
    }

    var createTaskReply = client_.CreateTasksAsync(sessionId_,
                                                   null,
                                                   taskRequests)
                                 .Result;
    switch (createTaskReply.ResponseCase)
    {
      case CreateTaskReply.ResponseOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.ResponseOneofCase.CreationStatusList:
        return taskRequests.Select(request => request.ExpectedOutputKeys.Single());
      case CreateTaskReply.ResponseOneofCase.Error:
        throw new Exception("Error : " + createTaskReply.Error);
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
