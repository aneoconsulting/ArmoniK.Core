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
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Tests.Client;

using Google.Protobuf;

using Grpc.Net.Client;

using Htc.Mock;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client;

public class SessionClient : ISessionClient
{
  private readonly GrpcChannel               channel_;
  private readonly ILogger<GridClient>       logger_;
  private readonly Results.ResultsClient     resultsClient_;
  private readonly string                    sessionId_;
  private readonly Submitter.SubmitterClient submitterClient_;

  public SessionClient(GrpcChannel         channel,
                       string              sessionId,
                       ILogger<GridClient> logger)
  {
    submitterClient_ = new Submitter.SubmitterClient(channel);
    resultsClient_   = new Results.ResultsClient(channel);
    channel_         = channel;
    logger_          = logger;
    sessionId_       = sessionId;
  }


  public void Dispose()
  {
    submitterClient_.CancelSession(new Session
                                   {
                                     Id = sessionId_,
                                   });
    channel_.LogStatsFromSessionAsync(sessionId_,
                                      logger_)
            .Wait();
  }

  public byte[] GetResult(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

    var availabilityReply = submitterClient_.WaitForAvailability(resultRequest);

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

    var response = submitterClient_.GetResultAsync(resultRequest);
    return response.Result;
  }

  public Task WaitSubtasksCompletion(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

    var availabilityReply = submitterClient_.WaitForAvailability(resultRequest);

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
      var reply = resultsClient_.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                       {
                                                         SessionId = sessionId_,
                                                         Results =
                                                         {
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "root",
                                                           },
                                                         },
                                                       });

      var taskRequest = new TaskRequest
                        {
                          Payload = ByteString.CopyFrom(payload),
                          DataDependencies =
                          {
                            dependencies,
                          },
                          ExpectedOutputKeys =
                          {
                            reply.Results.Single()
                                 .ResultId,
                          },
                        };
      ;
      logger_.LogDebug("Dependencies : {dep}",
                       string.Join(", ",
                                   dependencies.Select(item => item.ToString())));
      taskRequests.Add(taskRequest);
    }

    var createTaskReply = submitterClient_.CreateTasksAsync(sessionId_,
                                                            null,
                                                            taskRequests)
                                          .Result;
    switch (createTaskReply.ResponseCase)
    {
      case CreateTaskReply.ResponseOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.ResponseOneofCase.CreationStatusList:
        logger_.LogInformation("task created {taskId}",
                               createTaskReply.CreationStatusList.CreationStatuses.Select(status => status.TaskInfo.TaskId)
                                              .Single());
        return taskRequests.Select(request => request.ExpectedOutputKeys.Single());
      case CreateTaskReply.ResponseOneofCase.Error:
        throw new Exception("Error : " + createTaskReply.Error);
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
