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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Tests.Client;

using Google.Protobuf;

using Grpc.Core;

using Htc.Mock;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client;

public sealed class SessionClient : ISessionClient
{
  private readonly ChannelBase               channel_;
  private readonly ILogger<GridClient>       logger_;
  private readonly Results.ResultsClient     resultsClient_;
  private readonly string                    sessionId_;
  private readonly Submitter.SubmitterClient submitterClient_;

  public SessionClient(ChannelBase         channel,
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

  [SuppressMessage("Style",
                   "CA2208",
                   Justification = "availabilityReply.TypeCase is not a real argument")]
  public byte[] GetResult(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

#pragma warning disable CS0612 // Type or member is obsolete
    var availabilityReply = submitterClient_.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete

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

  [SuppressMessage("Style",
                   "CA2208",
                   Justification = "availabilityReply.TypeCase is not a real argument")]
  public Task WaitSubtasksCompletion(string id)
  {
    var resultRequest = new ResultRequest
                        {
                          ResultId = id,
                          Session  = sessionId_,
                        };

#pragma warning disable CS0612 // Type or member is obsolete
    var availabilityReply = submitterClient_.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete

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

  [SuppressMessage("Style",
                   "CA2208",
                   Justification = "createTaskReply.ResponseCase is not a real argument")]
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
        throw new ArgumentOutOfRangeException(nameof(createTaskReply.ResponseCase));
    }
  }
}
