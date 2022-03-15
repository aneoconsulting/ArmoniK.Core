// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Stream.Client;

using Google.Protobuf;

using Htc.Mock;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client
{
  public class SessionClient : ISessionClient
  {
    private readonly Submitter.SubmitterClient client_;
    private readonly ILogger<GridClient>       logger_;
    private readonly string                 sessionId_;

    public SessionClient(Submitter.SubmitterClient client, string sessionId, ILogger<GridClient> logger)
    {
      client_    = client;
      logger_    = logger;
      sessionId_ = sessionId;
    }


    public void Dispose()
    {
    }

    public byte[] GetResult(string id)
    {
      using var _      = logger_.LogFunction(id);
      var resultRequest = new ResultRequest
      {
        Key     = id,
        Session = sessionId_,
      };

      var availabilityReply = client_.WaitForAvailability(resultRequest);

      switch (availabilityReply.TypeCase)
      {
        case AvailabilityReply.TypeOneofCase.None:
          throw new Exception("Issue with Server !");
        case AvailabilityReply.TypeOneofCase.Ok:
          break;
        case AvailabilityReply.TypeOneofCase.Error:
          throw new Exception($"Task in Error - {id}");
        case AvailabilityReply.TypeOneofCase.NotCompletedTask:
          throw new Exception($"Task not completed - {id}");
        default:
          throw new ArgumentOutOfRangeException(nameof(availabilityReply.TypeCase));
      }

      var taskOutput = client_.TryGetTaskOutput(resultRequest);

      switch (taskOutput.TypeCase)
      {
        case Output.TypeOneofCase.None:
          throw new Exception("Issue with Server !");
        case Output.TypeOneofCase.Ok:
          break;
        case Output.TypeOneofCase.Error:
          throw new Exception($"Task in Error - {id}");
        default:
          throw new ArgumentOutOfRangeException(nameof(taskOutput.TypeCase));
      }

      var response = client_.GetResultAsync(resultRequest);
      return response.Result;
    }

    public Task WaitSubtasksCompletion(string id)
    {
      using var _      = logger_.LogFunction(id);
      client_.WaitForCompletion(new WaitRequest
      {
        Filter = new TaskFilter
        {
          Task = new TaskFilter.Types.IdsRequest
          {
            Ids =
            {
              sessionId_ + "%" + id,
            },
          },
        },
        StopOnFirstTaskCancellation = true,
        StopOnFirstTaskError        = true,
      });
      return Task.CompletedTask;
    }

    public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies)
    {
      using var _         = logger_.LogFunction();
      logger_.LogDebug("payload with dependencies {len}", payloadsWithDependencies.Count());
      var taskRequests = new List<TaskRequest>();

      foreach (var (payload, dependencies) in payloadsWithDependencies)
      {
        var taskId = Guid.NewGuid().ToString();
        logger_.LogDebug("Create task {task}", taskId);
        var taskRequest = new TaskRequest
        {
          Id      = sessionId_ + "%" + taskId,
          Payload =  ByteString.CopyFrom(payload),
          DataDependencies =
          {
            // p.Item2,
            dependencies.Select(i => sessionId_ + "%" + i),
          },
          ExpectedOutputKeys =
          {
            sessionId_ + "%" + taskId,
          },
        };
        logger_.LogDebug("Dependencies : {dep}",
                         string.Join(", ",
                                     dependencies.Select(item => item.ToString())));
        taskRequests.Add(taskRequest);
      }

      var createTaskReply = client_.CreateTasksAsync(sessionId_,
                                                          null,
                                                          taskRequests).Result;
      switch (createTaskReply.DataCase)
      {
        case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
          throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
        case CreateTaskReply.DataOneofCase.None:
          throw new Exception("Issue with Server !");
        case CreateTaskReply.DataOneofCase.Successfull:
          Console.WriteLine("Task Created");
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(createTaskReply.DataCase));
      }

      var taskCreated = taskRequests.Select(t => t.Id);

      logger_.LogDebug("Tasks created : {ids}",
                                   taskCreated);
      return taskCreated;
    }
  }
}