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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Stream.Worker;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Common
{
  public class WorkerService : WorkerStreamWrapper
  {
    public WorkerService(ILoggerFactory loggerFactory) : base(loggerFactory)
    {
    }

    public override async Task<Output> Process(ITaskHandler taskHandler)
    {
      var output = new Output();
      logger_.LogInformation("Execute task {sessionId} {taskId}", taskHandler.SessionId, taskHandler.TaskId);
      logger_.LogDebug("ExpectedResults {expectedResults}",
                       taskHandler.ExpectedResults);
      logger_.LogDebug("Execute Task {task}", taskHandler.TaskId);
      try
      {
        var payload = TestPayload.Deserialize(taskHandler.Payload);
        if (payload != null)
          switch (payload.Type)
          {
            case TestPayload.TaskType.Compute:
            {
              var input = BitConverter.ToInt32(payload.DataBytes);
              var result = new TestPayload
              {
                Type      = TestPayload.TaskType.Result,
                DataBytes = BitConverter.GetBytes(input * input),
              };
              await taskHandler.SendResult(payload.ResultKey,
                                           result.Serialize());
              output = new Output
              {
                Ok     = new Empty(),
                Status = TaskStatus.Completed,
              };
              break;
            }
            case TestPayload.TaskType.Result:
              break;
            case TestPayload.TaskType.Undefined:
              break;
            case TestPayload.TaskType.None:
              break;
            case TestPayload.TaskType.Error:
              throw new Exception("Expected exception in Task");
            case TestPayload.TaskType.Transfer:
            {
              var taskId = "transfer" + Guid.NewGuid();

              payload.Type = TestPayload.TaskType.Compute;
              var req = new TaskRequest
              {
                Id      = taskId,
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  payload.ResultKey,
                },
              };
              await taskHandler.CreateTasksAsync(new[] { req });
              logger_.LogDebug("Sub Task created : {subtaskid}",
                               taskId);
              output = new Output
              {
                Ok     = new Empty(),
                Status = TaskStatus.Completed,
              };
            }
              break;
            case TestPayload.TaskType.DatadepTransfer:
            {
              var         taskId = "DatadepTransfer-" + Guid.NewGuid();
              TaskRequest req;
              if (taskHandler.ExpectedResults.Count != 2)
                throw new ArgumentOutOfRangeException();

              var resId = taskHandler.ExpectedResults.First();
              var depId = taskHandler.ExpectedResults.Last();
              var input = BitConverter.ToInt32(payload.DataBytes);

              payload.Type = TestPayload.TaskType.DatadepCompute;

              req = new TaskRequest
              {
                Id      = taskId,
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  resId,
                },
                DataDependencies =
                {
                  depId,
                },
              };

              logger_.LogDebug("DataDepTransfer Input {input}", input);
              var result = new TestPayload
              {
                Type      = TestPayload.TaskType.Result,
                DataBytes = BitConverter.GetBytes(input * input),
              };
              await taskHandler.SendResult(depId,
                                           result.Serialize());

              await taskHandler.CreateTasksAsync(new[] { req });
              logger_.LogDebug("Sub Task created : {subtaskid}",
                               taskId);

              output = new Output
              {
                Ok     = new Empty(),
                Status = TaskStatus.Completed,
              };
            }
              break;
            case TestPayload.TaskType.DatadepCompute:
            {
              if (taskHandler.ExpectedResults.Count != 1)
                throw new ArgumentOutOfRangeException();
              if (taskHandler.DataDependencies.Count != 1)
                throw new ArgumentOutOfRangeException();

              var resId    = taskHandler.ExpectedResults.First();
              var input    = BitConverter.ToInt32(payload.DataBytes);
              var payload2 = TestPayload.Deserialize(taskHandler.DataDependencies.Values.First());

              if (payload2.Type != TestPayload.TaskType.Result)
                throw new Exception();

              var input2 = BitConverter.ToInt32(payload2.DataBytes);
              
              logger_.LogDebug("DataDepCompute Input1 {input}",
                               input);
              logger_.LogDebug("DataDepCompute Input2 {input}",
                               input2);

              var result = new TestPayload
              {
                Type      = TestPayload.TaskType.Result,
                DataBytes = BitConverter.GetBytes(input * input + input2),
              };
              await taskHandler.SendResult(resId,
                                           result.Serialize());

              output = new Output
              {
                Ok     = new Empty(),
                Status = TaskStatus.Completed,
              };
            }
              break;
            case TestPayload.TaskType.ReturnFailed:
              output = new Output
              {
                Error = new Output.Types.Error
                {
                  Details = "Failed task",
                  KillSubTasks = true,
                },
                Status = TaskStatus.Failed,
              };
              break;
            default:
              throw new ArgumentOutOfRangeException();
          }
      }
      catch (Exception ex)
      {
        logger_.LogError(ex,
                         "Error while computing task");

        output = new Output
        {
          Error = new Output.Types.Error
          {
            Details = ex.Message + ex.StackTrace,
            KillSubTasks = true,
          },
          Status = TaskStatus.Error,
        };
      }

      return output;
    }
  }
}