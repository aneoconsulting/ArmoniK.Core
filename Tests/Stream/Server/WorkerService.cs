// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Server;

public class WorkerService : WorkerStreamWrapper
{
  public WorkerService(ILoggerFactory      loggerFactory,
                       GrpcChannelProvider provider)
    : base(loggerFactory,
           provider)
  {
  }

  [SuppressMessage("Style",
                   "CA2208: Call the ArgumentOutOfRangeException constructor that contains a message and/or paramName parameter",
                   Justification = "taskHandler.ExpectedResults.Count is not a real argument")]
  public override async Task<Output> Process(ITaskHandler taskHandler)
  {
    var output = new Output();
    logger_.LogInformation("Execute task {sessionId} {taskId}",
                           taskHandler.SessionId,
                           taskHandler.TaskId);
    logger_.LogDebug("ExpectedResults {expectedResults}",
                     taskHandler.ExpectedResults);
    logger_.LogDebug("Execute Task {taskId}",
                     taskHandler.TaskId);
    logger_.LogDebug("Payload size {payloadSize}",
                     taskHandler.Payload.Length);

    if (taskHandler.Payload.Length == 0)
    {
      output.Error = new Output.Types.Error
                     {
                       Details = "No payload",
                     };
      return output;
    }

    try
    {
      var payload = TestPayload.Deserialize(taskHandler.Payload);
      if (payload is not null)
      {
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
            await taskHandler.SendResult(payload.ResultKey ?? throw new NullReferenceException(),
                                         result.Serialize())
                             .ConfigureAwait(false);
            output = new Output
                     {
                       Ok = new Empty(),
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
                        Payload = ByteString.CopyFrom(payload.Serialize()),
                        ExpectedOutputKeys =
                        {
                          payload.ResultKey,
                        },
                      };
            await taskHandler.CreateTasksAsync(new[]
                                               {
                                                 req,
                                               })
                             .ConfigureAwait(false);
            logger_.LogDebug("Sub Task created : {subtaskId}",
                             taskId);
            output = new Output
                     {
                       Ok = new Empty(),
                     };
          }
            break;
          case TestPayload.TaskType.DatadepTransfer:
          {
            var taskId = "DataDepTransfer-" + Guid.NewGuid();
            if (taskHandler.ExpectedResults.Count != 2)
            {
              throw new ArgumentOutOfRangeException(nameof(payload.Type));
            }

            var resId = taskHandler.ExpectedResults.First();
            var depId = taskHandler.ExpectedResults.Last();
            var input = BitConverter.ToInt32(payload.DataBytes);

            payload.Type = TestPayload.TaskType.DatadepCompute;

            var req = new TaskRequest
                      {
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

            logger_.LogDebug("DataDepTransfer Input {input}",
                             input);
            var result = new TestPayload
                         {
                           Type      = TestPayload.TaskType.Result,
                           DataBytes = BitConverter.GetBytes(input * input),
                         };
            await taskHandler.SendResult(depId,
                                         result.Serialize())
                             .ConfigureAwait(false);

            await taskHandler.CreateTasksAsync(new[]
                                               {
                                                 req,
                                               })
                             .ConfigureAwait(false);
            logger_.LogDebug("Sub Task created : {subtaskId}",
                             taskId);

            output = new Output
                     {
                       Ok = new Empty(),
                     };
          }
            break;
          case TestPayload.TaskType.DatadepCompute:
          {
            if (taskHandler.ExpectedResults.Count != 1 || taskHandler.DataDependencies.Count != 1)
            {
              throw new ArgumentOutOfRangeException(nameof(payload.Type));
            }

            var resId    = taskHandler.ExpectedResults.First();
            var input    = BitConverter.ToInt32(payload.DataBytes);
            var payload2 = TestPayload.Deserialize(taskHandler.DataDependencies.Values.First());

            if (payload2 is not
                {
                  Type: TestPayload.TaskType.Result,
                })
            {
              throw new Exception();
            }

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
                                         result.Serialize())
                             .ConfigureAwait(false);

            output = new Output
                     {
                       Ok = new Empty(),
                     };
          }
            break;
          case TestPayload.TaskType.ReturnFailed:
            output = new Output
                     {
                       Error = new Output.Types.Error
                               {
                                 Details = "Failed task",
                               },
                     };
            break;
          case TestPayload.TaskType.PayloadCheckSum:
            var resultPayloadCheckSum = new TestPayload
                                        {
                                          Type      = TestPayload.TaskType.Result,
                                          DataBytes = SHA256.HashData(payload.DataBytes ?? throw new NullReferenceException()),
                                        };
            await taskHandler.SendResult(taskHandler.ExpectedResults.Single(),
                                         resultPayloadCheckSum.Serialize())
                             .ConfigureAwait(false);
            output = new Output
                     {
                       Ok = new Empty(),
                     };
            break;
          default:
            throw new ArgumentOutOfRangeException(nameof(payload.Type));
        }
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
                         },
               };
    }

    return output;
  }
}
