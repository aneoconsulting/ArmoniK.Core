// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

// In samples, Random can be used

#pragma warning disable SEC0115

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.Bench.Server;

[UsedImplicitly]
public class BenchComputerService : WorkerStreamWrapper
{
  public BenchComputerService(ILoggerFactory      loggerFactory,
                              ComputePlane        options,
                              GrpcChannelProvider provider)
    : base(loggerFactory,
           options,
           provider)
    => logger_ = loggerFactory.CreateLogger<BenchComputerService>();

  public override async Task<Output> Process(ITaskHandler taskHandler)
  {
    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId));
    using var logFunction = logger_.LogFunction("ProcessBench",
                                                LogLevel.Information);
    logger_.LogDebug("DataDependencies {DataDependencies}",
                     taskHandler.DataDependencies.Keys);
    logger_.LogDebug("ExpectedResults {ExpectedResults}",
                     taskHandler.ExpectedResults);

    try
    {
      if (!taskHandler.TaskOptions.Options.TryGetValue("PayloadSize",
                                                       out var payloadSizeStr))
      {
        throw new InvalidOperationException("PayloadSize should be defined in task options");
      }

      var payloadSize = int.Parse(payloadSizeStr);

      if (taskHandler.Payload.Length != payloadSize * 1024)
      {
        throw new InvalidOperationException("Payload should have the same size as the one specified");
      }

      var sleep = int.Parse(taskHandler.TaskOptions.Options.GetValueOrDefault("TaskDurationMs",
                                                                              "100"));

      logger_.LogInformation("Sleep for {sleepTimeMs}",
                             sleep);

      await Task.Delay(sleep)
                .ConfigureAwait(false);

      if (!taskHandler.TaskOptions.Options.TryGetValue("ResultSize",
                                                       out var resultSizeStr))
      {
        throw new InvalidOperationException("ResultSize should be defined in task options");
      }

      var resultSize = int.Parse(resultSizeStr);
      var rnd        = new Random();

      foreach (var resultId in taskHandler.ExpectedResults)
      {
        var dataBytes = new byte[resultSize * 1024];
        rnd.NextBytes(dataBytes);
        await taskHandler.SendResult(resultId,
                                     dataBytes)
                         .ConfigureAwait(false);
      }

      var taskError = taskHandler.TaskOptions.Options.GetValueOrDefault("TaskError",
                                                                        string.Empty);

      if (taskError != string.Empty && taskHandler.TaskId.EndsWith(taskError))
      {
        logger_.LogInformation("Return Deterministic Error Output");
        return new Output
               {
                 Error = new Output.Types.Error
                         {
                           Details = "Deterministic Error",
                         },
               };
      }

      var taskRpcException = taskHandler.TaskOptions.Options.GetValueOrDefault("TaskRpcException",
                                                                               string.Empty);

      if (taskRpcException != string.Empty && taskHandler.TaskId.EndsWith(taskRpcException))
      {
        throw new RpcException(new Status(StatusCode.Internal,
                                          "Deterministic Exception"));
      }

      return new Output
             {
               Ok = new Empty(),
             };
    }
    catch (RpcException ex)
    {
      var taskRpcException = taskHandler.TaskOptions.Options.GetValueOrDefault("TaskRpcException",
                                                                               string.Empty);
      if (taskRpcException != string.Empty && taskHandler.TaskId.EndsWith(taskRpcException))
      {
        throw;
      }

      logger_.LogError(ex,
                       "Error while computing task");

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message + ex.StackTrace,
                       },
             };
    }

    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Error while computing task");

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message + ex.StackTrace,
                       },
             };
    }
  }
}
