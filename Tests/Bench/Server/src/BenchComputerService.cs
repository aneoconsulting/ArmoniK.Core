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

using ArmoniK.Api.Common.Channel.Utils;
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
                              GrpcChannelProvider provider)
    : base(loggerFactory,
           provider)
    => logger_ = loggerFactory.CreateLogger<BenchComputerService>();

  public override async Task<Output> Process(ITaskHandler taskHandler)
  {
    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId));
    logger_.LogDebug("DataDependencies {DataDependencies}",
                     taskHandler.DataDependencies.Keys);
    logger_.LogDebug("ExpectedResults {ExpectedResults}",
                     taskHandler.ExpectedResults);

    try
    {
      var sleep = int.Parse(taskHandler.TaskOptions.Options.GetValueOrDefault("TaskDurationMs",
                                                                              "100"));

      await Task.Delay(sleep)
                .ConfigureAwait(false);

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
