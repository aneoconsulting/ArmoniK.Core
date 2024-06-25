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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;

using Google.Protobuf;

using Grpc.Core;

using Htc.Mock.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Server;

public class SampleComputerService : WorkerStreamWrapper
{
  public SampleComputerService(ILoggerFactory      loggerFactory,
                               ComputePlane        options,
                               GrpcChannelProvider provider)
    : base(loggerFactory,
           options,
           provider)
    => logger_ = loggerFactory.CreateLogger<SampleComputerService>();

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
      var (runConfiguration, request) = DataAdapter.ReadPayload(taskHandler.Payload);

      var inputs = request.Dependencies.ToDictionary(id => id,
                                                     id =>
                                                     {
                                                       logger_.LogInformation("Looking for result for Id {id}",
                                                                              id);
                                                       var isOkay = taskHandler.DataDependencies.TryGetValue(id,
                                                                                                             out var data);
                                                       if (!isOkay)
                                                       {
                                                         throw new KeyNotFoundException(id);
                                                       }

                                                       return Encoding.Default.GetString(data!);
                                                     });
      logger_.LogDebug("Inputs {input}",
                       inputs);

      var fastCompute = bool.Parse(taskHandler.TaskOptions.Options.GetValueOrDefault("FastCompute",
                                                                                     "true"));
      var useLowMem = bool.Parse(taskHandler.TaskOptions.Options.GetValueOrDefault("UseLowMem",
                                                                                   "true"));
      var smallOutput = bool.Parse(taskHandler.TaskOptions.Options.GetValueOrDefault("SmallOutput",
                                                                                     "true"));

      logger_.LogDebug("Execute HtcMock request with FastCompute {FastCompute}, UseLowMem {UseLowMem} and SmallOutput {SmallOutput}",
                       fastCompute,
                       useLowMem,
                       smallOutput);
      var requestProcessor = new RequestProcessor(fastCompute,
                                                  useLowMem,
                                                  smallOutput,
                                                  runConfiguration,
                                                  logger_);
      var res = requestProcessor.GetResult(request,
                                           inputs);
      logger_.LogDebug("Result for processing request is HasResult={hasResult}, Value={value}",
                       res.Result.HasResult,
                       res.Result.Value);

      if (res.Result.HasResult)
      {
        await taskHandler.SendResult(taskHandler.ExpectedResults.Single(),
                                     Encoding.Default.GetBytes(res.Result.Value))
                         .ConfigureAwait(false);
      }
      else
      {
        var requests = res.SubRequests.GroupBy(r => r.Dependencies.Count == 0)
                          .ToDictionary(g => g.Key,
                                        g => g);
        logger_.LogDebug("Will submit {count} new tasks",
                         requests[true]
                           .Count());
        var readyRequests = requests[true];

        var createResultsResponse = await taskHandler.CreateResultsMetaDataAsync(readyRequests.Select(r => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                                           {
                                                                                                             Name = r.Id,
                                                                                                           }))
                                                     .ConfigureAwait(false);

        // todo : can be batched in order to improve memory usage
        var taskRequests = readyRequests.Select(r => new TaskRequest
                                                     {
                                                       Payload = ByteString.CopyFrom(DataAdapter.BuildPayload(runConfiguration,
                                                                                                              r)),
                                                       DataDependencies =
                                                       {
                                                         r.Dependencies,
                                                       },
                                                       ExpectedOutputKeys =
                                                       {
                                                         createResultsResponse.Results.Single(resultMetaData => resultMetaData.Name == r.Id)
                                                                              .ResultId,
                                                       },
                                                     })
                                        .ToList();

        var createTaskReply = await taskHandler.CreateTasksAsync(taskRequests)
                                               .ConfigureAwait(false);
        if (createTaskReply.ResponseCase != CreateTaskReply.ResponseOneofCase.CreationStatusList)
        {
          throw new Exception("Error while creating tasks");
        }

        var req = requests[false]
          .Single();

        req.Dependencies.Clear();
        foreach (var dependencyId in taskRequests.Select(taskRequest => taskRequest.ExpectedOutputKeys.Single()))
        {
          req.Dependencies.Add(dependencyId);
        }

        await taskHandler.CreateTasksAsync(new[]
                                           {
                                             new TaskRequest
                                             {
                                               Payload = ByteString.CopyFrom(DataAdapter.BuildPayload(runConfiguration,
                                                                                                      req)),
                                               DataDependencies =
                                               {
                                                 req.Dependencies,
                                               },
                                               ExpectedOutputKeys =
                                               {
                                                 taskHandler.ExpectedResults,
                                               },
                                             },
                                           })
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
