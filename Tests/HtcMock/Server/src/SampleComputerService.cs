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
// but WITHOUT ANY WARRANTY

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Utils;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Stream.Worker;

using Google.Protobuf;

using Grpc.Core;

using Htc.Mock.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Server;

public class SampleComputerService : WorkerStreamWrapper
{
  public SampleComputerService(ILoggerFactory      loggerFactory,
                               GrpcChannelProvider provider)
    : base(loggerFactory,
           provider)
  {
    logger_ = loggerFactory.CreateLogger<SampleComputerService>();
  }

  public override async Task<Output> Process(ITaskHandler taskHandler)
  {
    using var scopedLog = logger_.BeginNamedScope("Execute task",
                                                  ("sessionId", taskHandler.SessionId),
                                                  ("taskId", taskHandler.TaskId));
    logger_.LogTrace("DataDependencies {DataDependencies}",
                     taskHandler.DataDependencies.Keys);
    logger_.LogTrace("ExpectedResults {ExpectedResults}",
                     taskHandler.ExpectedResults);

    try
    {
      var (runConfiguration, request) = DataAdapter.ReadPayload(taskHandler.Payload);

      var inputs = request.Dependencies.ToDictionary(id => id,
                                                     id =>
                                                     {
                                                       logger_.LogInformation("Looking for result for Id {id}",
                                                                              id);
                                                       var armonikId = taskHandler.SessionId + "%" + id;
                                                       var isOkay = taskHandler.DataDependencies.TryGetValue(armonikId,
                                                                                                             out var data);
                                                       if (!isOkay)
                                                       {
                                                         throw new KeyNotFoundException(armonikId);
                                                       }

                                                       return Encoding.Default.GetString(data);
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
        await taskHandler.CreateTasksAsync(readyRequests.Select(r =>
                                                                {
                                                                  var taskId = taskHandler.SessionId + "%" + r.Id;
                                                                  logger_.LogDebug("Create task {task}",
                                                                                   taskId);
                                                                  return new TaskRequest
                                                                         {
                                                                           Id = taskId,
                                                                           Payload = ByteString.CopyFrom(DataAdapter.BuildPayload(runConfiguration,
                                                                                                                                  r)),
                                                                           DataDependencies =
                                                                           {
                                                                             r.Dependencies.Select(s => taskHandler.SessionId + "%" + s),
                                                                           },
                                                                           ExpectedOutputKeys =
                                                                           {
                                                                             taskId,
                                                                           },
                                                                         };
                                                                }))
                         .ConfigureAwait(false);
        var req = requests[false]
          .Single();
        await taskHandler.CreateTasksAsync(new[]
                                           {
                                             new TaskRequest
                                             {
                                               Id = taskHandler.SessionId + "%" + req.Id,
                                               Payload = ByteString.CopyFrom(DataAdapter.BuildPayload(runConfiguration,
                                                                                                      req)),
                                               DataDependencies =
                                               {
                                                 req.Dependencies.Select(s => taskHandler.SessionId + "%" + s),
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
