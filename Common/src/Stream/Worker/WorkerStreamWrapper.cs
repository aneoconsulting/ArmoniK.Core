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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Stream.Worker;

[PublicAPI]
public class WorkerStreamWrapper : Api.gRPC.V1.Worker.Worker.WorkerBase
{
  private readonly ILoggerFactory               loggerFactory_;
  public           ILogger<WorkerStreamWrapper> logger_;

  public WorkerStreamWrapper(ILoggerFactory loggerFactory)
  {
    logger_        = loggerFactory.CreateLogger<WorkerStreamWrapper>();
    loggerFactory_ = loggerFactory;
  }

  public sealed override async Task<ProcessReply> Process(IAsyncStreamReader<ProcessRequest> requestStream,
                                                          ServerCallContext                  context)
  {
    Output output;
    {
      var taskHandler = await TaskHandler.Create(requestStream,
                                                 new Configuration
                                                 {
                                                   DataChunkMaxSize = 50 * 1024,
                                                 },
                                                 loggerFactory_,
                                                 context.CancellationToken)
                                         .ConfigureAwait(false);

      using var _ = logger_.BeginNamedScope("Execute task",
                                            ("taskId", taskHandler.TaskId),
                                            ("sessionId", taskHandler.SessionId));
      logger_.LogDebug("Execute Process");
      output = await Process(taskHandler)
                 .ConfigureAwait(false);
    }
    return new ProcessReply
           {
             Output = output,
           };
  }

  public virtual Task<Output> Process(ITaskHandler taskHandler)
    => throw new RpcException(new Status(StatusCode.Unimplemented,
                                         ""));

  public override Task<HealthCheckReply> HealthCheck(Empty             request,
                                                     ServerCallContext context)
    => Task.FromResult(new HealthCheckReply
                       {
                         Status = HealthCheckReply.Types.ServingStatus.Serving,
                       });
}
