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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcEventsService : Events.EventsBase
{
  private readonly ILogger<GrpcEventsService>                  logger_;
  private readonly FunctionExecutionMetrics<GrpcEventsService> meter_;
  private readonly IResultTable                                resultTable_;
  private readonly IResultWatcher                              resultWatcher_;
  private readonly ITaskTable                                  taskTable_;
  private readonly ITaskWatcher                                taskWatcher_;

  public GrpcEventsService(ITaskTable                                  taskTable,
                           ITaskWatcher                                taskWatcher,
                           IResultTable                                resultTable,
                           IResultWatcher                              resultWatcher,
                           FunctionExecutionMetrics<GrpcEventsService> meter,
                           ILogger<GrpcEventsService>                  logger)
  {
    logger_        = logger;
    taskTable_     = taskTable;
    taskWatcher_   = taskWatcher;
    resultTable_   = resultTable;
    resultWatcher_ = resultWatcher;
    meter_         = meter;
  }

  [RequiresPermission(typeof(GrpcEventsService),
                      nameof(GetEvents))]
  public override async Task GetEvents(EventSubscriptionRequest                       request,
                                       IServerStreamWriter<EventSubscriptionResponse> responseStream,
                                       ServerCallContext                              context)
  {
    using var measure = meter_.CountAndTime();
    var wtg = new WatchToGrpc(taskTable_,
                              taskWatcher_,
                              resultTable_,
                              resultWatcher_,
                              logger_);

    try
    {
      await foreach (var eventSubscriptionResponse in wtg.GetEvents(request.SessionId,
                                                                    request.ReturnedEvents,
                                                                    request.TasksFilters,
                                                                    request.ResultsFilters,
                                                                    context.CancellationToken)
                                                         .ConfigureAwait(false))
      {
        await responseStream.WriteAsync(eventSubscriptionResponse,
                                        context.CancellationToken)
                            .ConfigureAwait(false);
      }
    }
    catch (OperationCanceledException e)
    {
      logger_.LogWarning(e,
                         "Subscription cancelled, no more messages will be sent.");
    }
  }
}
