// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using System.Collections.Generic;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

public class GrpcEventsService : Events.EventsBase
{
  private readonly ILogger<GrpcEventsService> logger_;
  private readonly IResultTable               resultTable_;
  private readonly IResultWatcher             resultWatcher_;
  private readonly ITaskTable                 taskTable_;
  private readonly ITaskWatcher               taskWatcher_;

  public GrpcEventsService(ITaskTable                 taskTable,
                           ITaskWatcher               taskWatcher,
                           IResultTable               resultTable,
                           IResultWatcher             resultWatcher,
                           ILogger<GrpcEventsService> logger)
  {
    logger_        = logger;
    taskTable_     = taskTable;
    taskWatcher_   = taskWatcher;
    resultTable_   = resultTable;
    resultWatcher_ = resultWatcher;
  }

  [RequiresPermission(typeof(GrpcEventsService),
                      nameof(GetEvents))]
  public override async Task GetEvents(EventSubscriptionRequest                       request,
                                       IServerStreamWriter<EventSubscriptionResponse> responseStream,
                                       ServerCallContext                              context)
  {
    var wtg = new WatchToGrpc(taskTable_,
                              taskWatcher_,
                              resultTable_,
                              resultWatcher_,
                              logger_);

    var enumerator = wtg.GetEvents(request.SessionId,
                                   context.CancellationToken)
                        .GetAsyncEnumerator();

    while (await enumerator.MoveNextAsync(context.CancellationToken)
                           .ConfigureAwait(false))
    {
      await responseStream.WriteAsync(enumerator.Current,
                                      context.CancellationToken)
                          .ConfigureAwait(false);
    }
  }
}
