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

using ArmoniK.Api.gRPC.V1.Graphs;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Graphs;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

public class GrpcGraphsService : Graphs.GraphsBase
{
  private readonly ILogger<GrpcApplicationsService> logger_;
  private readonly ITaskTable                       taskTable_;
  private readonly ITaskWatcher                     taskWatcher_;
  private readonly IResultTable                     resultTable_;
  private readonly IResultWatcher                   resultWatcher_;

  public GrpcGraphsService(ITaskTable                       taskTable,
                           ITaskWatcher                     taskWatcher,
                           IResultTable                     resultTable,
                           IResultWatcher                   resultWatcher,
                           ILogger<GrpcApplicationsService> logger)
  {
    logger_        = logger;
    taskTable_     = taskTable;
    taskWatcher_   = taskWatcher;
    resultTable_   = resultTable;
    resultWatcher_ = resultWatcher;
  }

  public override async Task GetGraphs(GraphSubscriptionRequest                  request,
                                       IServerStreamWriter<GraphContentResponse> responseStream,
                                       ServerCallContext                         context)
  {
    var wtg = new WatchToGrpc(taskTable_,
                              taskWatcher_,
                              resultTable_,
                              resultWatcher_,
                              logger_);

    var enumerator = wtg.GetGraph(request.SessionId,
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
