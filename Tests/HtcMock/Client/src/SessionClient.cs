// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Client;

using Google.Protobuf;

using Grpc.Core;

using Htc.Mock;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client;

public sealed class SessionClient : ISessionClient
{
  private readonly CancellationToken       cancellationToken_;
  private readonly ChannelBase             channel_;
  private readonly ILogger<GridClient>     logger_;
  private readonly Options.HtcMock         optionsHtcMock_;
  private readonly Results.ResultsClient   resultsClient_;
  private readonly string                  sessionId_;
  private readonly Sessions.SessionsClient sessionsClient_;
  private readonly Tasks.TasksClient       tasksClient_;

  public SessionClient(ChannelBase         channel,
                       string              sessionId,
                       Options.HtcMock     optionsHtcMock,
                       ILogger<GridClient> logger,
                       CancellationToken   cancellationToken)
  {
    cancellationToken_ = cancellationToken;
    resultsClient_     = new Results.ResultsClient(channel);
    sessionsClient_    = new Sessions.SessionsClient(channel);
    tasksClient_       = new Tasks.TasksClient(channel);
    channel_           = channel;
    logger_            = logger;
    sessionId_         = sessionId;
    optionsHtcMock_    = optionsHtcMock;
  }


  public void Dispose()
  {
    if (cancellationToken_.IsCancellationRequested)
    {
      sessionsClient_.CancelSession(new CancelSessionRequest
                                    {
                                      SessionId = sessionId_,
                                    },
                                    cancellationToken: CancellationToken.None);
    }
    else
    {
      sessionsClient_.CloseSession(new CloseSessionRequest
                                   {
                                     SessionId = sessionId_,
                                   },
                                   cancellationToken: CancellationToken.None);

      // Only meaningful for a completed run; it also fails when no task has finished yet
      var stats = channel_.ComputeThroughput(sessionId_,
                                             CancellationToken.None)
                          .Result;

      logger_.LogInformation("Throughput for session {session} : {sessionThroughput} task/s ({nTasks} tasks in {timespan})",
                             sessionId_,
                             stats.TasksCount / stats.Duration.TotalMilliseconds * 1000,
                             stats.TasksCount,
                             stats.Duration);
    }

    if (optionsHtcMock_.PurgeData)
    {
      sessionsClient_.PurgeSession(new PurgeSessionRequest
                                   {
                                     SessionId = sessionId_,
                                   },
                                   cancellationToken: CancellationToken.None);
    }
  }

  public byte[] GetResult(string id)
    => resultsClient_.DownloadResultData(sessionId_,
                                         id,
                                         CancellationToken.None)
                     .Result;

  public async Task WaitSubtasksCompletion(string id)
    => await new Events.EventsClient(channel_).WaitForResultsAsync(sessionId_,
                                                                   new[]
                                                                   {
                                                                     id,
                                                                   },
                                                                   parallelism: 1,
                                                                   cancellationToken: cancellationToken_)
                                              .ConfigureAwait(false);

  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies)
  {
    var resultsCreated = new List<string>();
    var i              = 0;

    foreach (var (payload, dependencies) in payloadsWithDependencies)
    {
      var payloads = resultsClient_.CreateResults(new CreateResultsRequest
                                                  {
                                                    SessionId = sessionId_,
                                                    Results =
                                                    {
                                                      new CreateResultsRequest.Types.ResultCreate
                                                      {
                                                        Data = UnsafeByteOperations.UnsafeWrap(payload),
                                                        Name = $"payload {i}",
                                                      },
                                                    },
                                                  },
                                                  cancellationToken: cancellationToken_);

      var result = resultsClient_.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                        {
                                                          SessionId = sessionId_,
                                                          Results =
                                                          {
                                                            new CreateResultsMetaDataRequest.Types.ResultCreate
                                                            {
                                                              Name = $"root {i}",
                                                            },
                                                          },
                                                        },
                                                        cancellationToken: cancellationToken_)
                                 .Results.Select(raw => raw.ResultId)
                                 .Single();

      tasksClient_.SubmitTasks(new SubmitTasksRequest
                               {
                                 SessionId = sessionId_,
                                 TaskCreations =
                                 {
                                   new SubmitTasksRequest.Types.TaskCreation
                                   {
                                     PayloadId = payloads.Results.Select(raw => raw.ResultId)
                                                         .Single(),
                                     DataDependencies =
                                     {
                                       dependencies,
                                     },
                                     ExpectedOutputKeys =
                                     {
                                       result,
                                     },
                                   },
                                 },
                               },
                               cancellationToken: cancellationToken_);

      logger_.LogDebug("Dependencies : {dep}",
                       string.Join(", ",
                                   dependencies.Select(item => item.ToString())));
      i++;
      resultsCreated.Add(result);
    }

    return resultsCreated;
  }
}
