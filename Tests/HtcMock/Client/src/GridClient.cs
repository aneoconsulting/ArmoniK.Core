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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Htc.Mock;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client;

public class GridClient : IGridClient
{
  private readonly ChannelBase         channel_;
  private readonly ILogger<GridClient> logger_;
  private readonly Options.HtcMock     optionsHtcMock_;

  public GridClient(ChannelBase     channel,
                    ILoggerFactory  loggerFactory,
                    Options.HtcMock optionsHtcMock)
  {
    channel_        = channel;
    optionsHtcMock_ = optionsHtcMock;
    logger_         = loggerFactory.CreateLogger<GridClient>();
  }

  public ISessionClient CreateSubSession(string taskId)
    => CreateSession();

  public ISessionClient CreateSession()
  {
    var client = new Sessions.SessionsClient(channel_);
    var createSessionReply = client.CreateSession(new CreateSessionRequest
                                                  {
                                                    DefaultTaskOption = new TaskOptions
                                                                        {
                                                                          MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                                                          MaxRetries  = 2,
                                                                          Priority    = 1,
                                                                          PartitionId = optionsHtcMock_.Partition,
                                                                          Options =
                                                                          {
                                                                            {
                                                                              "FastCompute", optionsHtcMock_.EnableFastCompute.ToString()
                                                                            },
                                                                            {
                                                                              "UseLowMem", optionsHtcMock_.EnableUseLowMem.ToString()
                                                                            },
                                                                            {
                                                                              "SmallOutput", optionsHtcMock_.EnableSmallOutput.ToString()
                                                                            },
                                                                            {
                                                                              "TaskError", optionsHtcMock_.TaskError
                                                                            },
                                                                            {
                                                                              "TaskRpcException", optionsHtcMock_.TaskRpcException
                                                                            },
                                                                          },
                                                                        },
                                                    PartitionIds =
                                                    {
                                                      optionsHtcMock_.Partition,
                                                    },
                                                  });

    logger_.LogInformation("Session {sessionId} created",
                           createSessionReply.SessionId);

    return new SessionClient(channel_,
                             createSessionReply.SessionId,
                             logger_);
  }
}
