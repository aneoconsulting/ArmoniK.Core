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

using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Notifies an agent through the <c>/stopcancelledtask</c> endpoint of its polling agent, on port 1080 of the address
///   recorded as owner of the task.
/// </summary>
public class HttpCancelledTaskNotifier : ICancelledTaskNotifier
{
  private readonly HttpClient                         httpClient_;
  private readonly ILogger<HttpCancelledTaskNotifier> logger_;

  /// <summary>
  ///   Creates the notifier
  /// </summary>
  /// <param name="httpClient">Client used to call the agents</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public HttpCancelledTaskNotifier(HttpClient                         httpClient,
                                   ILogger<HttpCancelledTaskNotifier> logger)
  {
    httpClient_ = httpClient;
    logger_     = logger;
  }

  /// <inheritdoc />
  /// <remarks>
  ///   An agent that refuses the connection is ignored, since it no longer runs the task. The request is not cancelled by
  ///   <paramref name="cancellationToken" />, as before this notifier was extracted.
  /// </remarks>
  public async Task NotifyAsync(string            ownerPodId,
                                CancellationToken cancellationToken)
  {
    try
    {
      await httpClient_.GetAsync("http://" + ownerPodId + ":1080/stopcancelledtask")
                       .ConfigureAwait(false);
    }
    // Ignore unreachable agents
    catch (HttpRequestException e) when (e is
                                         {
                                           InnerException: SocketException
                                                           {
                                                             SocketErrorCode: SocketError.ConnectionRefused,
                                                           },
                                         })
    {
      logger_.LogError(e,
                       "The agent with {OwnerPodId} was not reached successfully",
                       ownerPodId);
    }
  }
}
