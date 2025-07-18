// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

/// <summary>
///   Implements the <see cref="ITaskProcessingChecker" /> interface to check if a task is being processed by a specific
///   pod.
///   Uses HTTP requests to communicate with other pods and determine task processing status, with retry and timeout logic.
/// </summary>
public class TaskProcessingCheckerClient : ITaskProcessingChecker
{
  private const    int                                  Retries = 5;
  private readonly IHttpClientFactory                   httpClientFactory_;
  private readonly ILogger<TaskProcessingCheckerClient> logger_;
  private readonly TimeSpan                             requestTimeout_ = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskProcessingCheckerClient" /> class.
  /// </summary>
  /// <param name="httpClientFactory">The HTTP client factory used to create HTTP clients for communication with other pods.</param>
  /// <param name="logger">The logger used for diagnostic and trace logging.</param>
  public TaskProcessingCheckerClient(IHttpClientFactory                   httpClientFactory,
                                     ILogger<TaskProcessingCheckerClient> logger)
  {
    httpClientFactory_ = httpClientFactory;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task<bool> Check(string            taskId,
                                string            ownerPodId,
                                CancellationToken cancellationToken)
  {
    logger_.LogTrace("Check if task is processing");
    var client = httpClientFactory_.CreateClient();
    client.Timeout = requestTimeout_;

    for (var i = 0; i < Retries; i++)
    {
      try
      {
        var result = await client.GetStringAsync("http://" + ownerPodId + ":1080/taskprocessing",
                                                 cancellationToken)
                                 .ConfigureAwait(false);
        logger_.LogDebug("Result from other polling agent: {result}",
                         result);
        return result.Split(",")
                     .Contains(taskId);
      }
      catch (InvalidOperationException ex)
      {
        logger_.LogWarning(ex,
                           "Cannot communicate with other pod");
        return false;
      }
      catch (OperationCanceledException ex)
      {
        logger_.LogWarning(ex,
                           "Cannot communicate with other pod due to timeout of {time}",
                           requestTimeout_);
        return false;
      }
      catch (HttpRequestException ex) when (ex.InnerException is SocketException
                                                                 {
                                                                   SocketErrorCode: SocketError.ConnectionRefused,
                                                                 })
      {
        logger_.LogWarning(ex,
                           "Cannot communicate with other pod");
      }
      catch (HttpRequestException ex)
      {
        logger_.LogWarning(ex,
                           "Cannot communicate with other pod");
        return false;
      }
      catch (UriFormatException ex)
      {
        logger_.LogWarning(ex,
                           "Invalid other pod hostname");
        return false;
      }
    }

    logger_.LogWarning("Too many tries to communicate with other pod");
    return false;
  }
}
