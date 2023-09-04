// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

public class TaskProcessingCheckerClient : ITaskProcessingChecker
{
  private readonly IHttpClientFactory                   httpClientFactory_;
  private readonly ILogger<TaskProcessingCheckerClient> logger_;

  public TaskProcessingCheckerClient(IHttpClientFactory                   httpClientFactory,
                                     ILogger<TaskProcessingCheckerClient> logger)
  {
    httpClientFactory_ = httpClientFactory;
    logger_            = logger;
  }

  public async Task<bool> Check(string            taskId,
                                string            ownerPodId,
                                CancellationToken cancellationToken)
  {
    logger_.LogTrace("Check if task is processing");
    var client = httpClientFactory_.CreateClient();

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
}
