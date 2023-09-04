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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class RunningTaskProcessor : BackgroundService
{
  private readonly ILogger<RunningTaskProcessor> logger_;
  private readonly PostProcessingTaskQueue       postProcessingTaskQueue_;
  private readonly RunningTaskQueue              runningTaskQueue_;
  public           string                        CurrentTask = string.Empty;

  public RunningTaskProcessor(RunningTaskQueue              runningTaskQueue,
                              PostProcessingTaskQueue       postProcessingTaskQueue,
                              ILogger<RunningTaskProcessor> logger)
  {
    runningTaskQueue_        = runningTaskQueue;
    postProcessingTaskQueue_ = postProcessingTaskQueue;
    logger_                  = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    logger_.LogDebug("Start running task processing service");
    while (!stoppingToken.IsCancellationRequested)
    {
      try
      {
        while (postProcessingTaskQueue_.RemoveException(out var exception))
        {
          runningTaskQueue_.AddException(exception);
        }

        var taskHandler = await runningTaskQueue_.ReadAsync(stoppingToken)
                                                 .ConfigureAwait(false);
        CurrentTask = taskHandler.GetAcquiredTask();
        await taskHandler.ExecuteTask()
                         .ConfigureAwait(false);
        await postProcessingTaskQueue_.WriteAsync(taskHandler,
                                                  stoppingToken)
                                      .ConfigureAwait(false);
      }
      catch (Exception e)
      {
        runningTaskQueue_.AddException(ExceptionDispatchInfo.Capture(e)
                                                            .SourceException);
      }
      finally
      {
        CurrentTask = string.Empty;
      }
    }
  }
}
