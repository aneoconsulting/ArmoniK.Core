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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class RunningTaskProcessor : BackgroundService
{
  private readonly ExceptionManager              exceptionManager_;
  private readonly ILogger<RunningTaskProcessor> logger_;
  private readonly PostProcessingTaskQueue       postProcessingTaskQueue_;
  private readonly RunningTaskQueue              runningTaskQueue_;

  public RunningTaskProcessor(RunningTaskQueue              runningTaskQueue,
                              PostProcessingTaskQueue       postProcessingTaskQueue,
                              ExceptionManager              exceptionManager,
                              ILogger<RunningTaskProcessor> logger)
  {
    runningTaskQueue_        = runningTaskQueue;
    postProcessingTaskQueue_ = postProcessingTaskQueue;
    logger_                  = logger;
    exceptionManager_        = exceptionManager;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    await using var closeWriter = new Deferrer(postProcessingTaskQueue_.CloseWriter);
    await using var closeReader = new Deferrer(runningTaskQueue_.CloseReader);

    logger_.LogDebug("Start running task processing service");
    while (!exceptionManager_.EarlyCancellationToken.IsCancellationRequested)
    {
      try
      {
        TaskHandler taskHandler;
        try
        {
          taskHandler = await runningTaskQueue_.ReadAsync(Timeout.InfiniteTimeSpan,
                                                          exceptionManager_.EarlyCancellationToken)
                                               .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
          break;
        }

        await using var taskHandlerDispose = new Deferrer(taskHandler);

        var taskInfo = taskHandler.GetAcquiredTaskInfo();

        using var _ = logger_.BeginPropertyScope(("messageHandler", taskInfo.MessageId),
                                                 ("taskId", taskInfo.TaskId),
                                                 ("sessionId", taskInfo.SessionId));
        await taskHandler.ExecuteTask()
                         .ConfigureAwait(false);
        await postProcessingTaskQueue_.WriteAsync(taskHandler,
                                                  Timeout.InfiniteTimeSpan,
                                                  exceptionManager_.LateCancellationToken)
                                      .ConfigureAwait(false);

        taskHandlerDispose.Reset();
      }
      catch (ChannelClosedException)
      {
        break;
      }
      catch (TaskPausedException)
      {
        break;
      }
      catch (Exception e)
      {
        exceptionManager_.RecordError(logger_,
                                      e,
                                      "Error while executing task");
      }
    }

    logger_.LogWarning("End of running task processor; no more tasks will be executed");
  }
}
