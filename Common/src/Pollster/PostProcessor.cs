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
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class PostProcessor : BackgroundService
{
  private readonly ILogger<PostProcessor>  logger_;
  private readonly PostProcessingTaskQueue postProcessingTaskQueue_;

  public PostProcessor(PostProcessingTaskQueue postProcessingTaskQueue,
                       ILogger<PostProcessor>  logger)
  {
    postProcessingTaskQueue_ = postProcessingTaskQueue;
    logger_                  = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    while (!stoppingToken.IsCancellationRequested)
    {
      var taskHandler = await postProcessingTaskQueue_.ReadAsync(stoppingToken)
                                                      .ConfigureAwait(false);
      await using var taskHandlerDispose = new Deferrer(taskHandler);

      var taskInfo = taskHandler.GetAcquiredTaskInfo();

      using var _ = logger_.BeginPropertyScope(("messageHandler", taskInfo.MessageId),
                                               ("taskId", taskInfo.TaskId),
                                               ("sessionId", taskInfo.SessionId));

      try
      {
        await taskHandler.PostProcessing()
                         .ConfigureAwait(false);
      }
      catch (Exception e)
      {
        logger_.LogError(e,
                         "Error during task post processing");
        postProcessingTaskQueue_.AddException(ExceptionDispatchInfo.Capture(e)
                                                                   .SourceException);
      }
    }

    logger_.LogWarning("End of task post processor; no more tasks will be finalized");
  }
}
