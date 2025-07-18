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
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   A background service that handles post-processing of completed tasks.
///   It reads task handlers from the post-processing queue and finalizes them
///   by performing result uploading, task submissions, message acknowledgment
///   and recording the success.
/// </summary>
public class PostProcessor : BackgroundService
{
  private readonly ExceptionManager        exceptionManager_;
  private readonly ILogger<PostProcessor>  logger_;
  private readonly PostProcessingTaskQueue postProcessingTaskQueue_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="PostProcessor" /> class.
  /// </summary>
  /// <param name="postProcessingTaskQueue">The queue containing tasks that need post-processing.</param>
  /// <param name="exceptionManager">The manager handling exceptions and cancellation.</param>
  /// <param name="logger">The logger for this class.</param>
  public PostProcessor(PostProcessingTaskQueue postProcessingTaskQueue,
                       ExceptionManager        exceptionManager,
                       ILogger<PostProcessor>  logger)
  {
    postProcessingTaskQueue_ = postProcessingTaskQueue;
    logger_                  = logger;
    exceptionManager_        = exceptionManager;
  }

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    exceptionManager_.Register();
    await using var closeReader = new Deferrer(postProcessingTaskQueue_.CloseReader);

    while (!exceptionManager_.LateCancellationToken.IsCancellationRequested)
    {
      try
      {
        var taskHandler = await postProcessingTaskQueue_.ReadAsync(Timeout.InfiniteTimeSpan,
                                                                   exceptionManager_.LateCancellationToken)
                                                        .ConfigureAwait(false);
        await using var taskHandlerDispose = new Deferrer(taskHandler);

        var taskInfo = taskHandler.GetAcquiredTaskInfo();

        using var _ = logger_.BeginPropertyScope(("messageHandler", taskInfo.MessageId),
                                                 ("taskId", taskInfo.TaskId),
                                                 ("sessionId", taskInfo.SessionId));

        await taskHandler.PostProcessing()
                         .ConfigureAwait(false);

        // Task processing has been successful.
        // Decrement number of recorded errors.
        exceptionManager_.RecordSuccess(logger_);
      }
      catch (ChannelClosedException)
      {
        break;
      }
      catch (OperationCanceledException) when (exceptionManager_.LateCancellationToken.IsCancellationRequested)
      {
        break;
      }
      catch (Exception e)
      {
        exceptionManager_.RecordError(logger_,
                                      e,
                                      "Error during task post processing");
      }
    }

    exceptionManager_.Stop(logger_,
                           "End of task post processor; no more tasks will be finalized");
  }
}
