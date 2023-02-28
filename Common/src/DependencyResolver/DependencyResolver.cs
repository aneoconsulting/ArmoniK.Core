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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.DependencyResolver;

public class DependencyResolver : BackgroundService, IInitializable
{
  private readonly ILogger<DependencyResolver> logger_;
  private readonly IPullQueueStorage           pullQueueStorage_;
  private readonly IPushQueueStorage           pushQueueStorage_;
  private readonly IResultTable                resultTable_;
  private readonly ITaskTable                  taskTable_;

  public DependencyResolver(IPullQueueStorage           pullQueueStorage,
                            IPushQueueStorage           pushQueueStorage,
                            ITaskTable                  taskTable,
                            IResultTable                resultTable,
                            ILogger<DependencyResolver> logger)
  {
    pullQueueStorage_ = pullQueueStorage;
    pushQueueStorage_ = pushQueueStorage;
    taskTable_        = taskTable;
    resultTable_      = resultTable;
    logger_           = logger;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    using var logFunction = logger_.LogFunction();
    while (!stoppingToken.IsCancellationRequested)
    {
      try
      {
        var messages = pullQueueStorage_.PullMessagesAsync(1,
                                                           stoppingToken);

        await foreach (var message in messages.WithCancellation(stoppingToken)
                                              .ConfigureAwait(false))
        {
          using var scopedLogger = logger_.BeginNamedScope("Prefetch messageHandler",
                                                           ("messageHandler", message.MessageId),
                                                           ("taskId", message.TaskId));

          var taskData = await taskTable_.ReadTaskAsync(message.TaskId,
                                                        stoppingToken)
                                         .ConfigureAwait(false);

          switch (taskData.Status)
          {
            case TaskStatus.Creating:

              var dependenciesStatus = await TaskLifeCycleHelper.CheckTaskDependencies(taskData,
                                                                                       resultTable_,
                                                                                       logger_,
                                                                                       stoppingToken)
                                                                .ConfigureAwait(false);
              logger_.LogInformation("task dependencies : {resolved}",
                                     dependenciesStatus);

              switch (dependenciesStatus)
              {
                case TaskLifeCycleHelper.DependenciesStatus.Aborted:
                  // not done means that another pod put this task in error so we do not need to do it a second time
                  // so nothing to do
                  if (await taskTable_.SetTaskErrorAsync(taskData.TaskId,
                                                         "One of the input data is aborted.",
                                                         stoppingToken)
                                      .ConfigureAwait(false))
                  {
                    await resultTable_.AbortTaskResults(taskData.SessionId,
                                                        taskData.TaskId,
                                                        stoppingToken)
                                      .ConfigureAwait(false);
                  }

                  message.Status = QueueMessageStatus.Cancelled;
                  break;
                case TaskLifeCycleHelper.DependenciesStatus.Available:
                  await pushQueueStorage_.PushMessagesAsync(new[]
                                                            {
                                                              taskData.TaskId,
                                                            },
                                                            taskData.Options.PartitionId,
                                                            taskData.Options.Priority,
                                                            stoppingToken)
                                         .ConfigureAwait(false);

                  await taskTable_.FinalizeTaskCreation(new[]
                                                        {
                                                          taskData.TaskId,
                                                        },
                                                        stoppingToken)
                                  .ConfigureAwait(false);
                  message.Status = QueueMessageStatus.Processed;
                  break;
                case TaskLifeCycleHelper.DependenciesStatus.Processing:
                  message.Status = QueueMessageStatus.Processed;
                  break;
                default:
                  throw new ArgumentOutOfRangeException();
              }

              break;
            case TaskStatus.Cancelling:
              logger_.LogInformation("Task is being cancelled");
              message.Status = QueueMessageStatus.Cancelled;
              await taskTable_.SetTaskCanceledAsync(taskData.TaskId,
                                                    CancellationToken.None)
                              .ConfigureAwait(false);
              await resultTable_.AbortTaskResults(taskData.SessionId,
                                                  taskData.TaskId,
                                                  CancellationToken.None)
                                .ConfigureAwait(false);
              break;
            case TaskStatus.Submitted:
            case TaskStatus.Dispatched:
            case TaskStatus.Completed:
            case TaskStatus.Error:
            case TaskStatus.Cancelled:
            case TaskStatus.Timeout:
            case TaskStatus.Processed:
            case TaskStatus.Processing:
              logger_.LogInformation("Task {status}, task will be removed from this queue",
                                     taskData.Status);
              message.Status = QueueMessageStatus.Processed;
              break;
            case TaskStatus.Unspecified:
            default:
              throw new ArgumentOutOfRangeException();
          }


          await message.DisposeAsync()
                       .ConfigureAwait(false);
        }
      }
      catch (Exception ex)
      {
        logger_.LogError(ex,
                         "Error during task processing");
        throw;
      }
    }
  }
}
