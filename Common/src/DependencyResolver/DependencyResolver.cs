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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.DependencyResolver;

/// <summary>
///   Service for checking the status of the dependencies from the tasks in the queue
/// </summary>
public class DependencyResolver : IInitializable
{
  private readonly ILogger<DependencyResolver> logger_;
  private readonly IPullQueueStorage           pullQueueStorage_;
  private readonly IPushQueueStorage           pushQueueStorage_;
  private readonly IResultTable                resultTable_;
  private readonly ITaskTable                  taskTable_;

  /// <summary>
  ///   Initializes the <see cref="DependencyResolver" />
  /// </summary>
  /// <param name="pullQueueStorage">Interface to get tasks from the queue</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
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
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    var checks = new List<Task<HealthCheckResult>>
                 {
                   pullQueueStorage_.Check(tag),
                   pushQueueStorage_.Check(tag),
                   resultTable_.Check(tag),
                   taskTable_.Check(tag),
                 };

    var exceptions  = new List<Exception>();
    var data        = new Dictionary<string, object>();
    var description = new StringBuilder();
    var worstStatus = HealthStatus.Healthy;

    foreach (var healthCheckResult in await checks.WhenAll()
                                                  .ConfigureAwait(false))
    {
      if (healthCheckResult.Status == HealthStatus.Healthy)
      {
        continue;
      }

      if (healthCheckResult.Exception is not null)
      {
        exceptions.Add(healthCheckResult.Exception);
      }

      foreach (var (key, value) in healthCheckResult.Data)
      {
        data[key] = value;
      }

      if (healthCheckResult.Description is not null)
      {
        description.AppendLine(healthCheckResult.Description);
      }

      worstStatus = worstStatus < healthCheckResult.Status
                      ? worstStatus
                      : healthCheckResult.Status;
    }

    return new HealthCheckResult(worstStatus,
                                 description.ToString(),
                                 new AggregateException(exceptions),
                                 data);
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    await pushQueueStorage_.Init(cancellationToken)
                           .ConfigureAwait(false);
    await pullQueueStorage_.Init(cancellationToken)
                           .ConfigureAwait(false);
    await resultTable_.Init(cancellationToken)
                      .ConfigureAwait(false);
    await taskTable_.Init(cancellationToken)
                    .ConfigureAwait(false);
  }

  /// <summary>
  ///   Long running task that pulls message that represents tasks from queue, check their dependencies and if dependencies
  ///   are available, put them in the appropriate queue
  /// </summary>
  /// <param name="stoppingToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task ExecuteAsync(CancellationToken stoppingToken)
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
