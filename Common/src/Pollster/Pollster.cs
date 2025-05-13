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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Controls the polling and processing of tasks from the queue, manages task execution lifecycle,
///   and provides health check capabilities.
/// </summary>
public class Pollster : IInitializable
{
  // Fields documentation
  private readonly ActivitySource                            activitySource_;
  private readonly IAgentHandler                             agentHandler_;
  private readonly DataPrefetcher                            dataPrefetcher_;
  private readonly ExceptionManager                          exceptionManager_;
  private readonly HealthCheckRecord                         healthCheckRecord_;
  private readonly ILogger<Pollster>                         logger_;
  private readonly ILoggerFactory                            loggerFactory_;
  private readonly int                                       messageBatchSize_;
  private readonly MeterHolder                               meterHolder_;
  private readonly IObjectStorage                            objectStorage_;
  private readonly string                                    ownerPodId_;
  private readonly string                                    ownerPodName_;
  private readonly Counter<int>                              pipeliningCounter_;
  private readonly Injection.Options.Pollster                pollsterOptions_;
  private readonly IPullQueueStorage                         pullQueueStorage_;
  private readonly IResultTable                              resultTable_;
  private readonly RunningTaskQueue                          runningTaskQueue_;
  private readonly ISessionTable                             sessionTable_;
  private readonly ISubmitter                                submitter_;
  private readonly ITaskProcessingChecker                    taskProcessingChecker_;
  private readonly ConcurrentDictionary<string, TaskHandler> taskProcessingDict_ = new();
  private readonly ITaskTable                                taskTable_;
  private readonly IWorkerStreamHandler                      workerStreamHandler_;
  private          bool                                      endLoopReached_;
  private          HealthCheckResult?                        healthCheckFailedResult_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="Pollster" /> class.
  /// </summary>
  /// <param name="pullQueueStorage">The storage service for pulling tasks from the queue.</param>
  /// <param name="dataPrefetcher">The service to prefetch data needed for task execution.</param>
  /// <param name="options">Configuration options for the compute plane.</param>
  /// <param name="pollsterOptions">Specific options for the pollster behavior.</param>
  /// <param name="exceptionManager">Manager to handle and record exceptions.</param>
  /// <param name="activitySource">Source for activity tracking and tracing.</param>
  /// <param name="logger">Logger for the pollster.</param>
  /// <param name="loggerFactory">Factory to create loggers for child components.</param>
  /// <param name="objectStorage">Storage for task-related objects.</param>
  /// <param name="resultTable">Table to store task results.</param>
  /// <param name="submitter">Service to submit new tasks.</param>
  /// <param name="sessionTable">Table to manage sessions.</param>
  /// <param name="taskTable">Table to store and retrieve task information.</param>
  /// <param name="taskProcessingChecker">Service to check if tasks can be processed.</param>
  /// <param name="workerStreamHandler">Handler for worker streams.</param>
  /// <param name="agentHandler">Handler for agents.</param>
  /// <param name="runningTaskQueue">Queue for running tasks.</param>
  /// <param name="identifier">Identifier for the agent running the pollster.</param>
  /// <param name="meterHolder">Holder for metrics collection.</param>
  /// <param name="healthCheckRecord">Record for the health check of the application.</param>
  /// <exception cref="ArgumentOutOfRangeException">Thrown when message batch size is less than 1.</exception>
  public Pollster(IPullQueueStorage          pullQueueStorage,
                  DataPrefetcher             dataPrefetcher,
                  ComputePlane               options,
                  Injection.Options.Pollster pollsterOptions,
                  ExceptionManager           exceptionManager,
                  ActivitySource             activitySource,
                  ILogger<Pollster>          logger,
                  ILoggerFactory             loggerFactory,
                  IObjectStorage             objectStorage,
                  IResultTable               resultTable,
                  ISubmitter                 submitter,
                  ISessionTable              sessionTable,
                  ITaskTable                 taskTable,
                  ITaskProcessingChecker     taskProcessingChecker,
                  IWorkerStreamHandler       workerStreamHandler,
                  IAgentHandler              agentHandler,
                  RunningTaskQueue           runningTaskQueue,
                  AgentIdentifier            identifier,
                  MeterHolder                meterHolder,
                  HealthCheckRecord          healthCheckRecord)
  {
    if (options.MessageBatchSize < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"The minimum value for {nameof(ComputePlane.MessageBatchSize)} is 1.");
    }

    logger_                = logger;
    loggerFactory_         = loggerFactory;
    activitySource_        = activitySource;
    pullQueueStorage_      = pullQueueStorage;
    exceptionManager_      = exceptionManager;
    dataPrefetcher_        = dataPrefetcher;
    pollsterOptions_       = pollsterOptions;
    messageBatchSize_      = options.MessageBatchSize;
    objectStorage_         = objectStorage;
    resultTable_           = resultTable;
    submitter_             = submitter;
    sessionTable_          = sessionTable;
    taskTable_             = taskTable;
    taskProcessingChecker_ = taskProcessingChecker;
    workerStreamHandler_   = workerStreamHandler;
    agentHandler_          = agentHandler;
    runningTaskQueue_      = runningTaskQueue;
    meterHolder_           = meterHolder;
    healthCheckRecord_     = healthCheckRecord;
    ownerPodId_            = identifier.OwnerPodId;
    ownerPodName_          = identifier.OwnerPodName;

    var started = DateTime.UtcNow;
    meterHolder_.Meter.CreateObservableCounter("uptime",
                                               () => (DateTime.UtcNow - started).TotalMilliseconds,
                                               "Milliseconds",
                                               "Duration from pollster start",
                                               meterHolder_.Tags);

    pipeliningCounter_ = meterHolder_.Meter.CreateCounter<int>("pipeline",
                                                               "Tasks",
                                                               "Number of tasks in the pipeline",
                                                               meterHolder_.Tags);

    meterHolder.Meter.CreateObservableGauge("TaskRunningTime",
                                            () => taskProcessingDict_.Values.MinBy(taskHandler => taskHandler.StartedAt) switch
                                                  {
                                                    null => 0.0,
                                                    // ReSharper disable once PatternAlwaysMatches
                                                    TaskHandler taskHandler => (DateTime.UtcNow - taskHandler.StartedAt).TotalMilliseconds,
                                                  },
                                            "Milliseconds",
                                            "Running time of the oldest task still processing by the Pollster",
                                            meterHolder_.Tags);

    meterHolder.Meter.CreateObservableGauge("TaskStartTime",
                                            () => (taskProcessingDict_.Values.MinBy(taskHandler => taskHandler.StartedAt) switch
                                                   {
                                                     null => TimeSpan.MaxValue,
                                                     // ReSharper disable once PatternAlwaysMatches
                                                     TaskHandler taskHandler => taskHandler.StartedAt - DateTime.UnixEpoch,
                                                   }).TotalSeconds,
                                            "Seconds",
                                            "Start time of the oldest task still processing by the Pollster",
                                            meterHolder_.Tags);
  }

  /// <summary>
  ///   Gets the collection of task IDs that are currently being processed.
  /// </summary>
  public ICollection<string> TaskProcessing
    => taskProcessingDict_.Keys;

  /// <summary>
  ///   Initializes the pollster and its dependencies.
  /// </summary>
  /// <param name="cancellationToken">Token to cancel the initialization.</param>
  /// <returns>A task representing the asynchronous initialization operation.</returns>
  public async Task Init(CancellationToken cancellationToken)
    => await Task.WhenAll(pullQueueStorage_.Init(cancellationToken),
                          dataPrefetcher_.Init(cancellationToken),
                          workerStreamHandler_.Init(cancellationToken),
                          objectStorage_.Init(cancellationToken),
                          resultTable_.Init(cancellationToken),
                          sessionTable_.Init(cancellationToken),
                          taskTable_.Init(cancellationToken))
                 .ConfigureAwait(false);

  /// <summary>
  ///   Performs health checks on the pollster and its dependencies.
  /// </summary>
  /// <param name="tag">The type of health check to perform.</param>
  /// <returns>The health check result.</returns>
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (healthCheckFailedResult_ is not null)
    {
      return healthCheckFailedResult_ ?? HealthCheckResult.Unhealthy("Health Check failed previously so this polling agent should be destroyed.");
    }

    if (endLoopReached_ && tag != HealthCheckTag.Liveness)
    {
      return HealthCheckResult.Unhealthy("End of main loop reached, no more tasks will be executed.");
    }

    // no need for description because this check is registered as the agent health check and it will add proper metadata.
    var result = await HealthCheckResultCombiner.Combine(tag,
                                                         string.Empty,
                                                         pullQueueStorage_,
                                                         dataPrefetcher_,
                                                         workerStreamHandler_,
                                                         objectStorage_,
                                                         resultTable_,
                                                         sessionTable_,
                                                         taskTable_)
                                                .ConfigureAwait(false);

    if (result.Status == HealthStatus.Unhealthy && tag == HealthCheckTag.Liveness)
    {
      healthCheckFailedResult_ = result;
    }

    if (tag == HealthCheckTag.Readiness && taskProcessingDict_.IsEmpty)
    {
      return HealthCheckResult.Unhealthy("No tasks to process");
    }

    return result;
  }

  /// <summary>
  ///   Stops any cancelled tasks that are currently being processed.
  /// </summary>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task StopCancelledTask()
  {
    foreach (var taskHandler in taskProcessingDict_.Values.ToArray())
    {
      await taskHandler.StopCancelledTask()
                       .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Main processing loop for the pollster that fetches and processes tasks.
  /// </summary>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task MainLoop()
  {
    try
    {
      await Init(exceptionManager_.EarlyCancellationToken)
        .ConfigureAwait(false);

      logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop");

      var acquisitionRetry = 0;

      while (!exceptionManager_.EarlyCancellationToken.IsCancellationRequested)
      {
        if (healthCheckFailedResult_ is not null)
        {
          var hcr = healthCheckFailedResult_.Value;
          exceptionManager_.FatalError(logger_,
                                       hcr.Exception,
                                       "Health Check failed with status {Status} thus no more tasks will be executed:\n{Description}",
                                       hcr.Status,
                                       hcr.Description);
          return;
        }

        logger_.LogTrace("Trying to fetch messages");

        logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop.{nameof(pullQueueStorage_.PullMessagesAsync)}");

        try
        {
          await using var messages = pullQueueStorage_.PullMessagesAsync(messageBatchSize_,
                                                                         exceptionManager_.EarlyCancellationToken)
                                                      .GetAsyncEnumerator(exceptionManager_.EarlyCancellationToken);

          // `messagesDispose` is guaranteed to be disposed *before* `messages` because it is defined _after_
          await using var messagesDispose = new Deferrer([SuppressMessage("ReSharper",
                                                                          "AccessToDisposedClosure")]
                                                         async () =>
                                                         {
                                                           // This deferrer is used to properly dispose every message when the pollster should stop due to
                                                           // too many errors or fatal error (broken worker, for instance)
                                                           // Messages may not be all disposed in case the DisposeAsync method throws an exception
                                                           // whereas it should not (good practices encourage to avoid this behavior)
                                                           var first = true;

                                                           while (await messages.MoveNextAsync()
                                                                                .ConfigureAwait(false))
                                                           {
                                                             if (first)
                                                             {
                                                               logger_.LogDebug("Start of queue messages disposition");
                                                               first = false;
                                                             }

                                                             await messages.Current.DisposeIgnoreErrorAsync(logger_)
                                                                           .ConfigureAwait(false);
                                                           }
                                                         });

          while (await messages.MoveNextAsync()
                               .ConfigureAwait(false))
          {
            var             message        = messages.Current;
            await using var messageDispose = new Deferrer(() => message.DisposeIgnoreErrorAsync(logger_));

            var taskHandlerLogger = loggerFactory_.CreateLogger<TaskHandler>();
            using var _ = taskHandlerLogger.BeginNamedScope("Prefetch messageHandler",
                                                            ("messageHandler", message.MessageId),
                                                            ("taskId", message.TaskId),
                                                            ("ownerPodId", ownerPodId_));

            taskHandlerLogger.LogDebug("Start a new Task to process the messageHandler");

            var taskHandler = new TaskHandler(sessionTable_,
                                              taskTable_,
                                              resultTable_,
                                              submitter_,
                                              dataPrefetcher_,
                                              workerStreamHandler_,
                                              message,
                                              taskProcessingChecker_,
                                              ownerPodId_,
                                              ownerPodName_,
                                              activitySource_,
                                              agentHandler_,
                                              taskHandlerLogger,
                                              pollsterOptions_,
                                              () =>
                                              {
                                                taskProcessingDict_.TryRemove(message.TaskId,
                                                                              out var _);
                                                pipeliningCounter_.Add(-1);
                                              },
                                              exceptionManager_,
                                              new FunctionExecutionMetrics<TaskHandler>(meterHolder_),
                                              healthCheckRecord_);
            pipeliningCounter_.Add(1);
            // Message has been "acquired" by the taskHandler and will be disposed by the TaskHandler
            messageDispose.Reset();

            // Automatically dispose the taskHandler in case of error.
            // Once the taskHandler has been successfully sent,
            // the responsibility of the dispose is transferred.
            await using var taskHandlerDispose = new Deferrer(taskHandler);

            if (!taskProcessingDict_.TryAdd(message.TaskId,
                                            taskHandler))
            {
              message.Status = QueueMessageStatus.Processed;
              // TaskHandler is disposed automatically by `taskHandlerDispose`
              continue;
            }

            try
            {
              if (await taskHandler.AcquireTask()
                                   .ConfigureAwait(false) != AcquisitionStatus.Acquired)
              {
                // TaskHandler is disposed automatically by `taskHandlerDispose`
                continue;
              }

              await taskHandler.PreProcessing()
                               .ConfigureAwait(false);

              try
              {
                await runningTaskQueue_.WriteAsync(taskHandler,
                                                   pollsterOptions_.TimeoutBeforeNextAcquisition,
                                                   exceptionManager_.EarlyCancellationToken)
                                       .ConfigureAwait(false);

                // TaskHandler has been successfully sent to the next stage of the pipeline
                // So remove the automatic dispose of the TaskHandler
                taskHandlerDispose.Reset();
              }
              catch (Exception e)
              {
                await taskHandler.ReleaseAndPostponeTask()
                                 .ConfigureAwait(false);

                switch (e)
                {
                  // If there is still a running task after the acquire timeout
                  case TimeoutException:
                    // If we still have acquisition retries available, continue right away with the next message
                    acquisitionRetry += 1;
                    if (acquisitionRetry < pollsterOptions_.NbAcquisitionRetry)
                    {
                      break;
                    }

                    acquisitionRetry = 0;

                    // Otherwise, we just wait for the running task to finish before trying to acquire a new task
                    try
                    {
                      logger_.LogDebug("Too many acquire timeouts, waiting for processing task to finish");

                      // We dispose early the messages in order to avoid blocking them while not trying to acquire their corresponding tasks
                      // Disposing twice is safe as the second dispose (from the using) will just do nothing.
                      // ReSharper disable once DisposeOnUsingVariable
                      await taskHandlerDispose.DisposeAsync()
                                              .ConfigureAwait(false);
                      // ReSharper disable once DisposeOnUsingVariable
                      await messagesDispose.DisposeAsync()
                                           .ConfigureAwait(false);
                      await runningTaskQueue_.WaitForReader(Timeout.InfiniteTimeSpan,
                                                            exceptionManager_.EarlyCancellationToken)
                                             .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }

                    break;
                  // If the cancellation token has been triggered, we ignore the error and continue right away
                  case OperationCanceledException:
                    break;
                  // In case of any other error, we record the error and continue right away
                  default:
                    throw;
                }
              }
            }
            catch (Exception e)
            {
              exceptionManager_.RecordError(logger_,
                                            e,
                                            "Error during Task Pre-Processing");
            }
          }
        }
        catch (RpcException e) when (TaskHandler.IsStatusFatal(e.StatusCode))
        {
          // This exception should stop pollster
          healthCheckFailedResult_ = HealthCheckResult.Unhealthy("Worker unavailable",
                                                                 e);

          exceptionManager_.FatalError(logger_,
                                       e,
                                       "Worker unavailable");
          break;
        }
        catch (Exception e)
        {
          exceptionManager_.RecordError(logger_,
                                        e,
                                        "Error while processing the messages from the queue");
        }
      }

      exceptionManager_.Stop(logger_,
                             "End of Pollster main loop: Stop the application");
    }
    catch (Exception e)
    {
      healthCheckFailedResult_ = HealthCheckResult.Unhealthy("Error in pollster",
                                                             e);

      exceptionManager_.FatalError(logger_,
                                   e,
                                   "Error in pollster: Stop the application");
    }
    finally
    {
      runningTaskQueue_.CloseWriter();
      endLoopReached_ = true;
    }
  }
}
