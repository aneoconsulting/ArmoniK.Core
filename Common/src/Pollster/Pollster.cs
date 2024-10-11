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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
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
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class Pollster : IInitializable
{
  private readonly ActivitySource                            activitySource_;
  private readonly IAgentHandler                             agentHandler_;
  private readonly DataPrefetcher                            dataPrefetcher_;
  private readonly ExceptionManager                          exceptionManager_;
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
                  MeterHolder                meterHolder)
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

  public ICollection<string> TaskProcessing
    => taskProcessingDict_.Keys;

  public async Task Init(CancellationToken cancellationToken)
    => await Task.WhenAll(pullQueueStorage_.Init(cancellationToken),
                          dataPrefetcher_.Init(cancellationToken),
                          workerStreamHandler_.Init(cancellationToken),
                          objectStorage_.Init(cancellationToken),
                          resultTable_.Init(cancellationToken),
                          sessionTable_.Init(cancellationToken),
                          taskTable_.Init(cancellationToken))
                 .ConfigureAwait(false);

  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (healthCheckFailedResult_ is not null)
    {
      return healthCheckFailedResult_ ?? HealthCheckResult.Unhealthy("Health Check failed previously so this polling agent should be destroyed.");
    }

    if (endLoopReached_)
    {
      return HealthCheckResult.Unhealthy("End of main loop reached, no more tasks will be executed.");
    }

    var checks = new List<Task<HealthCheckResult>>
                 {
                   pullQueueStorage_.Check(tag),
                   dataPrefetcher_.Check(tag),
                   workerStreamHandler_.Check(tag),
                   objectStorage_.Check(tag),
                   resultTable_.Check(tag),
                   sessionTable_.Check(tag),
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

    var result = new HealthCheckResult(worstStatus,
                                       description.ToString(),
                                       new AggregateException(exceptions),
                                       data);

    if (worstStatus == HealthStatus.Unhealthy && tag == HealthCheckTag.Liveness)
    {
      healthCheckFailedResult_ = result;
    }

    if (tag == HealthCheckTag.Readiness && taskProcessingDict_.IsEmpty)
    {
      return HealthCheckResult.Unhealthy("No tasks to process");
    }

    return result;
  }

  public async Task StopCancelledTask()
  {
    foreach (var taskHandler in taskProcessingDict_.Values.ToArray())
    {
      await taskHandler.StopCancelledTask()
                       .ConfigureAwait(false);
    }
  }

  public async Task MainLoop()
  {
    try
    {
      await Init(exceptionManager_.EarlyCancellationToken)
        .ConfigureAwait(false);

      logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop");
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
                                              new FunctionExecutionMetrics<TaskHandler>(meterHolder_));
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
              catch (TimeoutException)
              {
                await taskHandler.ReleaseAndPostponeTask()
                                 .ConfigureAwait(false);
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
