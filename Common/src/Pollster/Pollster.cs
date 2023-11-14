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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class Pollster : IInitializable
{
  private readonly ActivitySource                            activitySource_;
  private readonly IAgentHandler                             agentHandler_;
  private readonly DataPrefetcher                            dataPrefetcher_;
  private readonly IHostApplicationLifetime                  lifeTime_;
  private readonly ILogger<Pollster>                         logger_;
  private readonly ILoggerFactory                            loggerFactory_;
  private readonly int                                       messageBatchSize_;
  private readonly IObjectStorage                            objectStorage_;
  private readonly string                                    ownerPodId_;
  private readonly string                                    ownerPodName_;
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
                  IHostApplicationLifetime   lifeTime,
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
                  RunningTaskQueue           runningTaskQueue)
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
    lifeTime_              = lifeTime;
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
    ownerPodId_            = LocalIpFinder.LocalIpv4Address();
    ownerPodName_          = Dns.GetHostName();
    Failed                 = false;
  }

  public ICollection<string> TaskProcessing
    => taskProcessingDict_.Keys;

  /// <summary>
  ///   Is true when the MainLoop exited with an error
  ///   Used in Unit tests
  /// </summary>
  public bool Failed { get; private set; }

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

    return result;
  }

  public async Task StopCancelledTask()
  {
    foreach (var taskHandler in taskProcessingDict_.Values)
    {
      await taskHandler.StopCancelledTask()
                       .ConfigureAwait(false);
    }
  }

  public async Task MainLoop(CancellationToken cancellationToken)
  {
    await Init(cancellationToken)
      .ConfigureAwait(false);

    var cts = new CancellationTokenSource();
    cancellationToken.Register(() =>
                               {
                                 logger_.LogTrace("Global cancellation has been triggered.");
                                 cts.Cancel();
                               });
    var recordedErrors = new Queue<Exception>();

    void RecordError(Exception e)
    {
      if (pollsterOptions_.MaxErrorAllowed < 0)
      {
        return;
      }

      recordedErrors.Enqueue(e);

      if (recordedErrors.Count <= pollsterOptions_.MaxErrorAllowed)
      {
        return;
      }

      logger_.LogError("Too many consecutive errors in MainLoop. Stopping processing");
      healthCheckFailedResult_ = HealthCheckResult.Unhealthy("Too many consecutive errors in MainLoop");
      cts.Cancel();

      throw new TooManyException(recordedErrors.ToArray());
    }

    try
    {
      logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop");
      while (!cancellationToken.IsCancellationRequested)
      {
        if (healthCheckFailedResult_ is not null)
        {
          logger_.LogWarning("Health Check failed thus no more tasks will be executed.");
          cts.Cancel();
          return;
        }

        logger_.LogTrace("Trying to fetch messages");

        logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop.{nameof(pullQueueStorage_.PullMessagesAsync)}");

        try
        {
          await using var messages = pullQueueStorage_.PullMessagesAsync(messageBatchSize_,
                                                                         cancellationToken)
                                                      .GetAsyncEnumerator(cancellationToken);

          while (await messages.MoveNextAsync()
                               .ConfigureAwait(false))
          {
            var message           = messages.Current;
            var taskHandlerLogger = loggerFactory_.CreateLogger<TaskHandler>();
            using var _ = taskHandlerLogger.BeginNamedScope("Prefetch messageHandler",
                                                            ("messageHandler", message.MessageId),
                                                            ("taskId", message.TaskId),
                                                            ("ownerPodId", ownerPodId_));

            // ReSharper disable once ExplicitCallerInfoArgument
            using var activity = activitySource_.StartActivity("ProcessQueueMessage");
            activity?.SetBaggage("TaskId",
                                 message.TaskId);
            activity?.SetBaggage("messageId",
                                 message.MessageId);

            taskHandlerLogger.LogDebug("Start a new Task to process the messageHandler");

            try
            {
              while (runningTaskQueue_.RemoveException(out var exception))
              {
                if (exception is RpcException rpcException && TaskHandler.IsStatusFatal(rpcException.StatusCode))
                {
                  // This exception should stop pollster
                  exception.RethrowWithStacktrace();
                }

                RecordError(exception);
              }
            }
            catch
            {
              // This catch is used to properly dispose every message when the pollster should stop due to
              // too many errors or fatal error (broken worker, for instance)
              // Messages may not be all disposed in case the DisposeAsync method throws an exception
              // whereas it should not (good practices encourage to avoid this behavior)
              logger_.LogDebug("Start of queue messages disposition");

              await message.DisposeAsync()
                           .ConfigureAwait(false);

              while (await messages.MoveNextAsync()
                                   .ConfigureAwait(false))
              {
                await messages.Current.DisposeAsync()
                              .ConfigureAwait(false);
              }

              logger_.LogDebug("End of queue messages disposition");

              throw;
            }

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
                                              () => taskProcessingDict_.TryRemove(message.TaskId,
                                                                                  out var _),
                                              cts);

            if (!taskProcessingDict_.TryAdd(message.TaskId,
                                            taskHandler))
            {
              message.Status = QueueMessageStatus.Processed;
              await taskHandler.DisposeAsync()
                               .ConfigureAwait(false);
              continue;
            }


            try
            {
              var precondition = await taskHandler.AcquireTask()
                                                  .ConfigureAwait(false);

              if (precondition)
              {
                try
                {
                  await taskHandler.PreProcessing()
                                   .ConfigureAwait(false);
                }
                catch
                {
                  await taskHandler.DisposeAsync()
                                   .ConfigureAwait(false);
                  throw;
                }

                await runningTaskQueue_.WriteAsync(taskHandler,
                                                   cancellationToken)
                                       .ConfigureAwait(false);

                await runningTaskQueue_.WaitForNextWriteAsync(pollsterOptions_.TimeoutBeforeNextAcquisition,
                                                              cancellationToken)
                                       .ConfigureAwait(false);

                // If the task was successful, we can remove a failure
                if (recordedErrors.Count > 0)
                {
                  recordedErrors.Dequeue();
                }
              }
              else
              {
                await taskHandler.DisposeAsync()
                                 .ConfigureAwait(false);
              }
            }
            catch (RpcException e) when (TaskHandler.IsStatusFatal(e.StatusCode))
            {
              // This exception should stop pollster
              throw;
            }
            catch (Exception e)
            {
              RecordError(e);
            }
          }
        }
        catch (RpcException e) when (e.StatusCode == StatusCode.Unavailable)
        {
          // This exception should stop pollster
          healthCheckFailedResult_ = HealthCheckResult.Unhealthy("Worker unavailable",
                                                                 e);
          cts.Cancel();
          throw;
        }
        catch (TooManyException)
        {
          // This exception should not be caught here
          throw;
        }
        catch (Exception e)
        {
          logger_.LogError(e,
                           "Error while processing the messages from the queue");
          RecordError(e);
        }
      }
    }
    catch (Exception e)
    {
      Failed = true;
      logger_.LogCritical(e,
                          "Error in pollster");
    }
    finally
    {
      logger_.LogWarning("End of Pollster main loop");
      endLoopReached_ = true;
    }
  }

  private sealed class TooManyException : AggregateException
  {
    public TooManyException(Exception[] inner)
      : base(inner)
    {
    }
  }
}
