// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class Pollster
{
  private readonly ActivitySource           activitySource_;
  private readonly DataPrefetcher           dataPrefetcher_;
  private readonly IHostApplicationLifetime lifeTime_;
  private readonly ILogger<Pollster>        logger_;
  private readonly int                      messageBatchSize_;
  private readonly PreconditionChecker      preconditionChecker_;
  private readonly IQueueStorage            queueStorage_;
  private readonly IObjectStorageFactory    objectStorageFactory_;
  private readonly IResultTable             resultTable_;
  private readonly ISubmitter               submitter_;
  private readonly IWorkerStreamHandler     workerStreamHandler_;


  public Pollster(IQueueStorage            queueStorage,
                  PreconditionChecker      preconditionChecker,
                  DataPrefetcher           dataPrefetcher,
                  ComputePlan              options,
                  IHostApplicationLifetime lifeTime,
                  ActivitySource           activitySource,
                  ILogger<Pollster>        logger,
                  IObjectStorageFactory    objectStorageFactory,
                  IResultTable             resultTable,
                  ISubmitter               submitter,
                  IWorkerStreamHandler     workerStreamHandler
                  )
  {
    if (options.MessageBatchSize < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"The minimum value for {nameof(ComputePlan.MessageBatchSize)} is 1.");
    }

    logger_               = logger;
    activitySource_       = activitySource;
    queueStorage_         = queueStorage;
    lifeTime_             = lifeTime;
    preconditionChecker_  = preconditionChecker;
    dataPrefetcher_       = dataPrefetcher;
    messageBatchSize_     = options.MessageBatchSize;
    objectStorageFactory_ = objectStorageFactory;
    resultTable_          = resultTable;
    submitter_            = submitter;
    workerStreamHandler_  = workerStreamHandler;
  }

  public async Task Init(CancellationToken cancellationToken)
  {
    await queueStorage_.Init(cancellationToken)
                       .ConfigureAwait(false);
    await dataPrefetcher_.Init(cancellationToken)
                         .ConfigureAwait(false);
    await preconditionChecker_.Init(cancellationToken)
                              .ConfigureAwait(false);
    await workerStreamHandler_.Init(cancellationToken)
                              .ConfigureAwait(false);
    await objectStorageFactory_.Init(cancellationToken)
                               .ConfigureAwait(false);
    await resultTable_.Init(cancellationToken)
                      .ConfigureAwait(false);
  }

  public async Task MainLoop(CancellationToken cancellationToken)
  {
    await Init(cancellationToken)
      .ConfigureAwait(false);

    cancellationToken.Register(() => logger_.LogError("Global cancellation has been triggered."));
    try
    {
      logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop");
      while (!cancellationToken.IsCancellationRequested)
      {
        logger_.LogDebug("Trying to fetch messages");

        logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop.{nameof(queueStorage_.PullAsync)}");
        var messages = queueStorage_.PullAsync(messageBatchSize_,
                                               cancellationToken);

        await foreach (var message in messages.WithCancellation(cancellationToken)
                                              .ConfigureAwait(false))
        {
          await using var msg = message;

          using var scopedLogger = logger_.BeginNamedScope("Prefetch messageHandler",
                                                           ("messageHandler", message.MessageId),
                                                           ("taskId", message.TaskId));

          using var activity = activitySource_.StartActivity("ProcessQueueMessage");
          activity?.SetBaggage("TaskId",
                               message.TaskId);
          activity?.SetBaggage("messageId",
                               message.MessageId);

          logger_.LogDebug("Start a new Task to process the messageHandler");

          var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken,
                                                                            cancellationToken);
          try
          {
            logger_.LogDebug("Loading task data");

            var precondition = await preconditionChecker_.CheckPreconditionsAsync(message,
                                                                                  cancellationToken)
                                                         .ConfigureAwait(false);

            if (precondition is not null)
            {
              var taskData = precondition;

              logger_.LogDebug("Start prefetch data");
              var computeRequestStream = await dataPrefetcher_.PrefetchDataAsync(taskData,
                                                                                 cancellationToken)
                                                              .ConfigureAwait(false);

              logger_.LogDebug("Start a new Task to process the messageHandler");
              using var requestProcessor = new RequestProcessor(workerStreamHandler_,
                                                                objectStorageFactory_,
                                                                logger_,
                                                                submitter_,
                                                                resultTable_,
                                                                activitySource_);

              var processResult = await requestProcessor.ProcessAsync(message,
                                                                      taskData,
                                                                      computeRequestStream,
                                                                      cancellationToken)
                                                        .ConfigureAwait(false);

              logger_.LogDebug("CompleteProcessing task processing");


              await processResult.ConfigureAwait(false);

              logger_.LogDebug("Task returned");
            }
          }
          catch (Exception e)
          {
            logger_.LogWarning(e,
                               "Error with messageHandler {messageId}",
                               message.MessageId);
            throw;
          }
        }
      }
    }
    catch (Exception e)
    {
      logger_.LogCritical(e,
                          "Error in pollster");
    }

    lifeTime_.StopApplication();
  }
}
