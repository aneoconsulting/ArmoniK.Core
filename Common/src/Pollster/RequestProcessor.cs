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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskCanceledException = System.Threading.Tasks.TaskCanceledException;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;
using TimeoutException = System.TimeoutException;

namespace ArmoniK.Core.Common.Pollster;

public class RequestProcessor : IDisposable
{
  private readonly ActivitySource        activitySource_;
  private readonly ILogger<Pollster>     logger_;
  private readonly IObjectStorageFactory objectStorageFactory_;
  private readonly IObjectStorage        resourcesStorage_;
  private readonly IResultTable          resultTable_;
  private readonly ISubmitter            submitter_;
  private readonly IWorkerStreamHandler  workerStreamHandler_;

  public RequestProcessor(IWorkerStreamHandler  workerStreamHandler,
                          IObjectStorageFactory objectStorageFactory,
                          ILogger<Pollster>     logger,
                          ISubmitter            submitter,
                          IResultTable          resultTable,
                          ActivitySource        activitySource)
  {
    workerStreamHandler_  = workerStreamHandler;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    submitter_            = submitter;
    resultTable_          = resultTable;
    activitySource_       = activitySource;
    resourcesStorage_     = objectStorageFactory.CreateResourcesStorage();
  }

  public async Task<Task> ProcessAsync(IQueueMessageHandler  messageHandler,
                                       TaskData              taskData,
                                       Queue<ComputeRequest> computeRequests,
                                       CancellationToken     cancellationToken)
  {
    try
    {
      var result = await ProcessInternalsAsync(taskData,
                                               computeRequests,
                                               cancellationToken)
                     .ConfigureAwait(false);

      messageHandler.Status = QueueMessageStatus.Processed;
      return result;
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Error while processing request");
      // TODO cancel session dispatch, cancel task ?
      //await submitter_.CancelDispatchSessionAsync(taskData.SessionId,
      //                                            dispatch.Id,
      //                                            cancellationToken)
      //                .ConfigureAwait(false);

      if (!await HandleExceptionAsync(e,
                                      taskData,
                                      messageHandler,
                                      cancellationToken)
             .ConfigureAwait(false))
      {
        throw;
      }

      throw new ArmoniKException("An error occurred while executing. Error has been managed.");
    }
  }


  [PublicAPI]
  private async Task<bool> HandleExceptionAsync(Exception            e,
                                                TaskData             taskData,
                                                IQueueMessageHandler messageHandler,
                                                CancellationToken    cancellationToken)
  {
    switch (e)
    {
      case TimeoutException:
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.TaskId,
                         taskData.SessionId);
        messageHandler.Status = QueueMessageStatus.Failed;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Timeout,
                                               CancellationToken.None)
                        .ConfigureAwait(false);
        return true;
      }
      case TaskCanceledException:
      {
        var details = string.Empty;

        if (messageHandler.CancellationToken.IsCancellationRequested)
        {
          details += "Message was cancelled. ";
        }

        if (cancellationToken.IsCancellationRequested)
        {
          details += "Root token was cancelled. ";
        }

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.TaskId,
                         taskData.SessionId,
                         details);
        messageHandler.Status = QueueMessageStatus.Cancelled;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Canceling,
                                               CancellationToken.None)
                        .ConfigureAwait(false);
        return true;
      }
      case ArmoniKException:
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.TaskId,
                         taskData.SessionId,
                         e.ToString());

        messageHandler.Status = QueueMessageStatus.Failed;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Error,
                                               CancellationToken.None)
                        .ConfigureAwait(false);
        await submitter_.ResubmitTask(taskData,
                                      cancellationToken)
                        .ConfigureAwait(false);
        return true;
      }
      case AggregateException ae:
      {
        foreach (var ie in ae.InnerExceptions)
          // If the exception was not handled, lazily allocate a list of unhandled
          // exceptions (to be rethrown later) and add it.
        {
          if (!await HandleExceptionAsync(ie,
                                          taskData,
                                          messageHandler,
                                          cancellationToken)
                 .ConfigureAwait(false))
          {
            return false;
          }
        }

        return true;
      }
      default:
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.TaskId,
                         taskData.SessionId);
        messageHandler.Status = QueueMessageStatus.Failed;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Error,
                                               CancellationToken.None)
                        .ConfigureAwait(false);
        await submitter_.ResubmitTask(taskData,
                                      cancellationToken)
                        .ConfigureAwait(false);
        return false;
      }
    }
  }

  public async Task<Task> ProcessInternalsAsync(TaskData              taskData,
                                                Queue<ComputeRequest> computeRequests,
                                                CancellationToken     cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}");
    activity?.SetBaggage("SessionId",
                         taskData.SessionId);
    activity?.SetBaggage("TaskId",
                         taskData.TaskId);

    logger_.LogDebug("Set task status to Processing");
    await submitter_.StartTask(taskData.TaskId,
                               cancellationToken)
                    .ConfigureAwait(false);

    workerStreamHandler_.StartTaskProcessing(taskData,
                                             cancellationToken);
    if (workerStreamHandler_.Pipe is null)
    {
      throw new ArmoniKException($"{nameof(IWorkerStreamHandler.Pipe)} should not be null");
    }

    var resultStorage = objectStorageFactory_.CreateResultStorage(taskData.SessionId);

    {
      using var activity2 = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.SendComputeRequests");
      // send the compute requests
      while (computeRequests.TryDequeue(out var computeRequest))
      {
        activity?.AddEvent(new ActivityEvent(computeRequest.TypeCase.ToString()));
        await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                   {
                                                     Compute = computeRequest,
                                                   })
                                  .ConfigureAwait(false);
      }
    }

    var requestProcessors = new ConcurrentDictionary<string, IProcessReplyProcessor>();

    activity?.AddEvent(new ActivityEvent("Processing ResponseStream"));

    await foreach (var reply in workerStreamHandler_.Pipe.Reader.WithCancellation(cancellationToken)
                                                    .ConfigureAwait(false))
    {
      if (reply.TypeCase == ProcessReply.TypeOneofCase.Output)
      {
        logger_.LogDebug("Received Task Output");

        async Task Epilog()
        {
          await requestProcessors.Values.Select(processor => processor.WaitForResponseCompletion(cancellationToken))
                                 .WhenAll()
                                 .ConfigureAwait(false);

          if (requestProcessors.Values.Any(processor => !processor.IsComplete()))
          {
            throw new ArmoniKException("All processors should be complete here");
          }

          await workerStreamHandler_.Pipe.CompleteAsync()
                                    .ConfigureAwait(false);

          if (reply.Output.TypeCase is Output.TypeOneofCase.Ok)
          {
            logger_.LogDebug("Complete processing of the request");
            await requestProcessors.Values.Select(processor => processor.CompleteProcessing(cancellationToken))
                                   .WhenAll()
                                   .ConfigureAwait(false);
          }

          await submitter_.CompleteTaskAsync(taskData.TaskId,
                                             reply.Output,
                                             cancellationToken)
                          .ConfigureAwait(false);

          logger_.LogDebug("End Task Epilog");
        }

        logger_.LogDebug("Start Task Epilog");
        // no await here because we want the epilog awaited outside of this function to pipeline task processing
        return Epilog();
      }

      if (string.IsNullOrEmpty(reply.RequestId))
      {
        logger_.LogWarning("No request Id in the received request, request will not be processed");
        switch (reply.TypeCase)
        {
          case ProcessReply.TypeOneofCase.Result:
            // todo result reply to acknowledge reception or say that there is an error
            // todo we need to improve the proto to have better communication and management of errors between worker and polling agent
            await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                       {
                                                         Resource = new ProcessRequest.Types.DataReply
                                                                    {
                                                                      Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                             {
                                                                               Error = "No request Id",
                                                                             },
                                                                      ReplyId = "Missing request Id",
                                                                    },
                                                       })
                                      .ConfigureAwait(false);
            break;
          case ProcessReply.TypeOneofCase.CreateLargeTask:
            await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                       {
                                                         CreateTask = new ProcessRequest.Types.CreateTask
                                                                      {
                                                                        Reply = new CreateTaskReply
                                                                                {
                                                                                  NonSuccessfullIds = new CreateTaskReply.Types.TaskIds
                                                                                                      {
                                                                                                        Ids =
                                                                                                        {
                                                                                                          "No request Id",
                                                                                                        },
                                                                                                      },
                                                                                },
                                                                        ReplyId = "Missing request Id",
                                                                      },
                                                       })
                                      .ConfigureAwait(false);
            break;
          case ProcessReply.TypeOneofCase.Resource:
            await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                       {
                                                         Resource = new ProcessRequest.Types.DataReply
                                                                    {
                                                                      Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                             {
                                                                               Error = "No request Id",
                                                                             },
                                                                      ReplyId = "Missing request Id",
                                                                    },
                                                       })
                                      .ConfigureAwait(false);
            break;
          case ProcessReply.TypeOneofCase.CommonData:
            await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                       {
                                                         CommonData = new ProcessRequest.Types.DataReply
                                                                      {
                                                                        Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                               {
                                                                                 Error = "No request Id",
                                                                               },
                                                                        ReplyId = "Missing request Id",
                                                                      },
                                                       })
                                      .ConfigureAwait(false);
            break;
          case ProcessReply.TypeOneofCase.DirectData:
            await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                       {
                                                         DirectData = new ProcessRequest.Types.DataReply
                                                                      {
                                                                        Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                               {
                                                                                 Error = "No request Id",
                                                                               },
                                                                        ReplyId = "Missing request Id",
                                                                      },
                                                       })
                                      .ConfigureAwait(false);
            break;
          case ProcessReply.TypeOneofCase.CreateSmallTask:
          case ProcessReply.TypeOneofCase.Output:
          case ProcessReply.TypeOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }

        await workerStreamHandler_.Pipe.CompleteAsync()
                                  .ConfigureAwait(false);

        return Task.CompletedTask;
      }

      var rp = requestProcessors.GetOrAdd(reply.RequestId,
                                          _ =>
                                          {
                                            logger_.LogDebug("Received new Reply of type {ReplyType} with Id {RequestId}",
                                                             reply.TypeCase,
                                                             reply.RequestId);
                                            return reply.TypeCase switch
                                                   {
                                                     ProcessReply.TypeOneofCase.Result => new
                                                       ResultProcessor(submitter_,
                                                                       taskData.SessionId,
                                                                       taskData.RetryOfIds.FirstOrDefault(taskData.TaskId),
                                                                       logger_),
                                                     ProcessReply.TypeOneofCase.CreateLargeTask => new CreateLargeTaskProcessor(submitter_,
                                                                                                                                workerStreamHandler_.Pipe,
                                                                                                                                taskData.SessionId,
                                                                                                                                taskData.TaskId,
                                                                                                                                logger_),
                                                     ProcessReply.TypeOneofCase.CreateSmallTask => throw new NotImplementedException(),
                                                     ProcessReply.TypeOneofCase.Resource => new ResourceRequestProcessor(resourcesStorage_,
                                                                                                                         workerStreamHandler_.Pipe,
                                                                                                                         logger_),
                                                     ProcessReply.TypeOneofCase.CommonData => new CommonDataRequestProcessor(resourcesStorage_,
                                                                                                                             workerStreamHandler_.Pipe,
                                                                                                                             logger_),
                                                     ProcessReply.TypeOneofCase.DirectData => new DirectDataRequestProcessor(resourcesStorage_,
                                                                                                                             workerStreamHandler_.Pipe,
                                                                                                                             logger_),
                                                     ProcessReply.TypeOneofCase.None   => throw new ArmoniKException("Unspecified process reply type"),
                                                     ProcessReply.TypeOneofCase.Output => throw new ArmoniKException("Unspecified process reply type"),
                                                     _                                 => throw new ArmoniKException("Unspecified process reply type"),
                                                   };
                                          });

      if (rp == null)
      {
        throw new ArmoniKException("request processor should not be null");
      }

      await rp.AddProcessReply(reply,
                               cancellationToken)
              .ConfigureAwait(false);
    }

    throw new ArmoniKException("This should never happen");
  }

  public void Dispose()
  {
    GC.SuppressFinalize(this);
  }
}