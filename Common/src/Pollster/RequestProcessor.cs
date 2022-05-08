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

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Output = ArmoniK.Api.gRPC.V1.Output;
using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using TaskCanceledException = System.Threading.Tasks.TaskCanceledException;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;
using TimeoutException = System.TimeoutException;

namespace ArmoniK.Core.Common.Pollster;

public class RequestProcessor : IDisposable
{
  private readonly ActivitySource                                    activitySource_;
  private readonly ILogger<Pollster>                                 logger_;
  private readonly IObjectStorageFactory                             objectStorageFactory_;
  private readonly IObjectStorage                                    resourcesStorage_;
  private readonly IResultTable                                      resultTable_;
  private readonly ISubmitter                                        submitter_;
  private readonly IWorkerStreamHandler                              workerStreamHandler_;
  private readonly List<(List<string> TaskIds, TaskOptions Options)> taskToFinalize_;

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
    taskToFinalize_        = new List<(List<string> TaskIds, TaskOptions Options)>();
  }

  public async Task<List<Task>> ProcessAsync(IQueueMessageHandler  messageHandler,
                                             TaskData              taskData,
                                             CancellationToken     cancellationToken)
  {
    try
    {
      var computeRequests = await workerStreamHandler_.StartTaskPrefetching(taskData,
                                                       cancellationToken).ConfigureAwait(false);

      taskToFinalize_.Clear();
      var result = await ProcessInternalsAsync(taskData, computeRequests,
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

  public async Task<List<Task>> ProcessInternalsAsync(TaskData              taskData,
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

    var resultStorage = objectStorageFactory_.CreateResultStorage(taskData.SessionId);

    workerStreamHandler_.WorkerRequestStream!.WriteOptions = new WriteOptions(WriteFlags.NoCompress);
    {
      using var activity2 = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.SendComputeRequests");
      // send the compute requests
      while (computeRequests.TryDequeue(out var computeRequest))
      {
        activity?.AddEvent(new ActivityEvent(computeRequest.TypeCase.ToString()));
        await workerStreamHandler_.WorkerRequestStream.WriteAsync(new ProcessRequest
                                                                  {
                                                                    Compute = computeRequest,
                                                                  },
                                                                  CancellationToken.None)
                                  .ConfigureAwait(false);
      }
    }

    var output = new List<Task>();

    var isComplete = false;

    activity?.AddEvent(new ActivityEvent("Processing ResponseStream"));
    // process incoming messages
    // TODO : To reduce memory consumption, do not generate subStream. Implement a state machine instead.
    await foreach (var singleReplyStream in workerStreamHandler_.WorkerResponseStream!.Separate(logger_,
                                                                                               cancellationToken)
                                                                .ConfigureAwait(false))
    {
      var       first     = singleReplyStream.First();
      using var activity2 = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.ProcessReply");
      if (isComplete)
      {
        throw new InvalidOperationException("Unexpected message from the worker after sending the task output");
      }

      activity?.AddEvent(new ActivityEvent(first.TypeCase.ToString()));
      switch (first.TypeCase)
      {
        case ProcessReply.TypeOneofCase.None:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply),
                                                $"received a {nameof(ProcessReply.TypeOneofCase.None)} reply type.");
        case ProcessReply.TypeOneofCase.Output:
          await output.WhenAll()
                      .ConfigureAwait(false);
          output.Clear();

          if (first.Output.TypeCase == Output.TypeOneofCase.Ok)
          {
            foreach (var (taskIds, options) in taskToFinalize_)
            {
              await submitter_.FinalizeTaskCreation(taskIds,
                                                    options,
                                                    cancellationToken)
                              .ConfigureAwait(false);
            }
          }

          await submitter_.CompleteTaskAsync(taskData.TaskId,
                                             first.Output,
                                             cancellationToken)
                          .ConfigureAwait(false);
          isComplete = true;
          break;
        case ProcessReply.TypeOneofCase.Result:
          output.Add(StoreResultAsync(resultStorage,
                                      first,
                                      singleReplyStream,
                                      taskData.SessionId,
                                      taskData.TaskId,
                                      cancellationToken));
          break;
        case ProcessReply.TypeOneofCase.CreateSmallTask:
          var replySmallTasksAsync = await SubmitSmallTasksAsync(taskData,
                                                                 first,
                                                                 cancellationToken)
                                       .ConfigureAwait(false);
          await workerStreamHandler_.WorkerRequestStream.WriteAsync(new ProcessRequest
                                                                    {
                                                                      CreateTask = new ProcessRequest.Types.CreateTask
                                                                                   {
                                                                                     Reply   = replySmallTasksAsync,
                                                                                     ReplyId = first.RequestId,
                                                                                   },
                                                                    },
                                                                    CancellationToken.None)
                                    .ConfigureAwait(false);
          break;
        case ProcessReply.TypeOneofCase.CreateLargeTask:
          var replyLargeTasksAsync = await SubmitLargeTasksAsync(taskData,
                                                                 first,
                                                                 singleReplyStream,
                                                                 cancellationToken)
                                       .ConfigureAwait(false);
          await workerStreamHandler_.WorkerRequestStream.WriteAsync(new ProcessRequest
                                                                    {
                                                                      CreateTask = new ProcessRequest.Types.CreateTask
                                                                                   {
                                                                                     Reply   = replyLargeTasksAsync,
                                                                                     ReplyId = first.RequestId,
                                                                                   },
                                                                    },
                                                                    CancellationToken.None)
                                    .ConfigureAwait(false);
          break;
        case ProcessReply.TypeOneofCase.Resource:
          await ProvideResourcesAsync(workerStreamHandler_.WorkerRequestStream,
                                      first,
                                      cancellationToken)
            .ConfigureAwait(false);
          break;
        case ProcessReply.TypeOneofCase.CommonData:
          await ProvideCommonDataAsync(workerStreamHandler_.WorkerRequestStream,
                                       first)
            .ConfigureAwait(false);
          break;
        case ProcessReply.TypeOneofCase.DirectData:
          await ProvideDirectDataAsync(workerStreamHandler_.WorkerRequestStream,
                                       first)
            .ConfigureAwait(false);
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply),
                                                "Unexpected message type in the stream. Wrong proto definition has been used ?");
      }
    }

    using (var _ = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.CloseStreams"))
    {
      await workerStreamHandler_.WorkerRequestStream.CompleteAsync()
                                .ConfigureAwait(false);
      await workerStreamHandler_.WorkerResponseStream!.MoveNext()
                                .ConfigureAwait(false);
    }

    if (!isComplete)
    {
      throw new InvalidOperationException("Unexpected end of stream from the worker");
    }

    return output;
  }


  private async Task ProvideResourcesAsync(IAsyncStreamWriter<ProcessRequest> requestStream,
                                           ProcessReply                       processReply,
                                           CancellationToken                  cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideResourcesAsync)}");
    var bytes = resourcesStorage_.GetValuesAsync(processReply.Resource.Key,
                                                 cancellationToken);

    await foreach (var dataReply in bytes.ToDataReply(processReply.RequestId,
                                                      processReply.Resource.Key,
                                                      cancellationToken)
                                         .WithCancellation(cancellationToken)
                                         .ConfigureAwait(false))
    {
      await requestStream.WriteAsync(new ProcessRequest
                                     {
                                       Resource = dataReply,
                                     }, CancellationToken.None)
                         .ConfigureAwait(false);
    }
  }

  [PublicAPI]
  public Task ProvideDirectDataAsync(IAsyncStreamWriter<ProcessRequest> streamRequestStream,
                                     ProcessReply                       reply)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideDirectDataAsync)}");
    return streamRequestStream.WriteAsync(new ProcessRequest
                                          {
                                            DirectData = new ProcessRequest.Types.DataReply
                                                         {
                                                           ReplyId = reply.RequestId,
                                                           Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                  {
                                                                    Key   = reply.CommonData.Key,
                                                                    Error = "Common data are not supported yet",
                                                                  },
                                                         },
                                          });
  }

  [PublicAPI]
  public Task ProvideCommonDataAsync(IAsyncStreamWriter<ProcessRequest> streamRequestStream,
                                     ProcessReply                       reply)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideCommonDataAsync)}");
    return streamRequestStream.WriteAsync(new ProcessRequest
                                          {
                                            CommonData = new ProcessRequest.Types.DataReply
                                                         {
                                                           ReplyId = reply.RequestId,
                                                           Init = new ProcessRequest.Types.DataReply.Types.Init
                                                                  {
                                                                    Key   = reply.CommonData.Key,
                                                                    Error = "Common data are not supported yet",
                                                                  },
                                                         },
                                          });
  }


  [PublicAPI]
  public async Task<CreateTaskReply> SubmitLargeTasksAsync(TaskData            taskData,
                                                     ProcessReply        first,
                                                     IList<ProcessReply> singleReplyStream,
                                                     CancellationToken   cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SubmitLargeTasksAsync)}");
    var tuple = await submitter_.CreateTasks(taskData.SessionId,
                                               taskData.TaskId,
                                               first.CreateLargeTask.InitRequest.TaskOptions,
                                               singleReplyStream.Skip(1)
                                                                .ReconstituteTaskRequest(logger_),
                                               cancellationToken)
                                  .ConfigureAwait(false);
    taskToFinalize_.Add(tuple);

    return new CreateTaskReply
           {
             Successfull = new Empty(),
           };
  }

  [PublicAPI]
  public async Task<CreateTaskReply> SubmitSmallTasksAsync(TaskData          taskData,
                                                     ProcessReply      request,
                                                     CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SubmitSmallTasksAsync)}");
    var tuple = await submitter_.CreateTasks(taskData.SessionId,
                                               taskData.TaskId,
                                               request.CreateSmallTask.TaskOptions,
                                               request.CreateSmallTask.TaskRequests.ToAsyncEnumerable()
                                                      .Select(taskRequest => new TaskRequest(taskRequest.Id,
                                                                                             taskRequest.ExpectedOutputKeys,
                                                                                             taskRequest.DataDependencies,
                                                                                             new[]
                                                                                             {
                                                                                               taskRequest.Payload.Memory,
                                                                                             }.ToAsyncEnumerable())),
                                               cancellationToken)
                                  .ConfigureAwait(false);
    taskToFinalize_.Add(tuple);

    return new CreateTaskReply
           {
             Successfull = new Empty(),
           };
  }

  [PublicAPI]
  public async Task StoreResultAsync(IObjectStorage      resultStorage,
                                     ProcessReply        first,
                                     IList<ProcessReply> singleReplyStream,
                                     string              sessionId,
                                     string              ownerTaskId,
                                     CancellationToken   cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(StoreResultAsync)}");
    await resultStorage.AddOrUpdateAsync(first.Result.Init.Key,
                                         singleReplyStream.Skip(1)
                                                          .Select(reply => reply.Result.Data.Data.Memory)
                                                          .ToAsyncEnumerable(),
                                         cancellationToken)
                       .ConfigureAwait(false);
    await resultTable_.SetResult(sessionId,
                                 ownerTaskId,
                                 first.Result.Init.Key,
                                 cancellationToken)
                      .ConfigureAwait(false);
  }

  public void Dispose()
  {
    GC.SuppressFinalize(this);
  }
}
