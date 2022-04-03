﻿// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Submitter = ArmoniK.Core.Common.gRPC.Services.Submitter;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

public class RequestProcessor : IInitializable
{
  private readonly ActivitySource            activitySource_;
  private readonly WorkerClientProvider      workerClientProvider_;
  private readonly IObjectStorageFactory     objectStorageFactory_;
  private readonly ILogger<RequestProcessor> logger_;
  private readonly IObjectStorage            resourcesStorage_;
  private readonly Submitter                 submitter_;
  private readonly IResultTable              resultTable_;

  public RequestProcessor(
    WorkerClientProvider      workerClientProvider,
    IObjectStorageFactory     objectStorageFactory,
    ILogger<RequestProcessor> logger,
    Submitter                 submitter,
    IResultTable              resultTable,
    ActivitySource            activitySource)
  {
    workerClientProvider_ = workerClientProvider;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    submitter_            = submitter;
    resultTable_          = resultTable;
    activitySource_       = activitySource;
    resourcesStorage_     = objectStorageFactory.CreateResourcesStorage();
  }

  public async Task<List<Task>> ProcessAsync(IQueueMessageHandler messageHandler,
                                             TaskData taskData,
                                             Dispatch dispatch,
                                             Queue<ProcessRequest.Types.ComputeRequest> computeRequests,
                                             CancellationToken cancellationToken)
  {
    try
    {
      var result =  await ProcessInternalsAsync(taskData,
                                         dispatch,
                                         computeRequests,
                                         cancellationToken);

      messageHandler.Status = QueueMessageStatus.Processed;
      return result;
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Error while processing request");
      await submitter_.CancelDispatchSessionAsync(taskData.SessionId,
                                                  dispatch.Id, cancellationToken);

      if (!await HandleExceptionAsync(e,
                                      taskData,
                                      messageHandler,
                                      cancellationToken))
      {
        throw;
      }

      throw new ArmoniKException("An error occurred while executing. Error has been managed.");
    }
  }

  
  [PublicAPI]
  private async Task<bool> HandleExceptionAsync(Exception e, TaskData taskData, IQueueMessageHandler messageHandler, CancellationToken cancellationToken)
  {
    switch (e)
    {
      case System.TimeoutException:
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.TaskId,
                         taskData.SessionId);
        messageHandler.Status = QueueMessageStatus.Failed;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Timeout,
                                               CancellationToken.None);
        return true;
      }
      case System.Threading.Tasks.TaskCanceledException:
      {
        var details = string.Empty;

        if (messageHandler.CancellationToken.IsCancellationRequested) details += "Message was cancelled. ";
        if (cancellationToken.IsCancellationRequested) details                += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.TaskId,
                         taskData.SessionId,
                         details);
        messageHandler.Status = QueueMessageStatus.Cancelled;
        await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                               TaskStatus.Canceling,
                                               CancellationToken.None);
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
                                               CancellationToken.None);
        return true;
      }
      case AggregateException ae:
      {
        foreach (var ie in ae.InnerExceptions)
          // If the exception was not handled, lazily allocate a list of unhandled
          // exceptions (to be rethrown later) and add it.
          if (!await HandleExceptionAsync(ie,
                                          taskData,
                                          messageHandler,
                                          cancellationToken))
            return false;

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
                                               CancellationToken.None);
        return false;
      }
    }
  }


  public async Task<List<Task>> ProcessInternalsAsync(TaskData                         taskData,
                                                      Dispatch                         dispatch,
                                             Queue<ProcessRequest.Types.ComputeRequest> computeRequests,
                                             CancellationToken                          cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}");
    activity?.SetBaggage("SessionId",
                         taskData.SessionId);
    activity?.SetBaggage("TaskId",
                         taskData.TaskId);
    activity?.SetBaggage("DispatchId",
                         taskData.DispatchId);

    var workerClient = await workerClientProvider_.GetAsync();

    logger_.LogDebug("Set task status to Processing");
    await submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                           TaskStatus.Processing,
                                           cancellationToken);

    using var stream = workerClient.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                            cancellationToken: cancellationToken);

    var resultStorage = objectStorageFactory_.CreateResultStorage(taskData.SessionId);

    stream.RequestStream.WriteOptions = new(WriteFlags.NoCompress);
    {
      using var activity2 = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.SendComputeRequests");
      // send the compute requests
      while (computeRequests.TryDequeue(out var computeRequest))
      {
        activity?.AddEvent(new ActivityEvent(computeRequest.TypeCase.ToString()));
        await stream.RequestStream.WriteAsync(new()
                                              {
                                                Compute = computeRequest,
                                              });
      }
    }

    var output = new List<Task>();

    var isComplete = false;


    activity?.AddEvent(new ActivityEvent("Processing ResponseStream"));
    // process incoming messages
    // TODO : To reduce memory consumption, do not generate subStream. Implement a state machine instead.
    await foreach (var singleReplyStream in stream.ResponseStream.Separate(logger_, cancellationToken))
    {
      var       first     = Enumerable.First<ProcessReply>(singleReplyStream);
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
          await output.WhenAll();
          output.Clear();
          output.Add(submitter_.FinalizeDispatch(taskData.TaskId,
                                                 dispatch,
                                                 cancellationToken));
          output.Add(submitter_.CompleteTaskAsync(taskData.TaskId,
                                                  first.Output,
                                                  cancellationToken));
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
                                                  dispatch.Id,
                                                  first,
                                                  cancellationToken);
          await stream.RequestStream.WriteAsync(new()
          {
            CreateTask = new()
            {
              Reply   = replySmallTasksAsync,
              ReplyId = first.RequestId,
            },
          });
          break;
        case ProcessReply.TypeOneofCase.CreateLargeTask:
          var replyLargeTasksAsync = await SubmitLargeTasksAsync(taskData,
                                                  dispatch.Id,
                                                  first,
                                                  singleReplyStream,
                                                  cancellationToken);
          await stream.RequestStream.WriteAsync(new()
          {
            CreateTask = new()
            {
              Reply   = replyLargeTasksAsync,
              ReplyId = first.RequestId,
            },
          });
          break;
        case ProcessReply.TypeOneofCase.Resource:
          await ProvideResourcesAsync(stream.RequestStream,
                                      first,
                                      cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.CommonData:
          await ProvideCommonDataAsync(stream.RequestStream,
                                       first);
          break;
        case ProcessReply.TypeOneofCase.DirectData:
          await ProvideDirectDataAsync(stream.RequestStream,
                                       first);
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply),
                                                "Unexpected message type in the stream. Wrong proto definition has been used ?");
      }
    }


    using (var _ = activitySource_.StartActivity($"{nameof(ProcessInternalsAsync)}.CloseStreams"))
    {
      await stream.RequestStream.CompleteAsync();
      await stream.ResponseStream.MoveNext();
    }

    if (!isComplete)
      throw new InvalidOperationException("Unexpected end of stream from the worker");


    return output;
  }


  private async Task ProvideResourcesAsync(IAsyncStreamWriter<ProcessRequest> requestStream, ProcessReply processReply, CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideResourcesAsync)}");
    var bytes = resourcesStorage_.GetValuesAsync(processReply.Resource.Key,
                                                    cancellationToken);

    await foreach (var dataReply in TaskAsyncEnumerableExtensions.WithCancellation<ProcessRequest.Types.DataReply>(bytes.ToDataReply(processReply.RequestId,
                                                                                                        processReply.Resource.Key,
                                                                                                        cancellationToken),
                                                                                      cancellationToken))
    {
      await requestStream.WriteAsync(new()
                                     {
                                       Resource = dataReply,
                                     });
    }
  }

  [PublicAPI]
  public Task ProvideDirectDataAsync(IAsyncStreamWriter<ProcessRequest> streamRequestStream, ProcessReply reply)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideDirectDataAsync)}");
    return streamRequestStream.WriteAsync(new()
                                          {
                                            DirectData = new()
                                                         {
                                                           ReplyId = reply.RequestId,
                                                           Init = new()
                                                                  {
                                                                    Key   = reply.CommonData.Key,
                                                                    Error = "Common data are not supported yet",
                                                                  },
                                                         },
                                          });
  }

  [PublicAPI]
  public Task ProvideCommonDataAsync(IAsyncStreamWriter<ProcessRequest> streamRequestStream, ProcessReply reply)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ProvideCommonDataAsync)}");
    return streamRequestStream.WriteAsync(new()
                                          {
                                            CommonData = new()
                                                         {
                                                           ReplyId = reply.RequestId,
                                                           Init = new()
                                                                  {
                                                                    Key   = reply.CommonData.Key,
                                                                    Error = "Common data are not supported yet",
                                                                  },
                                                         },
                                          });
  }


  [PublicAPI]
  public Task<CreateTaskReply> SubmitLargeTasksAsync(TaskData                      taskData,
                                                         string                         dispatchId,
                                                         ProcessReply                   first,
                                                         IList<ProcessReply> singleReplyStream,
                                                         CancellationToken              cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SubmitLargeTasksAsync)}");
    return submitter_.CreateTasks(taskData.SessionId,
                                  taskData.TaskId,
                                  dispatchId,
                                  first.CreateLargeTask.InitRequest.TaskOptions,
                                  singleReplyStream.Skip(1)
                                                   .ReconstituteTaskRequest(logger_),
                                  cancellationToken);
  }

  [PublicAPI]
  public Task<CreateTaskReply> SubmitSmallTasksAsync(TaskData         taskData,
                                                     string            dispatchId,
                                                     ProcessReply      request,
                                                     CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(SubmitSmallTasksAsync)}");
    return submitter_.CreateTasks(taskData.SessionId,
                                  taskData.ParentTaskId,
                                  dispatchId,
                                  request.CreateSmallTask.TaskOptions,
                                  request.CreateSmallTask.TaskRequests
                                         .ToAsyncEnumerable()
                                         .Select(taskRequest => new Common.gRPC.Services.TaskRequest(taskRequest.Id,
                                                                                                     taskRequest.ExpectedOutputKeys,
                                                                                                     taskRequest.DataDependencies,
                                                                                                     new[] { taskRequest.Payload.Memory }.ToAsyncEnumerable())),
                                  cancellationToken);
  }

  [PublicAPI]
  public async Task StoreResultAsync(IObjectStorage resultStorage,
                                     ProcessReply first,
                                     IList<ProcessReply> singleReplyStream,
                                     string sessionId,
                                     string ownerTaskId,
                                     CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(StoreResultAsync)}");
    await resultStorage.AddOrUpdateAsync(first.Result.Init.Key,
                                          singleReplyStream.Skip(1).Select(reply => reply.Result.Data.Data.Memory).ToAsyncEnumerable(),
                                          cancellationToken);
    await resultTable_.SetResult(sessionId,
                                 ownerTaskId,
                                 first.Result.Init.Key,
                                 cancellationToken);
  }

  private bool isInitialized_ = false;

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var resultTable  = resultTable_.Init(cancellationToken);
      var workerClientProvider = workerClientProvider_.Init(cancellationToken);
      await resultTable;
      await workerClientProvider;
      isInitialized_ = true;
    }
  }
}
