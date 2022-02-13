// This file is part of the ArmoniK project
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

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Submitter = ArmoniK.Core.Common.gRPC.Services.Submitter;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Compute.PollingAgent;

public class RequestProcessor
{
  private static readonly ActivitySource ActivitySource = new($"{typeof(RequestProcessor).FullName}");

  private readonly WorkerClientProvider      workerClientProvider_;
  private readonly IObjectStorageFactory     objectStorageFactory_;
  private readonly ILogger<RequestProcessor> logger_;
  private readonly IObjectStorage            resourcesStorage_;
  private readonly Submitter                 submitter_;

  public RequestProcessor(
    WorkerClientProvider      workerClientProvider,
    IObjectStorageFactory     objectStorageFactory,
    ILogger<RequestProcessor> logger,
    Submitter                 submitter)
  {
    workerClientProvider_ = workerClientProvider;
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    submitter_            = submitter;
    resourcesStorage_     = objectStorageFactory.CreateResourcesStorage();
  }

  public async Task<List<Task>> ProcessAsync(IQueueMessageHandler messageHandler,
                                             TaskData taskData,
                                             IDispatch dispatch,
                                             Queue<ProcessRequest.Types.ComputeRequest> computeRequests,
                                             CancellationToken cancellationToken)
  {
    try
    {
      return await ProcessInternalsAsync(taskData,
                                         dispatch,
                                         computeRequests,
                                         cancellationToken);
    }
    catch (Exception e)
    {
      await submitter_.CancelDispatchSessionAsync(taskData.SessionId,
                                                  dispatch.Id, cancellationToken);
      Console.WriteLine(e);

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
        Console.WriteLine(e);
        return false;
      }
    }
  }


  public async Task<List<Task>> ProcessInternalsAsync(TaskData                         taskData,
                                                      IDispatch                         dispatch,
                                             Queue<ProcessRequest.Types.ComputeRequest> computeRequests,
                                             CancellationToken                          cancellationToken)
  {
    using var activity = ActivitySource.StartActivity($"{nameof(ProcessAsync)}");

    var workerClient = await workerClientProvider_.GetAsync();

    logger_.LogDebug("Set task status to Processing");
    var updateTask = submitter_.UpdateTaskStatusAsync(taskData.TaskId,
                                                      TaskStatus.Processing,
                                                      cancellationToken);

    using var stream = workerClient.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration.ToTimeSpan(),
                                            cancellationToken: cancellationToken);

    var resultStorage = objectStorageFactory_.CreateResultStorage(taskData.SessionId);

    stream.RequestStream.WriteOptions = new(WriteFlags.NoCompress);
    {
      using var activity2 = ActivitySource.StartActivity($"{nameof(ProcessAsync)}.SendComputeRequests");
      // send the compute requests
      while (computeRequests.TryDequeue(out var computeRequest))
      {
        await stream.RequestStream.WriteAsync(new()
                                              {
                                                Compute = computeRequest,
                                              });
      }
    }

    var output = new List<Task>()
                 {
                   updateTask,
                 };

    var isComplete = false;


    // process incoming messages
    // TODO : To reduce memory consumption, do not generate subStream. Implement a state machine instead.
    await foreach (var singleReplyStream in stream.ResponseStream.Separate(cancellationToken))
    {
      var first = singleReplyStream.First();
      if (isComplete)
      {
        throw new InvalidOperationException("Unexpected message from the worker after sending the task output");
      }

      switch (first.TypeCase)
      {
        case ProcessReply.TypeOneofCase.None:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply),
                                                $"received a {nameof(ProcessReply.TypeOneofCase.None)} reply type.");
        case ProcessReply.TypeOneofCase.Output:
          output.Add(submitter_.FinalizeDispatch(taskData.TaskId,
                                                 dispatch,
                                                 first.Output,
                                                 cancellationToken));
          await stream.RequestStream.CompleteAsync();
          isComplete = true;
          break;
        case ProcessReply.TypeOneofCase.Result:
          output.Add(StoreResultAsync(resultStorage,
                                      first,
                                      singleReplyStream,
                                      cancellationToken));
          break;
        case ProcessReply.TypeOneofCase.CreateSmallTask:
          await SubmitSmallTasksAsync(taskData,
                                      dispatch.Id,
                                      first,
                                      cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.CreateLargeTask:
          await SubmitLargeTasksAsync(taskData,
                                      dispatch.Id,
                                      first,
                                      singleReplyStream,
                                      cancellationToken);
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

    stream.GetStatus().ThrowIfError();

    if(!isComplete)
      throw new InvalidOperationException("Unexpected end of stream from the worker");

    return output;

  }


  private async Task ProvideResourcesAsync(IAsyncStreamWriter<ProcessRequest> requestStream, ProcessReply processReply, CancellationToken cancellationToken)
  {
    using var activity = ActivitySource.StartActivity($"{nameof(ProvideResourcesAsync)}");
    var bytes = resourcesStorage_.TryGetValuesAsync(processReply.Resource.Key,
                                                    cancellationToken);

    await foreach (var dataReply in bytes.ToDataReply(processReply.RequestId,
                                                      processReply.Resource.Key,
                                                      cancellationToken)
                                         .WithCancellation(cancellationToken))
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
    using var activity = ActivitySource.StartActivity($"{nameof(ProvideDirectDataAsync)}");
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
    using var activity = ActivitySource.StartActivity($"{nameof(ProvideCommonDataAsync)}");
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
  public Task SubmitLargeTasksAsync(TaskData                      taskData,
                                    string                         dispatchId,
                                    ProcessReply                   first,
                                    IList<ProcessReply> singleReplyStream,
                                    CancellationToken              cancellationToken)
  {
    using var activity = ActivitySource.StartActivity($"{nameof(SubmitLargeTasksAsync)}");
    return submitter_.CreateTasks(taskData.SessionId,
                                  taskData.TaskId,
                                  dispatchId,
                                  first.CreateLargeTask.InitRequest.TaskOptions,
                                  singleReplyStream.Skip(1)
                                                   .ReconstituteTaskRequest(),
                                  cancellationToken);
  }

  [PublicAPI]
  public Task SubmitSmallTasksAsync(TaskData         taskData,
                                    string            dispatchId,
                                    ProcessReply      request,
                                    CancellationToken cancellationToken)
  {
    using var activity = ActivitySource.StartActivity($"{nameof(SubmitSmallTasksAsync)}");
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
  public Task StoreResultAsync(IObjectStorage resultStorage, ProcessReply first, IList<ProcessReply> singleReplyStream, CancellationToken cancellationToken)
  {
    using var activity = ActivitySource.StartActivity($"{nameof(StoreResultAsync)}");
    return resultStorage.AddOrUpdateAsync(first.Result.Init.Key,
                                          singleReplyStream.Skip(1).Select(reply => reply.Result.Data.Data.Memory).ToAsyncEnumerable(),
                                          cancellationToken);
  }

}
