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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Submitter = ArmoniK.Core.Common.gRPC.Services.Submitter;

namespace ArmoniK.Core.Compute.PollingAgent;

public class RequestProcessor
{
  private readonly Api.gRPC.V1.Worker.WorkerClient            workerClient_;
  private readonly ILogger<RequestProcessor>                  logger_;
  private readonly IObjectStorage                             resultStorage_;
  private readonly IObjectStorage                             payloadStorage_;
  private readonly IObjectStorage                             resourcesStorage_;
  private readonly ITaskData                                  taskData_;
  private readonly Queue<ProcessRequest.Types.ComputeRequest> computeRequests_ = new();
  private readonly Submitter                                  submitter_;

  public RequestProcessor(ITaskData                       taskData, 
                          Api.gRPC.V1.Worker.WorkerClient workerClient, 
                          IObjectStorageFactory           objectStorageFactory, 
                          ILogger<RequestProcessor>       logger,
                          Submitter                       submitter)
  {
    workerClient_   = workerClient;
    taskData_       = taskData;
    logger_         = logger;
    submitter_ = submitter;


    resultStorage_    = objectStorageFactory.CreateResultStorage(taskData_.SessionId);
    payloadStorage_   = objectStorageFactory.CreatePayloadStorage(taskData_.SessionId);
    resourcesStorage_ = objectStorageFactory.CreateResourcesStorage();
  }

  public async Task PrefetchTask(CancellationToken cancellationToken)
  {
    List<ByteString> payloadChunks;

    if (taskData_.HasPayload)
    {
      payloadChunks = new()
                      {
                        UnsafeByteOperations.UnsafeWrap(taskData_.Payload),
                      };
    }
    else
    {
      payloadChunks = await payloadStorage_.TryGetValuesAsync(taskData_.TaskId,
                                                        cancellationToken)
                                     .Select(bytes => UnsafeByteOperations.UnsafeWrap(bytes))
                                     .ToListAsync(cancellationToken);
    }


    computeRequests_.Enqueue(new()
                             {
                               InitRequest = new()
                                             {
                                               TaskId    = taskData_.TaskId,
                                               SessionId = taskData_.SessionId,
                                               TaskOptions =
                                               {
                                                 taskData_.Options.Options,
                                               },
                                               Payload = new()
                                                         {
                                                           DataComplete = payloadChunks.Count == 1,
                                                           Data         = payloadChunks[0],
                                                         },
                                             },
                             });


    if (payloadChunks.Count > 1)
    {

      for (var i = 1; i < payloadChunks.Count - 1; i++)
      {
        computeRequests_.Enqueue(new()
                                 {
                                   Payload = new()
                                          {
                                            Data         = payloadChunks[i],
                                            DataComplete = false,
                                          },
                                 });
      }

      computeRequests_.Enqueue(new()
                               {
                                 Payload = new()
                                        {
                                          Data         = payloadChunks[^1],
                                          DataComplete = true,
                                        },
                               });
    }

    foreach (var dataDependency in taskData_.DataDependencies)
    {
      var dependencyChunks = await resultStorage_.TryGetValuesAsync(dataDependency,
                                                                    cancellationToken)
                                                 .Select(bytes => UnsafeByteOperations.UnsafeWrap(bytes))
                                                 .ToListAsync(cancellationToken);


      computeRequests_.Enqueue(new()
                               {
                                 InitData = new()
                                            {
                                              Key = dataDependency,
                                              DataChunk = new()
                                                          {
                                                            Data         = dependencyChunks[0],
                                                            DataComplete = dependencyChunks.Count == 1,
                                                          },
                                            },
                               });

      if (dependencyChunks.Count > 1)
      {
        for (var i = 1; i < dependencyChunks.Count - 1; i++)
        {
          computeRequests_.Enqueue(new()
                                   {
                                     Data = new()
                                            {
                                              Data         = dependencyChunks[i],
                                              DataComplete = false,
                                            },
                                   });
        }

        computeRequests_.Enqueue(new()
                                 {
                                   Data = new()
                                          {
                                            Data         = dependencyChunks[^1],
                                            DataComplete = true,
                                          },
                                 });
      }
    }

  }

  public async Task ProcessTask(DateTime                                         deadline, 
                                CancellationToken                                cancellationToken)
  {
    using var stream = workerClient_.Process(deadline: deadline,
                                             cancellationToken: cancellationToken);

    stream.RequestStream.WriteOptions = new(WriteFlags.NoCompress);

    // send the compute requests
    while (computeRequests_.TryDequeue(out var computeRequest))
    {
      await stream.RequestStream.WriteAsync(new()
                                            {
                                              Compute = computeRequest,
                                            });
    }

    // process incoming messages
    await foreach (var (first, singleReplyStream) in stream.ResponseStream.Separate(cancellationToken))
    {

      switch (first.TypeCase)
      {
        case ProcessReply.TypeOneofCase.None:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply), $"received a {nameof(ProcessReply.TypeOneofCase.None)} reply type.");
        case ProcessReply.TypeOneofCase.Output:
          await StoreOutputAsync(first,
                                 singleReplyStream,
                                 cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.Result:
          await StoreResultAsync(first,
                                 singleReplyStream,
                                 cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.CreateSmallTask:
          await SubmitSmallTasksAsync(first,
                                      cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.CreateLargeTask:
          await SubmitLargeTasksAsync(first.RequestId,
                                      singleReplyStream,
                                      cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.Resource:
          await ProvideResourcesAsync(stream.RequestStream,
                                      first,
                                      cancellationToken);
          break;
        case ProcessReply.TypeOneofCase.CommonData:
        {
          await ProvideCommonDataAsync(stream.RequestStream,
                                       first);
          break;
        }
        case ProcessReply.TypeOneofCase.DirectData:
        {
          await ProvideDirectDataAsync(stream.RequestStream,
                                       first);
          break;
        }
        default:
          throw new ArgumentOutOfRangeException(nameof(ProcessReply));
      }
    }
  }


  private async Task ProvideResourcesAsync(IAsyncStreamWriter<ProcessRequest> requestStream, ProcessReply processReply, CancellationToken cancellationToken)
  {
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
    => streamRequestStream.WriteAsync(new()
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

  [PublicAPI]
  public Task ProvideCommonDataAsync(IAsyncStreamWriter<ProcessRequest> streamRequestStream, ProcessReply reply)
    => streamRequestStream.WriteAsync(new()
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




  private Task SubmitLargeTasksAsync(string requestId, IAsyncEnumerable<ProcessReply> singleReplyStream, CancellationToken cancellationToken)
    => submitter_.CreateLargeTasks(singleReplyStream.Select(reply =>
                                                            {
                                                              switch (reply.CreateLargeTask.TypeCase)
                                                              {
                                                                case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitRequest:
                                                                  return new CreateLargeTaskRequest()
                                                                         {
                                                                           InitRequest = new()
                                                                                         {
                                                                                           SessionId    = taskData_.SessionId,
                                                                                           ParentTaskId = taskData_.ParentTaskId,
                                                                                           TaskOptions  = reply.CreateLargeTask.InitRequest.TaskOptions,
                                                                                         },
                                                                         };
                                                                case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitTask:
                                                                  return new()
                                                                         {
                                                                           InitTask = reply.CreateLargeTask.InitTask,
                                                                         };
                                                                case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.TaskPayload:
                                                                  return new()
                                                                         {
                                                                           TaskPayload = reply.CreateLargeTask.TaskPayload,
                                                                         };
                                                                default:
                                                                  throw new ArgumentOutOfRangeException();
                                                              }
                                                            }), cancellationToken);

  private Task SubmitSmallTasksAsync(ProcessReply request, CancellationToken cancellationToken)
    => submitter_.CreateSmallTasks(new()
                                   {
                                     SessionId    = taskData_.SessionId,
                                     ParentTaskId = taskData_.ParentTaskId,
                                     TaskOptions  = request.CreateSmallTask.TaskOptions,
                                     TaskRequests =
                                     {
                                       request.CreateSmallTask.TaskRequests,
                                     },
                                   },
                                   cancellationToken);

  private Task StoreResultAsync(ProcessReply first, IAsyncEnumerable<ProcessReply> singleReplyStream, CancellationToken cancellationToken)
    => resultStorage_.AddOrUpdateAsync(first.Result.Init.Key,
                                       singleReplyStream.Select(reply => reply.Result.TypeCase == ProcessReply.Types.Result.TypeOneofCase.Init
                                                                           ? reply.Result.Init.ResultChunk.Data.Memory
                                                                           : reply.Result.Data.Data.Memory),
                                       cancellationToken);

  private Task StoreOutputAsync(ProcessReply first, IAsyncEnumerable<ProcessReply> singleReplyStream, CancellationToken cancellationToken)
    => payloadStorage_.AddOrUpdateAsync(first.Result.Init.Key,
                                       singleReplyStream.Select(reply => reply.Result.TypeCase == ProcessReply.Types.Result.TypeOneofCase.Init
                                                                           ? reply.Result.Init.ResultChunk.Data.Memory
                                                                           : reply.Result.Data.Data.Memory),
                                       cancellationToken);

}
