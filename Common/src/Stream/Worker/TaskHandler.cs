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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Stream.Worker
{
  public class TaskHandler : ITaskHandler
  {
    public static async Task<TaskHandler> Create(IAsyncStreamReader<ProcessRequest> requestStream,
                                                 IServerStreamWriter<ProcessReply> responseStream,
                                                 Configuration configuration,
                                                 ILogger<TaskHandler> logger,
                                                 CancellationToken cancellationToken)
    {
      var output = new TaskHandler(requestStream,
                                   responseStream,
                                   configuration,
                                   cancellationToken,
                                   logger);
      await output.Init();
      return output;
    }

    private readonly IAsyncStreamReader<ProcessRequest> requestStream_;
    private readonly IServerStreamWriter<ProcessReply>  responseStream_;
    private readonly CancellationToken                  cancellationToken_;

    private TaskHandler(IAsyncStreamReader<ProcessRequest> requestStream,
                        IServerStreamWriter<ProcessReply>  responseStream,
                        Configuration                      configuration,
                        CancellationToken                  cancellationToken,
                        ILogger<TaskHandler>               logger)
    {
      requestStream_     = requestStream;
      responseStream_    = responseStream;
      cancellationToken_ = cancellationToken;
      Configuration      = configuration;
      logger_            = logger;
    }

    protected async Task Init()
    {
      crsm_ = new ComputeRequestStateMachine(logger_);
      if (!await requestStream_.MoveNext())
        throw new InvalidOperationException("Request stream ended unexpectedly.");

      if (requestStream_.Current.TypeCase != ProcessRequest.TypeOneofCase.Compute ||
          requestStream_.Current.Compute.TypeCase != ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitRequest)
        throw new InvalidOperationException("Expected a Compute request type with InitRequest to start the stream.");

      crsm_.InitRequest();
      var initRequest = requestStream_.Current.Compute.InitRequest;
      sessionId_       = initRequest.SessionId;
      taskId_          = initRequest.TaskId;
      taskOptions_     = initRequest.TaskOptions;
      expectedResults_ = initRequest.ExpectedOutputKeys;

      if (initRequest.Payload.DataComplete)
        payload_ = initRequest.Payload.Data.ToByteArray();
      else
      {
        var chunks    = new List<ByteString>();
        var dataChunk = initRequest.Payload;

        chunks.Add(dataChunk.Data);

        while (!dataChunk.DataComplete)
        {
          if (!await requestStream_.MoveNext())
            throw new InvalidOperationException("Request stream ended unexpectedly.");

          if (requestStream_.Current.TypeCase != ProcessRequest.TypeOneofCase.Compute ||
              requestStream_.Current.Compute.TypeCase != ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload)
            throw new InvalidOperationException("Expected a Compute request type with Payload to continue the stream.");

          dataChunk = requestStream_.Current.Compute.Payload;

          chunks.Add(dataChunk.Data);
          crsm_.AddPayloadChunk();
        }


        var size = chunks.Sum(s => s.Length);

        var payload = new byte[size];

        var start = 0;

        foreach (var chunk in chunks)
        {
          chunk.CopyTo(payload,
                       start);
          start += chunk.Length;
        }

        payload_ = payload;
      }
      crsm_.CompletePayload();

      var dataDependencies = new Dictionary<string, byte[]>();

      ProcessRequest.Types.ComputeRequest.Types.InitData initData;
      do
      {
        if (!await requestStream_.MoveNext())
          throw new InvalidOperationException("Request stream ended unexpectedly.");


        if (requestStream_.Current.TypeCase != ProcessRequest.TypeOneofCase.Compute ||
            requestStream_.Current.Compute.TypeCase != ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData)
          throw new InvalidOperationException("Expected a Compute request type with InitData to continue the stream.");

        initData = requestStream_.Current.Compute.InitData;
        if (!string.IsNullOrEmpty(initData.Key))
        {
          crsm_.InitDataDependency();
          var chunks    = new List<ByteString>();

          while(true)
          {
            if (!await requestStream_.MoveNext())
              throw new InvalidOperationException("Request stream ended unexpectedly.");

            if (requestStream_.Current.TypeCase != ProcessRequest.TypeOneofCase.Compute ||
                requestStream_.Current.Compute.TypeCase != ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data)
              throw new InvalidOperationException("Expected a Compute request type with Data to continue the stream.");

            var dataChunk = requestStream_.Current.Compute.Data;

            if(dataChunk.TypeCase == DataChunk.TypeOneofCase.Data)
            {
              chunks.Add(dataChunk.Data);
              crsm_.AddDataDependencyChunk();
            }

            if(dataChunk.TypeCase == DataChunk.TypeOneofCase.None)
              throw new InvalidOperationException("Expected a Compute request type with a DataChunk Payload to continue the stream.");
            if (dataChunk.TypeCase == DataChunk.TypeOneofCase.DataComplete)
              break;
          }

          var size = chunks.Sum(s => s.Length);

          var data = new byte[size];

          var start = 0;

          foreach (var chunk in chunks)
          {
            chunk.CopyTo(data,
                         start);
            start += chunk.Length;
          }

          dataDependencies[initData.Key] = data;
          crsm_.CompleteDataDependency();
        }
      } while (!string.IsNullOrEmpty(initData.Key));

      crsm_.CompleteRequest();
      dataDependencies_ = dataDependencies;
      isInitialized_   = true;
    }

    private bool                   isInitialized_;
    private Exception TaskHandlerException(string argumentName)
      => isInitialized_? new ($"Error in initalization: {argumentName} is null") : new InvalidOperationException("");

    /// <inheritdoc />
    public string SessionId => sessionId_ ?? throw TaskHandlerException(nameof(SessionId));

    /// <inheritdoc />
    public string TaskId => taskId_ ?? throw TaskHandlerException(nameof(TaskId));

    /// <inheritdoc />
    public IReadOnlyDictionary<string, string> TaskOptions => taskOptions_ ?? throw TaskHandlerException(nameof(TaskOptions));

    /// <inheritdoc />
    public byte[] Payload => payload_ ?? throw TaskHandlerException(nameof(Payload));

    /// <inheritdoc />
    public IReadOnlyDictionary<string, byte[]> DataDependencies => dataDependencies_ ?? throw TaskHandlerException(nameof(DataDependencies));

    /// <inheritdoc />
    public IList<string> ExpectedResults => expectedResults_ ?? throw TaskHandlerException(nameof(ExpectedResults));

    /// <inheritdoc />
    public Configuration Configuration { get; init; }

    /// <inheritdoc />
    public async Task CreateTasksAsync(IEnumerable<TaskRequest> tasks, TaskOptions? taskOptions = null)
    {
      try
      {
        await semaphore_.WaitAsync(cancellationToken_);

        var requestId = $"R#{messageCounter_++}";

        foreach (var createLargeTaskRequest in tasks.ToRequestStream(taskOptions,
                                                                     Configuration.DataChunkMaxSize))
        {
          await responseStream_.WriteAsync(new()
                                           {
                                             RequestId       = requestId,
                                             CreateLargeTask = createLargeTaskRequest,
                                           });
        }

        if (!await requestStream_.MoveNext(cancellationToken_))
          throw new InvalidOperationException("Request stream ended unexpectedly.");


        var current = requestStream_.Current;

        if (current.TypeCase != ProcessRequest.TypeOneofCase.CreateTask)
          throw new InvalidOperationException("Expected a CreateTask answer.");

        if (current.CreateTask.ReplyId != requestId)
          throw new InvalidOperationException($"Expected reply for request {requestId}");

        var reply = current.CreateTask.Reply;
        if (reply.DataCase == CreateTaskReply.DataOneofCase.NonSuccessfullIds)
          throw new AggregateException(reply
                                      .NonSuccessfullIds
                                      .Ids
                                      .Select(s => new InvalidOperationException($"Could not create task it id={s}")));
      }
      finally
      {
        semaphore_.Release();
      }
    }

    /// <inheritdoc />
    public Task<byte[]> RequestResource(string key)
      => throw new NotImplementedException();

    /// <inheritdoc />
    public Task<byte[]> RequestCommonData(string key)
      => throw new NotImplementedException();

    /// <inheritdoc />
    public Task<byte[]> RequestDirectData(string key)
      => throw new NotImplementedException();

    /// <inheritdoc />
    public async Task SendResult(string key, byte[] data)
    {
      try
      {
        await semaphore_.WaitAsync(cancellationToken_);
        var requestId = $"R#{messageCounter_++}";

        var reply = new ProcessReply()
                    {
                      Result = new()
                               {
                                 Init = new()
                                        {
                                          Key = key,
                                        }
                               },
                      RequestId = requestId,
                    };

        await responseStream_.WriteAsync(reply);
        var start = 0;

        while (start < data.Length)
        {
          var chunkSize = Math.Min(Configuration.DataChunkMaxSize,
                                   data.Length - start);

          reply = new()
                  {
                    Result = new()
                             {
                               Data = new()
                                      {

                                        Data = ByteString.CopyFrom(data.AsMemory().Span.Slice(start,
                                                                                              chunkSize)),
                                      },
                             },
                    RequestId = requestId,
                  };

          await responseStream_.WriteAsync(reply);

          start += chunkSize;
        }

        reply = new()
        {
          Result = new()
          {
            Data = new()
            {

              DataComplete = true,
            },
          },
          RequestId = requestId,
        };

        await responseStream_.WriteAsync(reply);

        reply = new()
                {
                  Result = new()
                           {
                             Init = new()
                                    {
                                      LastResult = true,
                                    },
                           },
                    RequestId = requestId,
                };

        await responseStream_.WriteAsync(reply);


      }
      finally
      {
        semaphore_.Release();
      }
    }

    private int messageCounter_;

    private readonly SemaphoreSlim                        semaphore_ = new(1);
    private readonly ILogger<TaskHandler>                 logger_;
    private          string?                              sessionId_;
    private          string?                              taskId_;
    private          IReadOnlyDictionary<string, string>? taskOptions_;
    private          byte[]?                              payload_;
    private          IReadOnlyDictionary<string, byte[]>? dataDependencies_;
    private          IList<string>?                       expectedResults_;
    private          ComputeRequestStateMachine?          crsm_;
  }
}
