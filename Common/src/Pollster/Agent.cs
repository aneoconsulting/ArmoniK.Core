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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Represents the internal processing requests received by the agent. Provides methods to process those requests
/// </summary>
public sealed class Agent : IAgent
{
  private readonly List<TaskCreationRequest>                  createdTasks_;
  private readonly ILogger                                    logger_;
  private readonly IObjectStorage                             objectStorage_;
  private readonly IPushQueueStorage                          pushQueueStorage_;
  private readonly IResultTable                               resultTable_;
  private readonly Dictionary<string, (byte[] id, long size)> sentResults_;
  private readonly SessionData                                sessionData_;
  private readonly ISubmitter                                 submitter_;
  private readonly TaskData                                   taskData_;
  private readonly ITaskTable                                 taskTable_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="Agent" />
  /// </summary>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorage">Interface class to manage tasks data</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="sessionData">OpaqueId of the session</param>
  /// <param name="taskData">OpaqueId of the task</param>
  /// <param name="folder">Shared folder between Agent and Worker</param>
  /// <param name="token">Token send to the worker to identify the running task</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public Agent(ISubmitter        submitter,
               IObjectStorage    objectStorage,
               IPushQueueStorage pushQueueStorage,
               IResultTable      resultTable,
               ITaskTable        taskTable,
               SessionData       sessionData,
               TaskData          taskData,
               string            folder,
               string            token,
               ILogger           logger)
  {
    submitter_        = submitter;
    objectStorage_    = objectStorage;
    pushQueueStorage_ = pushQueueStorage;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    logger_           = logger;
    createdTasks_     = new List<TaskCreationRequest>();
    sentResults_      = new Dictionary<string, (byte[] id, long size)>();
    sessionData_      = sessionData;
    taskData_         = taskData;
    Folder            = folder;
    Token             = token;
  }

  /// <inheritdoc />
  public string Token { get; }

  /// <inheritdoc />
  public string Folder { get; }

  /// <inheritdoc />
  public string SessionId
    => sessionData_.SessionId;

  /// <inheritdoc />
  /// <exception cref="ArmoniKException"></exception>
  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(FinalizeTaskCreation),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    logger_.LogDebug("Finalize child task creation");

    await submitter_.FinalizeTaskCreation(createdTasks_,
                                          sessionData_,
                                          taskData_.TaskId,
                                          cancellationToken)
                    .ConfigureAwait(false);

    foreach (var (result, (id, size)) in sentResults_)
    {
      await resultTable_.CompleteResult(taskData_.SessionId,
                                        result,
                                        size,
                                        id,
                                        cancellationToken)
                        .ConfigureAwait(false);
    }

    await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                  resultTable_,
                                                  pushQueueStorage_,
                                                  sessionData_,
                                                  sentResults_.Keys,
                                                  logger_,
                                                  cancellationToken)
                             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public void Dispose()
  {
  }

  /// <inheritdoc />
  public async Task<string> GetResourceData(string            token,
                                            string            resultId,
                                            CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetResourceData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    ThrowIfInvalidToken(token);


    try
    {
      var opaqueId = (await resultTable_.GetResult(resultId,
                                                   cancellationToken)
                                        .ConfigureAwait(false)).OpaqueId;

      await using var fs = new FileStream(Path.Combine(Folder,
                                                       resultId),
                                          FileMode.OpenOrCreate);
      await using var w = new BinaryWriter(fs);
      await foreach (var chunk in objectStorage_.GetValuesAsync(opaqueId,
                                                                cancellationToken)
                                                .ConfigureAwait(false))
      {
        w.Write(chunk);
      }


      return resultId;
    }
    catch (Exception ex) when (ex is ResultNotFoundException or ObjectDataNotFoundException)
    {
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "ResultId not found"),
                             "ResultId not found");
    }
  }

  /// <inheritdoc />
  public Task<string> GetCommonData(string            token,
                                    string            resultId,
                                    CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetCommonData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    ThrowIfInvalidToken(token);

    throw new NotImplementedException("Common data are not implemented yet");
  }

  /// <inheritdoc />
  public Task<string> GetDirectData(string            token,
                                    string            resultId,
                                    CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetDirectData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    ThrowIfInvalidToken(token);

    throw new NotImplementedException("Direct data are not implemented yet");
  }

  /// <inheritdoc />
  public async Task<ICollection<TaskCreationRequest>> SubmitTasks(ICollection<TaskSubmissionRequest> requests,
                                                                  TaskOptions?                       taskOptions,
                                                                  string                             sessionId,
                                                                  string                             token,
                                                                  CancellationToken                  cancellationToken)
  {
    ThrowIfInvalidToken(token);

    var options = TaskLifeCycleHelper.ValidateSession(sessionData_,
                                                      taskOptions,
                                                      taskData_.TaskId,
                                                      pushQueueStorage_.MaxPriority,
                                                      logger_,
                                                      cancellationToken);

    if (requests.Count == 0)
    {
      return new List<TaskCreationRequest>();
    }

    var createdTasks = requests.Select(creation => new TaskCreationRequest(Guid.NewGuid()
                                                                               .ToString(),
                                                                           creation.PayloadId,
                                                                           TaskOptions.Merge(creation.Options,
                                                                                             options),
                                                                           creation.ExpectedOutputKeys,
                                                                           creation.DataDependencies))
                               .ToList();

    await TaskLifeCycleHelper.CreateTasks(taskTable_,
                                          resultTable_,
                                          sessionId,
                                          taskData_.TaskId,
                                          createdTasks,
                                          logger_,
                                          cancellationToken)
                             .ConfigureAwait(false);

    createdTasks_.AddRange(createdTasks);

    return createdTasks;
  }

  /// <inheritdoc />
  public async Task<ICollection<Result>> CreateResults(string                                                                  token,
                                                       IEnumerable<(ResultCreationRequest request, ReadOnlyMemory<byte> data)> requests,
                                                       CancellationToken                                                       cancellationToken)
  {
    ThrowIfInvalidToken(token);

    var results = await requests.Select(async rc =>
                                        {
                                          var result = new Result(rc.request.SessionId,
                                                                  Guid.NewGuid()
                                                                      .ToString(),
                                                                  rc.request.Name,
                                                                  taskData_.TaskId,
                                                                  "",
                                                                  ResultStatus.Created,
                                                                  new List<string>(),
                                                                  DateTime.UtcNow,
                                                                  0,
                                                                  Array.Empty<byte>());

                                          var add = await objectStorage_.AddOrUpdateAsync(new List<ReadOnlyMemory<byte>>
                                                                                          {
                                                                                            rc.data,
                                                                                          }.ToAsyncEnumerable(),
                                                                                          cancellationToken)
                                                                        .ConfigureAwait(false);

                                          return (result, add);
                                        })
                                .WhenAll()
                                .ConfigureAwait(false);

    await resultTable_.Create(results.Select(tuple => tuple.result)
                                     .AsICollection(),
                              cancellationToken)
                      .ConfigureAwait(false);

    foreach (var result in results)
    {
      sentResults_.Add(result.result.ResultId,
                       result.add);
    }

    return results.Select(tuple => tuple.result)
                  .AsICollection();
  }

  /// <inheritdoc />
  public async Task<ICollection<string>> NotifyResultData(string              token,
                                                          ICollection<string> resultIds,
                                                          CancellationToken   cancellationToken)
  {
    ThrowIfInvalidToken(token);

    foreach (var result in resultIds)
    {
      await using var fs = new FileStream(Path.Combine(Folder,
                                                       result),
                                          FileMode.OpenOrCreate);
      using var r       = new BinaryReader(fs);
      var       channel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>();

      var addTask = objectStorage_.AddOrUpdateAsync(channel.Reader.ReadAllAsync(cancellationToken),
                                                    cancellationToken);

      int read;
      do
      {
        var buffer = new byte[PayloadConfiguration.MaxChunkSize];
        read = r.Read(buffer,
                      0,
                      PayloadConfiguration.MaxChunkSize);
        if (read > 0)
        {
          await channel.Writer.WriteAsync(buffer.AsMemory(0,
                                                          read),
                                          cancellationToken)
                       .ConfigureAwait(false);
        }
      } while (read != 0);

      channel.Writer.Complete();

      var add = await addTask.ConfigureAwait(false);
      sentResults_.Add(result,
                       add);
    }

    return resultIds;
  }

  /// <inheritdoc />
  public async Task<ICollection<Result>> CreateResultsMetaData(string                             token,
                                                               IEnumerable<ResultCreationRequest> requests,
                                                               CancellationToken                  cancellationToken)
  {
    ThrowIfInvalidToken(token);

    var results = requests.Select(rc => new Result(rc.SessionId,
                                                   Guid.NewGuid()
                                                       .ToString(),
                                                   rc.Name,
                                                   taskData_.TaskId,
                                                   "",
                                                   ResultStatus.Created,
                                                   new List<string>(),
                                                   DateTime.UtcNow,
                                                   0,
                                                   Array.Empty<byte>()))
                          .AsICollection();

    await resultTable_.Create(results,
                              cancellationToken)
                      .ConfigureAwait(false);

    return results;
  }

  private void ThrowIfInvalidToken(string token)
  {
    if (string.IsNullOrEmpty(token))
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Missing communication token"),
                             "Missing communication token");
    }

    if (token != Token)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Wrong communication token"),
                             "Wrong communication token");
    }
  }
}
