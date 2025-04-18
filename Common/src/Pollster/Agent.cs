// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
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
  private readonly ConcurrentBag<ICollection<Result>>                                 createdResults_ = new();
  private readonly ConcurrentBag<ICollection<TaskCreationRequest>>                    createdTasks_   = new();
  private readonly ILogger                                                            logger_;
  private readonly ConcurrentBag<ICollection<string>>                                 notifiedResults_ = new();
  private readonly IObjectStorage                                                     objectStorage_;
  private readonly IPushQueueStorage                                                  pushQueueStorage_;
  private readonly ConcurrentBag<ICollection<(string id, ReadOnlyMemory<byte> data)>> resultsData_ = new();
  private readonly IResultTable                                                       resultTable_;
  private readonly SessionData                                                        sessionData_;
  private readonly ISubmitter                                                         submitter_;
  private readonly TaskData                                                           taskData_;
  private readonly ITaskTable                                                         taskTable_;

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
  public async Task CreateResultsAndSubmitChildTasksAsync(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(CreateResultsAndSubmitChildTasksAsync),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    logger_.LogDebug("Create and populate results and submit child tasks");

    var createdResults = createdResults_.SelectMany(x => x)
                                        .AsICollection();
    var createdTasks = createdTasks_.SelectMany(x => x)
                                    .AsICollection();

    await resultTable_.Create(createdResults,
                              cancellationToken)
                      .ConfigureAwait(false);

    var resultsToComplete = await StoreDataAsync(cancellationToken)
                              .ConfigureAwait(false);

    await resultTable_.CompleteManyResults(resultsToComplete.ViewSelect(pair => (pair.Key, pair.Value.size, pair.Value.id)),
                                           taskData_.TaskId,
                                           cancellationToken)
                      .ConfigureAwait(false);

    await TaskLifeCycleHelper.CreateTasks(taskTable_,
                                          resultTable_,
                                          sessionData_.SessionId,
                                          taskData_.TaskId,
                                          createdTasks,
                                          logger_,
                                          cancellationToken)
                             .ConfigureAwait(false);

    await submitter_.FinalizeTaskCreation(createdTasks,
                                          sessionData_,
                                          taskData_.TaskId,
                                          cancellationToken)
                    .ConfigureAwait(false);

    await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                  resultTable_,
                                                  pushQueueStorage_,
                                                  sessionData_,
                                                  resultsToComplete.Keys,
                                                  logger_,
                                                  cancellationToken)
                             .ConfigureAwait(false);


    var subtaskOutputIds = createdTasks.SelectMany(task => task.ExpectedOutputKeys)
                                       .ToHashSet();

    var completedResultIds = resultsToComplete.Keys.ToHashSet();

    var missingOutputIds = taskData_.ExpectedOutputIds.Where(resultId => !subtaskOutputIds.Contains(resultId) && !completedResultIds.Contains(resultId))
                                    .AsICollection();

    var orphanIds = createdResults.Select(result => result.ResultId)
                                  .Where(resultId => !subtaskOutputIds.Contains(resultId) && !completedResultIds.Contains(resultId))
                                  .AsICollection();


    if (completedResultIds.Count == 0)
    {
      logger_.LogWarning("No result was completed");
    }

    if (missingOutputIds.Count > 0)
    {
      logger_.LogError("Expected output results were neither completed nor delegated to subtask: {@ResultIds}",
                       missingOutputIds);
    }

    if (orphanIds.Count > 0)
    {
      logger_.LogError("New results were neither completed nor delegated to subtask: {@ResultIds}",
                       orphanIds);
    }
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
  public Task<ICollection<TaskCreationRequest>> SubmitTasks(ICollection<TaskSubmissionRequest> requests,
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
      return Task.FromResult(Array.Empty<TaskCreationRequest>()
                                  .AsICollection());
    }

    var createdTasks = requests.Select(creation => new TaskCreationRequest(Guid.NewGuid()
                                                                               .ToString(),
                                                                           creation.PayloadId,
                                                                           TaskOptions.Merge(creation.Options,
                                                                                             options),
                                                                           creation.ExpectedOutputKeys,
                                                                           creation.DataDependencies))
                               .AsICollection();

    createdTasks_.Add(createdTasks);
    return Task.FromResult(createdTasks);
  }

  /// <inheritdoc />
  public Task<ICollection<Result>> CreateResults(string                                                                  token,
                                                 IEnumerable<(ResultCreationRequest request, ReadOnlyMemory<byte> data)> requests,
                                                 CancellationToken                                                       cancellationToken)
  {
    ThrowIfInvalidToken(token);

    var now = DateTime.UtcNow;

    var results = requests.Select(tuple => (new Result(tuple.request.SessionId,
                                                       Guid.NewGuid()
                                                           .ToString(),
                                                       tuple.request.Name,
                                                       taskData_.TaskId,
                                                       "",
                                                       "",
                                                       ResultStatus.Created,
                                                       new List<string>(),
                                                       now,
                                                       now,
                                                       0,
                                                       Array.Empty<byte>(),
                                                       false), tuple.data))
                          .AsICollection();


    resultsData_.Add(results.ViewSelect(tuple => (tuple.Item1.ResultId, tuple.data)));
    createdResults_.Add(results.ViewSelect(tuple => tuple.Item1));
    return Task.FromResult<ICollection<Result>>(results.ViewSelect(tuple => tuple.Item1));
  }

  /// <inheritdoc />
  public Task<ICollection<string>> NotifyResultData(string              token,
                                                    ICollection<string> resultIds,
                                                    CancellationToken   cancellationToken)
  {
    ThrowIfInvalidToken(token);
    notifiedResults_.Add(resultIds);
    return Task.FromResult(resultIds);
  }

  /// <inheritdoc />
  public Task<ICollection<Result>> CreateResultsMetaData(string                             token,
                                                         IEnumerable<ResultCreationRequest> requests,
                                                         CancellationToken                  cancellationToken)
  {
    ThrowIfInvalidToken(token);

    var results = requests.Select(request => new Result(request.SessionId,
                                                        Guid.NewGuid()
                                                            .ToString(),
                                                        request.Name,
                                                        taskData_.TaskId,
                                                        "",
                                                        "",
                                                        ResultStatus.Created,
                                                        new List<string>(),
                                                        DateTime.UtcNow,
                                                        null,
                                                        0,
                                                        Array.Empty<byte>(),
                                                        false))
                          .AsICollection();

    createdResults_.Add(results);
    return Task.FromResult(results);
  }

  private async Task<Dictionary<string, (byte[] id, long size)>> StoreDataAsync(CancellationToken cancellationToken)
  {
    var resultsToComplete = new Dictionary<string, (byte[] id, long size)>();

    foreach (var result in notifiedResults_.SelectMany(x => x))
    {
      await using var fs = new FileStream(Path.Combine(Folder,
                                                       result),
                                          FileMode.OpenOrCreate);
      var channel = Channel.CreateBounded<ReadOnlyMemory<byte>>(5);
      var addTask = objectStorage_.AddOrUpdateAsync(new ObjectData
                                                    {
                                                      ResultId  = result,
                                                      SessionId = sessionData_.SessionId,
                                                    },
                                                    channel.Reader.ReadAllAsync(cancellationToken),
                                                    cancellationToken);

      int read;
      do
      {
        var buffer = new byte[PayloadConfiguration.MaxChunkSize];
        read = await fs.ReadAsync(buffer,
                                  0,
                                  PayloadConfiguration.MaxChunkSize,
                                  cancellationToken)
                       .ConfigureAwait(false);
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
      resultsToComplete[result] = add;
    }

    foreach (var (resultId, memory) in resultsData_.SelectMany(x => x))
    {
      var add = await objectStorage_.AddOrUpdateAsync(new ObjectData
                                                      {
                                                        SessionId = sessionData_.SessionId,
                                                        ResultId  = resultId,
                                                      },
                                                      new List<ReadOnlyMemory<byte>>
                                                      {
                                                        memory,
                                                      }.ToAsyncEnumerable(),
                                                      cancellationToken)
                                    .ConfigureAwait(false);
      resultsToComplete[resultId] = add;
    }

    return resultsToComplete;
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
