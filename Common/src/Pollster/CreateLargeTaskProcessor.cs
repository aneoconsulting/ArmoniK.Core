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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
/// Processor for <see cref="ProcessReply.TypeOneofCase.CreateLargeTask"/>
/// </summary>
internal class CreateLargeTaskProcessor : IProcessReplyProcessor
{
  private readonly ISubmitter                               submitter_;
  private readonly IAsyncPipe<ProcessReply, ProcessRequest> asyncPipe_;
  private readonly ILogger                                  logger_;
  private          IList<string>?                           taskIds_;
  private          TaskOptions?                             options_;
  private readonly string                                   sessionId_;
  private readonly string                                   parentTaskId_;
  private readonly ProcessReplyCreateLargeTaskStateMachine  fsm_;
  private readonly Channel<TaskRequest>                     taskRequestsChannel_ = Channel.CreateBounded<TaskRequest>(10);
  private          Channel<ReadOnlyMemory<byte>>?           payloadsChannel_;
  private          Task?                                    completionTask_;

  /// <summary>
  /// Initializes the class with its required objects
  /// </summary>
  /// <param name="submitter">Interface class to manage tasks</param>
  /// <param name="asyncPipe">Interface class to exchange requests</param>
  /// <param name="sessionId">Session Id</param>
  /// <param name="parentTaskId">Parent task Id</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public CreateLargeTaskProcessor(ISubmitter                               submitter,
                                  IAsyncPipe<ProcessReply, ProcessRequest> asyncPipe,
                                  string                                   sessionId,
                                  string                                   parentTaskId,
                                  ILogger                                  logger)
  {
    submitter_    = submitter;
    asyncPipe_    = asyncPipe;
    logger_       = logger;
    sessionId_    = sessionId;
    parentTaskId_ = parentTaskId;
    fsm_          = new ProcessReplyCreateLargeTaskStateMachine(logger);
  }

  /// <inheritdoc />
  public async Task AddProcessReply(ProcessReply      processReply,
                                    CancellationToken cancellationToken)
  {
    switch (processReply.CreateLargeTask.TypeCase)
    {
      case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitRequest:
        fsm_.InitRequest();

        completionTask_ = Task.Run(async () =>
                                   {
                                     (taskIds_, options_) = await submitter_.CreateTasks(sessionId_,
                                                                                         parentTaskId_,
                                                                                         processReply.CreateLargeTask.InitRequest.TaskOptions,
                                                                                         taskRequestsChannel_.Reader.ReadAllAsync(cancellationToken),
                                                                                         cancellationToken)
                                                                            .ConfigureAwait(false);

                                     logger_.LogTrace("Send Task creation reply");
                                     await asyncPipe_.WriteAsync(new ProcessRequest
                                                                 {
                                                                   CreateTask = new ProcessRequest.Types.CreateTask
                                                                                {
                                                                                  Reply = new CreateTaskReply
                                                                                          {
                                                                                            Successfull = new Empty(),
                                                                                          },
                                                                                  ReplyId = processReply.RequestId,
                                                                                },
                                                                 })
                                                     .ConfigureAwait(false);
                                     logger_.LogTrace("Task creation reply sent");
                                   },
                                   cancellationToken);
        break;
      case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitTask:

        switch (processReply.CreateLargeTask.InitTask.TypeCase)
        {
          case InitTaskRequest.TypeOneofCase.Header:
            fsm_.AddHeader();
            payloadsChannel_ = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
                                                                             {
                                                                               SingleWriter = true,
                                                                               SingleReader = true,
                                                                             });

            await taskRequestsChannel_.Writer.WriteAsync(new TaskRequest(processReply.CreateLargeTask.InitTask.Header.Id,
                                                                         processReply.CreateLargeTask.InitTask.Header.ExpectedOutputKeys,
                                                                         processReply.CreateLargeTask.InitTask.Header.DataDependencies,
                                                                         payloadsChannel_.Reader.ReadAllAsync(cancellationToken)),
                                                         cancellationToken)
                                      .ConfigureAwait(false);


            break;
          case InitTaskRequest.TypeOneofCase.LastTask:
            fsm_.CompleteRequest();
            taskRequestsChannel_.Writer.Complete();
            break;
          case InitTaskRequest.TypeOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }

        break;
      case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.TaskPayload:
        switch (processReply.CreateLargeTask.TaskPayload.TypeCase)
        {
          case DataChunk.TypeOneofCase.Data:
            fsm_.AddDataChunk();
            await payloadsChannel_!.Writer.WriteAsync(processReply.CreateLargeTask.TaskPayload.Data.Memory,
                                                      cancellationToken)
                                   .ConfigureAwait(false);
            break;
          case DataChunk.TypeOneofCase.DataComplete:
            fsm_.CompleteData();
            payloadsChannel_!.Writer.Complete();
            payloadsChannel_ = null;
            break;
          case DataChunk.TypeOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }

        break;
      case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  /// <inheritdoc />
  public bool IsComplete()
    => fsm_.IsComplete();

  /// <inheritdoc />
  public async Task WaitForResponseCompletion(CancellationToken cancellationToken)
    => await completionTask_!.WaitAsync(cancellationToken)
                             .ConfigureAwait(false);

  /// <inheritdoc />
  public Task Cancel()
    => throw new NotImplementedException();

  /// <inheritdoc />
  public async Task CompleteProcessing(CancellationToken cancellationToken)
    => await submitter_.FinalizeTaskCreation(taskIds_!,
                                             options_!,
                                             cancellationToken)
                       .ConfigureAwait(false);
}
