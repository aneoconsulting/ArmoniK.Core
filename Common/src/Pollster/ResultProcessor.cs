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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.StateMachines;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
/// Processor for <see cref="ProcessReply.TypeOneofCase.Result"/>
/// </summary>
internal class ResultProcessor : IProcessReplyProcessor
{
  private readonly ProcessReplyResultStateMachine fsm_;
  private readonly ISubmitter                     submitter_;
  private readonly string                         sessionId_;
  private readonly string                         ownerTaskId_;
  private          Task?                          completionTask_;
  private readonly Channel<ReadOnlyMemory<byte>>  chunksChannel_;

  /// <summary>
  /// Initializes the class with its required objects
  /// </summary>
  /// <param name="submitter">Interface class to manage tasks</param>
  /// <param name="sessionId">Session Id of the task that owns the result</param>
  /// <param name="ownerTaskId">Task Id of the task that owns the result</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public ResultProcessor(ISubmitter submitter,
                         string     sessionId,
                         string     ownerTaskId,
                         ILogger    logger)
  {
    submitter_   = submitter;
    sessionId_   = sessionId;
    ownerTaskId_ = ownerTaskId;
    fsm_         = new ProcessReplyResultStateMachine(logger);
    chunksChannel_ = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
                                                                   {
                                                                     SingleWriter = true,
                                                                     SingleReader = true,
                                                                   });
  }

  /// <inheritdoc />
  public async Task AddProcessReply(ProcessReply      processReply,
                                    CancellationToken cancellationToken)
  {
    switch (processReply.Result.TypeCase)
    {
      case ProcessReply.Types.Result.TypeOneofCase.Init:
        switch (processReply.Result.Init.TypeCase)
        {
          case InitKeyedDataStream.TypeOneofCase.Key:
            fsm_.InitKey();
            completionTask_ = Task.Run(async () =>
                                       {
                                         await submitter_.SetResult(sessionId_,
                                                                    ownerTaskId_,
                                                                    processReply.Result.Init.Key,
                                                                    chunksChannel_.Reader.ReadAllAsync(cancellationToken),
                                                                    cancellationToken)
                                                         .ConfigureAwait(false);
                                       },
                                       cancellationToken);
            break;
          case InitKeyedDataStream.TypeOneofCase.LastResult:
            fsm_.CompleteRequest();
            break;
          case InitKeyedDataStream.TypeOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }

        break;
      case ProcessReply.Types.Result.TypeOneofCase.Data:
        switch (processReply.Result.Data.TypeCase)
        {
          case DataChunk.TypeOneofCase.Data:
            fsm_.AddDataChunk();
            await chunksChannel_.Writer.WriteAsync(processReply.Result.Data.Data.Memory,
                                                   cancellationToken)
                                .ConfigureAwait(false);
            break;
          case DataChunk.TypeOneofCase.DataComplete:
            fsm_.CompleteData();
            chunksChannel_.Writer.Complete();
            break;
          case DataChunk.TypeOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }

        break;
      case ProcessReply.Types.Result.TypeOneofCase.None:
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
  public Task CompleteProcessing(CancellationToken cancellationToken)
    => Task.CompletedTask;
}
