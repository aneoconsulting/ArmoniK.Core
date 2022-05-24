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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

internal class ResultProcessor : IProcessReplyProcessor
{
  private readonly ProcessReplyResultStateMachine fsm_;
  private readonly IObjectStorage                 resultStorage_;
  private readonly IResultTable                   resultTable_;
  private          string?                        resultKey_;
  private readonly string                         sessionId_;
  private readonly string                         ownerTaskId_;
  private readonly IList<ReadOnlyMemory<byte>>    chunks_;

  public ResultProcessor(IObjectStorage resultStorage,
                         IResultTable   resultTable,
                         string         sessionId,
                         string         ownerTaskId,
                         ILogger        logger)
  {
    resultStorage_ = resultStorage;
    resultTable_   = resultTable;
    sessionId_     = sessionId;
    ownerTaskId_   = ownerTaskId;
    fsm_           = new ProcessReplyResultStateMachine(logger);
    chunks_        = new List<ReadOnlyMemory<byte>>();
  }

  public Task AddProcessReply(ProcessReply      processReply,
                              CancellationToken cancellationToken)
  {
    switch (processReply.Result.TypeCase)
    {
      case ProcessReply.Types.Result.TypeOneofCase.Init:
        switch (processReply.Result.Init.TypeCase)
        {
          case InitKeyedDataStream.TypeOneofCase.Key:
            fsm_.InitKey();

            resultKey_ = processReply.Result.Init.Key;

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
            chunks_.Add(processReply.Result.Data.Data.Memory);
            break;
          case DataChunk.TypeOneofCase.DataComplete:
            fsm_.CompleteData();
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
    return Task.CompletedTask;
  }

  public bool IsComplete()
    => fsm_.IsComplete();

  public async Task WaitForResponseCompletion(CancellationToken cancellationToken)
  {
    if (!IsComplete())
    {
      throw new InvalidOperationException("Result should be complete");
    }

    if (resultKey_ == null)
    {
      throw new InvalidOperationException();
    }

    await resultStorage_.AddOrUpdateAsync(resultKey_,
                                          chunks_.ToAsyncEnumerable(),
                                          cancellationToken)
                        .ConfigureAwait(false);
    await resultTable_.SetResult(sessionId_,
                                 ownerTaskId_,
                                 resultKey_,
                                 cancellationToken)
                      .ConfigureAwait(false);
  }

  public Task Cancel()
    => throw new NotImplementedException();

  public Task CompleteProcessing(CancellationToken cancellationToken)
    => Task.CompletedTask;
}
