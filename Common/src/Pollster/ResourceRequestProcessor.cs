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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class ResourceRequestProcessor : IProcessReplyProcessor
{
  private readonly IObjectStorage                           resourcesStorage_;
  private readonly IAsyncPipe<ProcessReply, ProcessRequest> pipe_;
  private readonly ILogger                                  logger_;

  public ResourceRequestProcessor(IObjectStorage                           resourcesStorage,
                                  IAsyncPipe<ProcessReply, ProcessRequest> pipe,
                                  ILogger                                  logger)
  {
    resourcesStorage_ = resourcesStorage;
    pipe_             = pipe;
    logger_           = logger;
  }

  public async Task AddProcessReply(ProcessReply      processReply,
                                    CancellationToken cancellationToken)
  {
    IAsyncEnumerable<byte[]> bytes;
    try
    {
      bytes = resourcesStorage_.GetValuesAsync(processReply.Resource.Key,
                                               cancellationToken);
    }
    catch (ObjectDataNotFoundException)
    {
      await pipe_.WriteAsync(new ProcessRequest
                             {
                               Resource = new ProcessRequest.Types.DataReply
                                          {
                                            ReplyId = processReply.RequestId,
                                            Init = new ProcessRequest.Types.DataReply.Types.Init
                                                   {
                                                     Key   = processReply.Resource.Key,
                                                     Error = "Key not found",
                                                   },
                                          },
                             })
                 .ConfigureAwait(false);
      return;
    }

    await foreach (var dataReply in ToDataReply(bytes,
                                                processReply.RequestId,
                                                processReply.Resource.Key,
                                                cancellationToken)
                                    .WithCancellation(cancellationToken)
                                    .ConfigureAwait(false))
    {
      await pipe_.WriteAsync(new ProcessRequest
                             {
                               Resource = dataReply,
                             })
                 .ConfigureAwait(false);
    }
  }

  public bool IsComplete()
    => true;

  public Task WaitForResponseCompletion(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task Cancel()
    => throw new NotImplementedException();

  public Task CompleteProcessing(CancellationToken cancellationToken)
    => Task.CompletedTask;


  private static async IAsyncEnumerable<ProcessRequest.Types.DataReply> ToDataReply(IAsyncEnumerable<byte[]>                   bytes,
                                                                             string                                     replyId,
                                                                             string                                     key,
                                                                             [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var enumerator = bytes.GetAsyncEnumerator(cancellationToken);
    if (!await enumerator.MoveNextAsync(cancellationToken)
                         .ConfigureAwait(false))
    {
      throw new InvalidOperationException("No data were retrieved.");
    }

    yield return new ProcessRequest.Types.DataReply
                 {
                   ReplyId = replyId,
                   Init = new ProcessRequest.Types.DataReply.Types.Init
                          {
                            Key = key,
                            Data = new DataChunk
                                   {
                                     Data = UnsafeByteOperations.UnsafeWrap(enumerator.Current),
                                   },
                          },
                 };

    while (await enumerator.MoveNextAsync()
                           .ConfigureAwait(false))
    {
      yield return new ProcessRequest.Types.DataReply
                   {
                     ReplyId = replyId,
                     Data = new DataChunk
                            {
                              Data = UnsafeByteOperations.UnsafeWrap(enumerator.Current),
                            },
                   };
    }

    yield return new ProcessRequest.Types.DataReply
                 {
                   ReplyId = replyId,
                   Data = new DataChunk
                          {
                            DataComplete = true,
                          },
                 };
  }
}
