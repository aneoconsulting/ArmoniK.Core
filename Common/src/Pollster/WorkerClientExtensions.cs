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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;

namespace ArmoniK.Core.Common.Pollster;

public static class WorkerClientExtensions
{
  public static IAsyncEnumerable<TaskRequest> ReconstituteTaskRequest(
    this IEnumerable<ProcessReply> stream,
    ILogger logger)
    => stream.ToAsyncEnumerable().ReconstituteTaskRequest(CancellationToken.None, logger);
  
  public static async IAsyncEnumerable<TaskRequest> ReconstituteTaskRequest(
    this                     IAsyncEnumerable<ProcessReply> stream,
    [EnumeratorCancellation] CancellationToken              cancellationToken,
    ILogger logger)
  {
    var enumerator = stream.GetAsyncEnumerator(cancellationToken);

    Channel<ReadOnlyMemory<byte>>? channel = null;

    TaskRequest? taskRequest = null;
    while (await enumerator.MoveNextAsync(cancellationToken))
    {
      var current = enumerator.Current;

      switch (current.CreateLargeTask.TypeCase)
      {
        case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitTask:
          switch (current.CreateLargeTask.InitTask.TypeCase)
          {
            case InitTaskRequest.TypeOneofCase.Header:
              channel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new()
                                                                      {
                                                                        SingleWriter = true,
                                                                        SingleReader = true,
                                                                      });
              taskRequest = new(current.CreateLargeTask.InitTask.Header.Id,
                                current.CreateLargeTask.InitTask.Header.ExpectedOutputKeys,
                                current.CreateLargeTask.InitTask.Header.DataDependencies,
                                channel.Reader.ReadAllAsync(cancellationToken));
              break;
            case InitTaskRequest.TypeOneofCase.LastTask:
              yield break;
            case InitTaskRequest.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.TaskPayload:
          switch (current.CreateLargeTask.TaskPayload.TypeCase)
          {
            case DataChunk.TypeOneofCase.Data:
              await channel!.Writer.WriteAsync(current.CreateLargeTask.TaskPayload.Data.Memory,
                                               cancellationToken);
              break;
            case DataChunk.TypeOneofCase.DataComplete:
              channel!.Writer.Complete();
              channel = null;
              yield return taskRequest!;
              taskRequest = null;
              break;
            case DataChunk.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.None:
        case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitRequest:
        default:
          throw new InvalidOperationException();
      }
    }

    if (channel is not null || taskRequest is not null)
    {
      throw new InvalidOperationException("stream ended unexpectedly.");
    }
  }






  public static async IAsyncEnumerable<IList<ProcessReply>> Separate(
    this                     IAsyncStreamReader<ProcessReply> stream,
    ILogger                                                   logger,
    [EnumeratorCancellation] CancellationToken                cancellationToken)
  {
    List<ProcessReply>? output = null;

    var replyType = ProcessReply.TypeOneofCase.None;
    var requestId = string.Empty;

    var isLargeTaskPayloadFinished = true;


    await foreach (var reply in stream.ReadAllAsync(cancellationToken)
                                      .WithCancellation(cancellationToken))
    {
      void InitNewStream(bool singleStream)
      {
        if (output is not null || replyType is not ProcessReply.TypeOneofCase.None || !string.IsNullOrEmpty(requestId))
          throw new InvalidOperationException("Stream unexpectedly initialized a new object. Objects all need to be explicitly terminated.");

        if (!singleStream)
        {
          output    = new();
          replyType = reply.TypeCase;
          requestId = reply.RequestId;
        }
      }

      void EndStream()
      {
        output    = null;
        replyType = ProcessReply.TypeOneofCase.None;
        requestId = string.Empty;
      }



      if (replyType != ProcessReply.TypeOneofCase.None && reply.TypeCase != replyType)
      {
        throw new InvalidOperationException("Stream messages unexpectedly changed their types. Current implementation does not support stream multiplexing.");
      }

      if (!string.IsNullOrEmpty(requestId) && reply.RequestId != requestId)
      {
        throw new InvalidOperationException("Stream messages unexpectedly changed their requestId. Current implementation does not support stream multiplexing.");
      }

      switch (reply.TypeCase)
      {
        case ProcessReply.TypeOneofCase.Output:
          switch (reply.Output.TypeCase)
          {
            case Output.TypeOneofCase.Error:
            case Output.TypeOneofCase.Ok:
              InitNewStream(true);
              yield return new[] { reply };
              yield break;
            case Output.TypeOneofCase.None:
            default:
              throw new InvalidOperationException("Incorrect type for " + nameof(reply.Output));
          }
        case ProcessReply.TypeOneofCase.Result:
          switch (reply.Result.TypeCase)
          {
            case ProcessReply.Types.Result.TypeOneofCase.Init:
              switch (reply.Result.Init.TypeCase)
              {
                case InitKeyedDataStream.TypeOneofCase.Key:
                  InitNewStream(false);
                  output!.Add(reply);
                  break;
                case InitKeyedDataStream.TypeOneofCase.LastResult:
                  InitNewStream(true);
                  break;
                case InitKeyedDataStream.TypeOneofCase.None:
                default:
                  throw new InvalidOperationException();
              }

              break;
            case ProcessReply.Types.Result.TypeOneofCase.Data:
              switch (reply.Result.Data.TypeCase)
              {
                case DataChunk.TypeOneofCase.Data:
                  output!.Add(reply);
                  break;
                case DataChunk.TypeOneofCase.DataComplete:
                  yield return output!;
                  EndStream();
                  break;
                case DataChunk.TypeOneofCase.None:
                default:
                  throw new InvalidOperationException();
              }

              break;
            case ProcessReply.Types.Result.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case ProcessReply.TypeOneofCase.CreateSmallTask:
          InitNewStream(true);
          yield return new[] { reply };
          break;
        case ProcessReply.TypeOneofCase.CreateLargeTask:
          switch (reply.CreateLargeTask.TypeCase)
          {
            case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitRequest:
              InitNewStream(false);
              output!.Add(reply);
              break;
            case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.InitTask:
              switch (reply.CreateLargeTask.InitTask.TypeCase)
              {
                case InitTaskRequest.TypeOneofCase.Header:
                  if (!isLargeTaskPayloadFinished)
                    throw new InvalidOperationException("Payload from the previous task has not been closed.");
                  isLargeTaskPayloadFinished = false;
                  output!.Add(reply);
                  break;
                case InitTaskRequest.TypeOneofCase.LastTask:
                  if (!isLargeTaskPayloadFinished)
                    throw new InvalidOperationException("Payload from the previous task has not been closed.");
                  yield return output!;
                  EndStream();
                  break;
                case InitTaskRequest.TypeOneofCase.None:
                default:
                  throw new InvalidOperationException();
              }

              break;
            case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.TaskPayload:
              switch (reply.CreateLargeTask.TaskPayload.TypeCase)
              {
                case DataChunk.TypeOneofCase.Data:
                  if (isLargeTaskPayloadFinished)
                    throw new InvalidOperationException("Unexpectedly received a task payload chunk.");
                  output!.Add(reply);
                  break;
                case DataChunk.TypeOneofCase.DataComplete:
                  isLargeTaskPayloadFinished = true;
                  output!.Add(reply);
                  break;
                case DataChunk.TypeOneofCase.None:
                default:
                  throw new InvalidOperationException();
              }

              break;
            case ProcessReply.Types.CreateLargeTaskRequest.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case ProcessReply.TypeOneofCase.Resource:
        case ProcessReply.TypeOneofCase.CommonData:
        case ProcessReply.TypeOneofCase.DirectData:
          throw new NotImplementedException();
        case ProcessReply.TypeOneofCase.None:
        default:
          throw new InvalidOperationException();
      }
    }
  }

  public static async IAsyncEnumerable<ProcessRequest.Types.DataReply> ToDataReply(this IAsyncEnumerable<byte[]>              bytes,
                                                                                   string                                     replyId,
                                                                                   string                                     key,
                                                                                   [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var enumerator = bytes.GetAsyncEnumerator(cancellationToken);
    if (!await enumerator.MoveNextAsync(cancellationToken))
    {
      throw new InvalidOperationException("No data were retrieved.");
    }

    yield return new()
                 {
                   ReplyId = replyId,
                   Init = new()
                          {
                            Key = key,
                            Data = new()
                                   {
                                     Data = UnsafeByteOperations.UnsafeWrap(enumerator.Current),
                                   },
                          },
                 };

    while (await enumerator.MoveNextAsync())
    {
      yield return new()
                   {
                     ReplyId = replyId,
                     Data = new()
                            {
                              Data = UnsafeByteOperations.UnsafeWrap(enumerator.Current),
                            },
                   };
    }

    yield return new()
                 {
                   ReplyId = replyId,
                   Data = new()
                          {
                            DataComplete = true,
                          },
                 };
  }
}
