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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Stream.Client
{
  [PublicAPI]
  public static class SubmitterClientExt
  {
    public static async Task<CreateTaskReply> CreateTasksAsync(this Submitter.SubmitterClient client,
                                                               string                         sessionId,
                                                               TaskOptions                    taskOptions,
                                                               IEnumerable<TaskRequest>       taskRequests,
                                                               CancellationToken              cancellationToken = default)
    {
      var serviceConfiguration = await client.GetServiceConfigurationAsync(new(),
                                                                           cancellationToken: cancellationToken);

      var stream = client.CreateLargeTasks(cancellationToken: cancellationToken);

      foreach (var createLargeTaskRequest in taskRequests.ToRequestStream(sessionId,
                                                                          taskOptions,
                                                                          serviceConfiguration.DataChunkMaxSize))
      {
        await stream.RequestStream.WriteAsync(createLargeTaskRequest);
      }

      return await stream.ResponseAsync;
    }



    

    public static IEnumerable<CreateLargeTaskRequest> ToRequestStream(this IEnumerable<TaskRequest> taskRequests,
                                                                      string                        sessionId,
                                                                      TaskOptions                   taskOptions,
                                                                      int                           chunkMaxSize)
    {
      yield return new()
                   {
                     InitRequest = new()
                                   {
                                     SessionId   = sessionId,
                                     TaskOptions = taskOptions,
                                   },
                   };

      using var taskRequestEnumerator = taskRequests.GetEnumerator();

      if (!taskRequestEnumerator.MoveNext())
      {
        yield break;
      }

      var currentRequest = taskRequestEnumerator.Current;

      while (taskRequestEnumerator.MoveNext())
      {

        foreach (var createLargeTaskRequest in currentRequest.ToRequestStream(false,
                                                                              chunkMaxSize))
        {
          yield return createLargeTaskRequest;
        }


        currentRequest = taskRequestEnumerator.Current;
      }

      foreach (var createLargeTaskRequest in currentRequest.ToRequestStream(true,
                                                                            chunkMaxSize))
      {
        yield return createLargeTaskRequest;
      }
    }

    public static IEnumerable<CreateLargeTaskRequest> ToRequestStream(this TaskRequest taskRequest,
                                                                      bool             isLast,
                                                                      int              chunkMaxSize)
    {
      yield return new()
                   {
                     InitTask = new()
                                {
                                  Header = new()
                                           {
                                             DataDependencies =
                                             {
                                               taskRequest.DataDependencies,
                                             },
                                             ExpectedOutputKeys =
                                             {
                                               taskRequest.ExpectedOutputKeys,
                                             },
                                             Id       = taskRequest.Id,
                                           },
                                },
                   };

      var start = 0;

      if (taskRequest.Payload.Length == 0)
      {
        yield return new()
        {
          TaskPayload = new()
          {
            Data = ByteString.Empty,
          },
        };
      }

      while (start < taskRequest.Payload.Length)
      {
        var chunkSize = Math.Min(chunkMaxSize,
                                 taskRequest.Payload.Length - start);

        yield return new()
                     {
                       TaskPayload = new()
                                     {
                                       Data = ByteString.CopyFrom(taskRequest.Payload.Span.Slice(start,
                                                                                                 chunkSize)),
                                     },
                     };

        start += chunkSize;
      }

      yield return new()
      {
        TaskPayload = new()
        {
          DataComplete = true,
        },
      };

      if (isLast)
      {
        yield return new()
                    {
                      InitTask = new()
                                 {
                                   LastTask = true,
                                 },
                    };

      }

    }

    public static async Task<byte[]> GetResultAsync(this Submitter.SubmitterClient client,
                                                               ResultRequest resultRequest,
                                                               CancellationToken              cancellationToken = default)
    {
      var streamingCall = client.TryGetResultStream(resultRequest);

      var result = new List<byte>();

      await foreach (var reply in streamingCall.ResponseStream.ReadAllAsync(cancellationToken))
        switch (reply.TypeCase)
        {
          case ResultReply.TypeOneofCase.Result:
            if (!reply.Result.DataComplete)
            {
              result.AddRange(reply.Result.Data.ToByteArray());
            }
            break;
          case ResultReply.TypeOneofCase.None:
            throw new Exception("Issue with Server !");
          case ResultReply.TypeOneofCase.Error:
            throw new Exception($"Error in task {reply.Error.TaskId}");
          case ResultReply.TypeOneofCase.NotCompletedTask:
            throw new Exception($"Task {reply.NotCompletedTask} not completed");
          default:
            throw new ArgumentOutOfRangeException();
        }

      return result.ToArray();
    }
  }
}
