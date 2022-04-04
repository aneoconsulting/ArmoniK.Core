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

using ArmoniK.Api.gRPC.V1;

using Google.Protobuf;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Stream.Worker
{
  [PublicAPI]
  public static class TaskRequestExtensions
  {
    public static IEnumerable<ProcessReply.Types.CreateLargeTaskRequest> ToRequestStream(this IEnumerable<TaskRequest> taskRequests,
                                                                                         TaskOptions?                  taskOptions,
                                                                                         int                           chunkMaxSize)
    {
      if(taskOptions is not null)
      {
        yield return new()
        {
          InitRequest = new()
          {
            TaskOptions = taskOptions,
          },
        };
      }
      else
      {
        yield return new()
        {
          InitRequest = new(),
        };
      }

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

    public static IEnumerable<ProcessReply.Types.CreateLargeTaskRequest> ToRequestStream(this TaskRequest taskRequest,
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
            Id = taskRequest.Id,
          },
        },
      };

      var start = 0;

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
  }
}
