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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.gRPC.Services;

public static class GrpcSubmitterExtensions
{
  public static async IAsyncEnumerable<TaskRequest> BuildRequests(this                     IAsyncEnumerator<CreateLargeTaskRequest> enumerator,
                                                                  [EnumeratorCancellation] CancellationToken                        cancellationToken)
  {
    var           id                 = string.Empty;
    IList<string> expectedOutputKeys = Array.Empty<string>();
    IList<string> dataDependencies   = Array.Empty<string>();
    var           chunks             = new Queue<ReadOnlyMemory<byte>>();

    while (await enumerator.MoveNextAsync(cancellationToken)
                           .ConfigureAwait(false))
    {
      switch (enumerator.Current.TypeCase)
      {
        case CreateLargeTaskRequest.TypeOneofCase.InitTask:
          if (!string.IsNullOrEmpty(id) || dataDependencies.Any() || expectedOutputKeys.Any() || chunks.Any())
          {
            throw new InvalidOperationException();
          }

          switch (enumerator.Current.InitTask.TypeCase)
          {
            case InitTaskRequest.TypeOneofCase.Header:

              id                 = enumerator.Current.InitTask.Header.Id;
              expectedOutputKeys = enumerator.Current.InitTask.Header.ExpectedOutputKeys;
              dataDependencies   = enumerator.Current.InitTask.Header.DataDependencies;
              break;
            case InitTaskRequest.TypeOneofCase.LastTask:
              yield break;
            case InitTaskRequest.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case CreateLargeTaskRequest.TypeOneofCase.TaskPayload:
          if (string.IsNullOrEmpty(id) || !expectedOutputKeys.Any())
          {
            throw new InvalidOperationException();
          }

          switch (enumerator.Current.TaskPayload.TypeCase)
          {
            case DataChunk.TypeOneofCase.Data:
              chunks.Enqueue(enumerator.Current.TaskPayload.Data.Memory);
              break;
            case DataChunk.TypeOneofCase.DataComplete:
              yield return new TaskRequest(id,
                                           expectedOutputKeys,
                                           dataDependencies,
                                           chunks.ToAsyncEnumerable());

              id                 = string.Empty;
              expectedOutputKeys = Array.Empty<string>();
              dataDependencies   = Array.Empty<string>();
              chunks             = new Queue<ReadOnlyMemory<byte>>();
              break;
            case DataChunk.TypeOneofCase.None:
            default:
              throw new InvalidOperationException();
          }

          break;
        case CreateLargeTaskRequest.TypeOneofCase.InitRequest:
        case CreateLargeTaskRequest.TypeOneofCase.None:
        default:
          throw new InvalidOperationException();
      }
    }
  }
}
