// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Grpc.Core;

namespace ArmoniK.Core.Common.gRPC.Services;

public static  class GrpcSubmitterExtensions
{
  public static async Task<(string Session, string Parent, string Dispatch, TaskOptions Options, IAsyncEnumerable<TaskRequest> Requests)> BuildCreateTaskArguments(this
    IAsyncStreamReader<CreateLargeTaskRequest> stream, CancellationToken cancellationToken)
  {
    var enumerator = stream.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

    if (!await enumerator.MoveNextAsync(cancellationToken))
      throw new RpcException(new(StatusCode.InvalidArgument,
                                 "stream contained no message"));

    var first = enumerator.Current;

    if (first.TypeCase != CreateLargeTaskRequest.TypeOneofCase.InitRequest)
      throw new RpcException(new(StatusCode.InvalidArgument,
                                 "First message in stream must be of type InitRequest"),
                             "First message in stream must be of type InitRequest");

    return (first.InitRequest.SessionId,
            first.InitRequest.SessionId,
            first.InitRequest.SessionId,
            first.InitRequest.TaskOptions,
            enumerator.BuildRequests(cancellationToken));

  }

  public static async IAsyncEnumerable<TaskRequest> BuildRequests(this
    IAsyncEnumerator<CreateLargeTaskRequest> enumerator,
    [EnumeratorCancellation] CancellationToken                       cancellationToken)
  {
    while (await enumerator.MoveNextAsync(cancellationToken))
    {
      if (enumerator.Current.TypeCase != CreateLargeTaskRequest.TypeOneofCase.InitTask)
        throw new RpcException(new(StatusCode.InvalidArgument,
                                   "Expected an InitTask"));

      var                         id                         = enumerator.Current.InitTask.Id;
      IEnumerable<string>         initTaskExpectedOutputKeys = enumerator.Current.InitTask.ExpectedOutputKeys;
      IEnumerable<string>         initTaskDataDependencies   = enumerator.Current.InitTask.DataDependencies;
      Queue<ReadOnlyMemory<byte>> chunks                     = new();

      chunks.Enqueue(enumerator.Current.InitTask.PayloadChunk.Data.Memory);

      if (!enumerator.Current.InitTask.PayloadChunk.DataComplete)
      {
        if (!await enumerator.MoveNextAsync(cancellationToken) || enumerator.Current.TypeCase != CreateLargeTaskRequest.TypeOneofCase.TaskPayload)
          throw new RpcException(new(StatusCode.InvalidArgument,
                                     "Previous InitTask message had an incomplete PayloadChunk. Expecting a new PayloadChunk."));

        chunks.Enqueue(enumerator.Current.TaskPayload.Data.Memory);

        while (!enumerator.Current.TaskPayload.DataComplete)
        {
          if (!await enumerator.MoveNextAsync(cancellationToken) || enumerator.Current.TypeCase != CreateLargeTaskRequest.TypeOneofCase.TaskPayload)
            throw new RpcException(new(StatusCode.InvalidArgument,
                                       "Previous message had an incomplete PayloadChunk. Expecting a new PayloadChunk."));

          chunks.Enqueue(enumerator.Current.TaskPayload.Data.Memory);
        } 
      }

      yield return new(id,
                       initTaskExpectedOutputKeys,
                       initTaskDataDependencies,
                       chunks.ToAsyncEnumerable());
    }
  }
}