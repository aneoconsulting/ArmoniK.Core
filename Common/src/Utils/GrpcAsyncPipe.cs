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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

namespace ArmoniK.Core.Common.Utils;

internal class GrpcAsyncPipe<TReadMessage, TWriteMessage> : IAsyncPipe<TReadMessage, TWriteMessage>
{
  private readonly Task<TReadMessage>                       response_;
  private readonly IClientStreamWriter<TWriteMessage> writer_;

  public GrpcAsyncPipe(Task<TReadMessage>                 response,
                       IClientStreamWriter<TWriteMessage> writer)
  {
    response_            = response;
    writer_              = writer;
    writer_.WriteOptions = new WriteOptions(WriteFlags.NoCompress);
  }


  public async Task<TReadMessage> ReadAsync(CancellationToken cancellationToken)
    => await response_.WaitAsync(cancellationToken)
                      .ConfigureAwait(false);

  public Task WriteAsync(TWriteMessage message)
    => writer_.WriteAsync(message);

  public async Task WriteAsync(IEnumerable<TWriteMessage> message)
  {
    foreach (var writeMessage in message)
    {
      await writer_.WriteAsync(writeMessage)
                   .ConfigureAwait(false);
    }
  }

  public Task CompleteAsync()
    => writer_.CompleteAsync();
}
