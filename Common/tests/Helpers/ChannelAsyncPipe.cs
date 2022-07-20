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
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class ChannelAsyncPipe<TReadMessage, TWriteMessage> : IAsyncPipe<TReadMessage, TWriteMessage>
{
  private readonly TReadMessage           message_;
  private readonly Channel<TReadMessage>  readerChannel_ = Channel.CreateUnbounded<TReadMessage>();
  private readonly Channel<TWriteMessage> writerChannel_ = Channel.CreateUnbounded<TWriteMessage>();

  public ChannelAsyncPipe(TReadMessage message)
  {
    message_ = message;
  }

  private ChannelAsyncPipe(Channel<TReadMessage> readerChannel,
                           Channel<TWriteMessage> writerChannel)
  {
    readerChannel_ = readerChannel;
    writerChannel_ = writerChannel;
  }

  public Task<TReadMessage> ReadAsync(CancellationToken cancellationToken)
    => Task.FromResult(message_);

  public async Task WriteAsync(TWriteMessage message)
    => await writerChannel_.Writer.WriteAsync(message)
                           .ConfigureAwait(false);

  public async Task WriteAsync(IEnumerable<TWriteMessage> messages)
  {
    foreach (var message in messages)
    {
      await writerChannel_.Writer.WriteAsync(message)
                          .ConfigureAwait(false);
    }
  }

  public Task CompleteAsync()
  {
    writerChannel_.Writer.Complete();
    return Task.CompletedTask;
  }

  public IAsyncPipe<TWriteMessage, TReadMessage> Reverse
    => new ChannelAsyncPipe<TWriteMessage, TReadMessage>(writerChannel_,
                                                         readerChannel_);
}
