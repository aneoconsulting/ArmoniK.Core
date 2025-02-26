// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-$CURRENT_YEAR.All rights reserved.
// 
// This program is free software:you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Utils;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public class ChannelStreamProducer : Stream, IAsyncEnumerable<byte[]>
{
  private readonly Channel<byte[]> channel_;

  public ChannelStreamProducer(int capacity = 0)
    => channel_ = capacity > 0
                    ? Channel.CreateBounded<byte[]>(capacity)
                    : Channel.CreateUnbounded<byte[]>();

  public ChannelReader<byte[]> Reader
    => channel_.Reader;

  public override bool CanRead
    => false;

  public override bool CanSeek
    => false;

  public override bool CanWrite
    => true;

  public override long Length
    => throw new NotSupportedException();

  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  public IAsyncEnumerator<byte[]> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    => Reader.ReadAllAsync(cancellationToken)
             .GetAsyncEnumerator(CancellationToken.None);

  public override void Flush()
  {
  }

  public override int Read(byte[] buffer,
                           int    offset,
                           int    count)
    => throw new NotSupportedException();

  public override long Seek(long       offset,
                            SeekOrigin origin)
    => throw new NotSupportedException();

  public override void SetLength(long value)
    => throw new NotSupportedException();

  public override void Write(byte[] buffer,
                             int    offset,
                             int    count)
    => Write(buffer.AsSpan(offset,
                           count));

  public override void Write(ReadOnlySpan<byte> buffer)
    => channel_.Writer.WriteAsync(buffer.ToArray())
               .WaitSync();

  public override Task WriteAsync(byte[]            buffer,
                                  int               offset,
                                  int               count,
                                  CancellationToken cancellationToken)
    => WriteAsync(buffer.AsMemory(offset,
                                  count),
                  cancellationToken)
      .AsTask();

  public override void Close()
    => channel_.Writer.Complete();

  public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
                                       CancellationToken    cancellationToken = new())
    => channel_.Writer.WriteAsync(buffer.ToArray(),
                                  cancellationToken);
}
