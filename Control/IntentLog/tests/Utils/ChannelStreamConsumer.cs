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
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Utils;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public class ChannelStreamConsumer : Stream
{
  private readonly ChannelReader<byte[]> channel_;
  private          ReadOnlyMemory<byte>  current_;

  public ChannelStreamConsumer(ChannelReader<byte[]> channel)
  {
    channel_ = channel;
    current_ = ReadOnlyMemory<byte>.Empty;
  }

  public override bool CanRead
    => true;

  public override bool CanSeek
    => false;

  public override bool CanWrite
    => false;

  public override long Length
    => throw new NotSupportedException();

  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  public override void Flush()
  {
  }

  public override int Read(byte[] buffer,
                           int    offset,
                           int    count)
    => Read(buffer.AsSpan(offset,
                          count));

  public override int Read(Span<byte> buffer)
  {
    WaitForBuffer()
      .WaitSync();
    return ReadBuffer(buffer);
  }

  public override Task<int> ReadAsync(byte[]            buffer,
                                      int               offset,
                                      int               count,
                                      CancellationToken cancellationToken)
    => ReadAsync(buffer.AsMemory(offset,
                                 count),
                 cancellationToken)
      .AsTask();

  public override ValueTask<int> ReadAsync(Memory<byte>      buffer,
                                           CancellationToken cancellationToken = new())
  {
    var wait = WaitForBuffer(cancellationToken);

    if (!wait.IsCompleted)
    {
      return Core();
    }

    wait.WaitSync();
    var count = ReadBuffer(buffer.Span);
    return new ValueTask<int>(count);

    async ValueTask<int> Core()
    {
      await wait.ConfigureAwait(false);

      return ReadBuffer(buffer.Span);
    }
  }

  private ValueTask WaitForBuffer(CancellationToken cancellationToken = default)
  {
    return channel_.Completion.IsCompleted
             ? new ValueTask()
             : Core();

    async ValueTask Core()
    {
      try
      {
        while (current_.IsEmpty)
        {
          current_ = await channel_.ReadAsync(cancellationToken)
                                   .ConfigureAwait(false);
        }
      }
      catch (ChannelClosedException)
      {
        // Empty on purpose
      }
    }
  }

  private int ReadBuffer(Span<byte> buffer)
  {
    var read = 0;

    while (!buffer.IsEmpty && !current_.IsEmpty)
    {
      if (current_.Length < buffer.Length)
      {
        read += current_.Length;

        current_.Span.CopyTo(buffer);
        buffer   = buffer[current_.Length..];
        current_ = ReadOnlyMemory<byte>.Empty;
        if (channel_.TryRead(out var remaining))
        {
          current_ = remaining;
        }
      }
      else
      {
        read += buffer.Length;

        current_.Span[..buffer.Length]
                .CopyTo(buffer);
        current_ = current_[buffer.Length..];
        buffer   = Span<byte>.Empty;
      }
    }

    return read;
  }

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

  public override void Close()
  {
    // Empty on purpose
  }
}
