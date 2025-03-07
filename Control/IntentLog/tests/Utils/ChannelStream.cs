// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Utils;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public class ChannelStream : Stream
{
  private readonly ChannelReader<byte[]> reader_;
  private readonly ChannelWriter<byte[]> writer_;
  private          ReadOnlyMemory<byte>  currentRead_;

  public ChannelStream(ChannelReader<byte[]> reader,
                       ChannelWriter<byte[]> writer)
  {
    reader_      = reader;
    writer_      = writer;
    currentRead_ = ReadOnlyMemory<byte>.Empty;
  }

  public override bool CanRead
    => true;

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
    return reader_.Completion.IsCompleted
             ? new ValueTask()
             : Core();

    async ValueTask Core()
    {
      try
      {
        while (currentRead_.IsEmpty)
        {
          currentRead_ = await reader_.ReadAsync(cancellationToken)
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

    while (!buffer.IsEmpty && !currentRead_.IsEmpty)
    {
      if (currentRead_.Length < buffer.Length)
      {
        read += currentRead_.Length;

        currentRead_.Span.CopyTo(buffer);
        buffer       = buffer[currentRead_.Length..];
        currentRead_ = ReadOnlyMemory<byte>.Empty;
        if (reader_.TryRead(out var remaining))
        {
          currentRead_ = remaining;
        }
      }
      else
      {
        read += buffer.Length;

        currentRead_.Span[..buffer.Length]
                    .CopyTo(buffer);
        currentRead_ = currentRead_[buffer.Length..];
        buffer       = Span<byte>.Empty;
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

  public override void Write(ReadOnlySpan<byte> buffer)
    => writer_.WriteAsync(buffer.ToArray())
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
    => writer_.Complete();

  public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
                                       CancellationToken    cancellationToken = new())
    => writer_.WriteAsync(buffer.ToArray(),
                          cancellationToken);


  public static (ChannelStream, ChannelStream) CreatePair(int capacity = 0)
  {
    var chan1 = capacity <= 0
                  ? Channel.CreateUnbounded<byte[]>()
                  : Channel.CreateBounded<byte[]>(capacity);
    var chan2 = capacity <= 0
                  ? Channel.CreateUnbounded<byte[]>()
                  : Channel.CreateBounded<byte[]>(capacity);

    var stream1 = new ChannelStream(chan1.Reader,
                                    chan2.Writer);
    var stream2 = new ChannelStream(chan2.Reader,
                                    chan1.Writer);

    return (stream1, stream2);
  }
}
